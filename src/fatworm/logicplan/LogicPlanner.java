package fatworm.logicplan;

import static fatworm.parser.FatwormParser.SELECT;
import static fatworm.parser.FatwormParser.SELECT_DISTINCT;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.BaseTree;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import fatworm.expr.*;
import fatworm.type.*;
import fatworm.optimize.Optimize;
import fatworm.parser.FatwormParser;
import fatworm.util.Env;
import fatworm.util.Util;

public class LogicPlanner {
	public LogicPlanner() {
	}
	
	public static Plan translate(CommonTree t) {
		switch (t.getType()) {
		case FatwormParser.SELECT:
		case FatwormParser.SELECT_DISTINCT:
			return transSelect(t, false);
		case FatwormParser.CREATE_DATABASE:
		case FatwormParser.DROP_DATABASE:
		default:
			return null;
		}
	}
	
	public static String getAttr(Tree t) {
		return t.getText().equals(".") ? t.getChild(0).getText() + "."+ t.getChild(1).getText() : t.getText();
	}

	public static Expr getExpr(Tree t) {
		switch (t.getType()) {
		case FatwormParser.SELECT:
		case FatwormParser.SELECT_DISTINCT:
			return new QueryCall(transSelect((BaseTree) t, false));
		case FatwormParser.SUM:
		case FatwormParser.AVG:
		case FatwormParser.COUNT:
		case FatwormParser.MAX:
		case FatwormParser.MIN:
			return new FuncCall(t.getType(), getAttr(t.getChild(0)));
		case FatwormParser.INTEGER_LITERAL:
			try {
				Integer value = Integer.parseInt(t.getText());
				return new IntLiteral(value);
			} catch (NumberFormatException e) {
				return new IntLiteral(new BigInteger(t.getText()));
			}
		case FatwormParser.FLOAT_LITERAL:
			return new FloatLiteral(Float.parseFloat(t.getText()));
		case FatwormParser.STRING_LITERAL:
			return new StringLiteral(Util.strip(t.getText()));
		case FatwormParser.TRUE:
		case FatwormParser.FALSE:
			return new BoolLiteral(t.getType() == FatwormParser.TRUE);
		case FatwormParser.AND:
			return new BinaryExpr(getExpr(t.getChild(0)), BinaryOp.AND, getExpr(t.getChild(1)));
		case FatwormParser.OR:
			return new BinaryExpr(getExpr(t.getChild(0)), BinaryOp.OR, getExpr(t.getChild(1)));
		case FatwormParser.EXISTS:
		case FatwormParser.NOT_EXISTS:
			if (!(t.getChild(0) instanceof BaseTree)) return null;
			return new ExistCall(transSelect((BaseTree) t.getChild(0), false), t.getType() == FatwormParser.NOT_EXISTS);
		case FatwormParser.IN:
			return new InCall(getExpr(t.getChild(0)), transSelect((BaseTree)t.getChild(1), false), t.getType() != FatwormParser.IN);
		case FatwormParser.ANY:
		case FatwormParser.ALL:
			return new AnyAllCall(getExpr(t.getChild(0)), BinaryOp.getBinaryOp(t.getChild(1).getText()), transSelect((BaseTree) t.getChild(2), false), t.getType() == FatwormParser.ALL);
			default:
				BinaryOp op = BinaryOp.getBinaryOp(t.getText());
				if (op != null && t.getChildCount() == 2) {
					Expr l = getExpr(t.getChild(0));
					Expr r = getExpr(t.getChild(1));
					return new BinaryExpr(l, op, r);
				} else if (op != null) { //negative
					Expr tmp = getExpr(t.getChild(0));
					if (tmp instanceof IntLiteral) {
						if (((IntLiteral)tmp).i instanceof DECIMAL) {
							DECIMAL f = (DECIMAL)((IntLiteral)tmp).i;
							return new IntLiteral(f.value.negate().toBigIntegerExact());
						} else {
							INT f = (INT)((IntLiteral)tmp).i;
							return new IntLiteral(-f.value);
						}
					} else if (tmp instanceof FloatLiteral) {
						FLOAT f = (FLOAT)((FloatLiteral)tmp).i;
						return new FloatLiteral(-f.value);
					} else if (tmp.isConst && tmp.value instanceof INT) {
						INT f = (INT)tmp.value;
						return new IntLiteral(-f.value);
					} else return new BinaryExpr(new IntLiteral(BigInteger.valueOf(0)), op, tmp);
				} else return new ID(getAttr(t));
		}
	}

	public static Plan transSelect(BaseTree t, boolean optFlag) {
		if (t.getType()!=FatwormParser.SELECT && t.getType()!=FatwormParser.SELECT_DISTINCT) return null;
		Plan ans = null;
		Plan src = null;
		Expr pred = null;
		Expr having = null;
		String groupBy = null;

		List<Integer> orderType = new ArrayList<Integer>();
		List<String> orderField = new ArrayList<String>();
		boolean hasOrder = false;

		for (Object o : t.getChildren()) {
			Tree x = (Tree) o;
			switch (x.getType()) {
			case FatwormParser.FROM:
				src = transFrom((BaseTree)x);
				break;
			case FatwormParser.WHERE:
				pred = getExpr(x.getChild(0));
				break;
			case FatwormParser.GROUP:
				groupBy = getAttr(x.getChild(0));
				break;
			case FatwormParser.HAVING:
				having = getExpr(x.getChild(0));
				break;
			case FatwormParser.ORDER:
				hasOrder = true;
				for (Object oo : ((BaseTree) x).getChildren()) {
					Tree xx = (Tree) oo;
					orderType.add(xx.getType() == FatwormParser.DESC ? xx.getType(): FatwormParser.ASC);
					orderField.add(xx.getType() == FatwormParser.DESC || xx.getType() == FatwormParser.ASC ? getAttr(xx.getChild(0)): getAttr(xx));
				}
				break;
			}
		}
		
		boolean hasAggr = groupBy != null;
		if(having != null) hasAggr |= having.hasAggr();
		
		boolean hasRename = false;
		List<Expr> expr = new ArrayList<Expr>();
		List<String> alias = new ArrayList<String>();
		boolean hasProjAll = false;
		boolean hasAs = false;
		for (Object o : t.getChildren()) {
			Tree x = (Tree) o;
			if (x.getType() == FatwormParser.FROM) break;

			if (x.getType() == FatwormParser.AS){
				hasAs = true;
				Expr tmp = getExpr(x.getChild(0));
				expr.add(tmp);
				String as = x.getChild(1).getText();
				alias.add(as);
				hasRename = true;
				hasAggr |= tmp.hasAggr();

				for (int i = 0; i < orderField.size(); i++)
					if (orderField.get(i).equalsIgnoreCase(as))
						orderField.set(i, tmp.toString());
				
				if (groupBy !=null && groupBy.equalsIgnoreCase(as))
					groupBy = tmp.toString();
			} else if (x.getText().equals("*") && x.getChildCount() == 0) {
				hasProjAll = true;
			} else {
				Expr tmp = getExpr(x);
				expr.add(tmp);
				alias.add(tmp.toString());
				hasAggr |= tmp.hasAggr();
			}
		}

		if(src == null) src = new ConstPlan();
		if(!hasAggr && having != null)
			pred = pred == null? having: new BinaryExpr(pred, BinaryOp.AND, having);
		
		ans = src;
		if (pred != null) {
			if (!pred.isConst)
				ans = new Select(src, pred);
			else if (!pred.evalPred(new Env()))
				ans = new Select(src, pred);
		}

		if (hasAggr)
			ans = new Group(ans, expr, groupBy, having, alias, hasOrder, hasAs);
		if (hasOrder)
			ans = new Order(ans, expr, orderField, orderType);
		if (!expr.isEmpty())
			ans = new Project(ans, expr, hasProjAll);
		if (hasRename)
			ans = new Rename(ans, alias);
		if (t.getType() == SELECT_DISTINCT)
			ans = new Distinct(ans);

		return optFlag ? Optimize.optimize(ans) : ans;
	}

	public static Plan transFrom(BaseTree t) {
		Plan ans = null;

		for (Object o : t.getChildren()){
			Tree x = (Tree) o;
			String table = null;
			Plan src = null;
			String as = null;
			if (x.getType() == FatwormParser.AS) {
				if (x.getChild(0).getType() == SELECT || x.getChild(0).getType() == SELECT_DISTINCT)
					src = transSelect((BaseTree) x.getChild(0), true);
				else table = x.getChild(0).getText();
				as = x.getChild(1).getText();
			} else table = x.getText();
			if (src == null) src = new FetchTable(table);
			if (as != null) src = new TableRename(src, as);
			ans = (ans == null) ? src : new Product(ans, src);
		}
		return ans;
	}
}