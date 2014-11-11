package fatworm.expr;

import java.util.List;

import fatworm.table.Schema;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.ExprEvalHelper;
import fatworm.util.Util;
import static java.sql.Types.INTEGER;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DECIMAL;

public class BinaryExpr extends Expr {
	
	public Expr l, r;
	public BinaryOp op;
	String myName;
    
	public BinaryExpr(Expr left, BinaryOp op, Expr right) {
		super();
		l = left;
		r = right;
		this.op = op;
		size = l.size + r.size + 1;
		depth = Math.max(l.depth,r.depth) + 1;
		isConst = l.isConst && r.isConst;
		value = isConst ? eval() : null;
		myAggr.addAll(l.getAggr());
		myAggr.addAll(r.getAggr());
		java_type = evalType(null);
	}
	
	private Type eval() {
		return ExprEvalHelper.evalHelper(l.value, op, r.value);
	}

	public boolean evalPred(Env env){
		Type x = eval(env);
		return Util.toBoolean(x);
	}
	
	@Override
	public String toString(){
		return l.toString() + op.toString() + r.toString();
	}
	
	@Override
	public boolean equals(Object o){
		if (o instanceof BinaryExpr) {
			BinaryExpr e = (BinaryExpr) o;
			return op.equals(e.op)
				&& ((l.depth == e.l.depth && l.equals(e.l) && r.equals(e.r)) || 
					(Util.isCommutative(op) && l.depth == e.r.depth && l.equals(e.r) && r.equals(e.l)));
		}
		return false;
	}

	@Override
	public int hashCode(){
		return Util.isCommutative(op)?l.hashCode() ^ r.hashCode() ^ op.hashCode():toString().hashCode();
	}
	
	@Override
	public Type eval(Env env) {
		if (isConst) return value;
		Type lval = l.eval(env);
		Type rval = r.eval(env);
		return ExprEvalHelper.evalHelper(lval, op, rval);
	}
	
	public int evalType(Schema src){
		int lt, rt;
		lt = src ==null ? l.getType() : l.getType(src);
		rt = src == null ? r.getType() : r.getType(src);
		switch(op){
		case LT:
		case GT:
		case LEQ:
		case GEQ:
		case EQ:
		case NEQ:
		case AND:
		case OR:
			return java.sql.Types.BOOLEAN;
		case ADD:
		case MINUS:
		case MUL:
			if (lt == INTEGER && rt ==INTEGER)
				return INTEGER;
			else if (lt == DECIMAL || rt == DECIMAL)
				return DECIMAL;
			else if (lt == FLOAT || rt == FLOAT)
				return FLOAT;
			else return java.sql.Types.NULL;
		case DIV:
			if (lt==INTEGER && rt == INTEGER)
				return FLOAT;
			else if (lt== DECIMAL || rt == DECIMAL)
				return DECIMAL;
			else if (lt== FLOAT || rt == FLOAT)
				return FLOAT;
			else return java.sql.Types.NULL;
		case MOD:
			return INTEGER;
		default:
			Util.error("Missing ops");
		}
		return java.sql.Types.NULL;
	}

	@Override
	public int getType(Schema schema) {
		if (java_type == java.sql.Types.NULL)
			return evalType(schema);
		return java_type;
	}
	
	public BinaryExpr toCNF(){
		Expr left = (l instanceof BinaryExpr)?((BinaryExpr)l).toCNF() : l;
		Expr right = (r instanceof BinaryExpr)?((BinaryExpr)r).toCNF() : r;
		BinaryExpr ans = null;
		if (this.op == BinaryOp.OR) {
			if (left.isAnd()) {
				ans = new BinaryExpr(
						new BinaryExpr(((BinaryExpr)left).l.clone(), BinaryOp.OR, right.clone()),
						BinaryOp.AND,
						new BinaryExpr(((BinaryExpr)left).r.clone(), BinaryOp.OR, right.clone())
						);
			} else if (right.isAnd()) {
				ans = new BinaryExpr(
						new BinaryExpr(left.clone(), BinaryOp.OR, ((BinaryExpr)right).l.clone()),
						BinaryOp.AND,
						new BinaryExpr(left.clone(), BinaryOp.OR, ((BinaryExpr)right).r.clone())
						);
			}
		}
		this.l = left;
		this.r = right;
		if (ans == null) ans = this;
		else ans = ans.toCNF();
		return ans;
	}
	


	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = l.getRequestedColumns();
		Util.addAllCol(ans, r.getRequestedColumns());
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		this.l.rename(oldName, newName);
		this.r.rename(oldName, newName);
	}

	@Override
	public boolean hasSubquery() {
		return this.l.hasSubquery() || this.r.hasSubquery();
	}

	@Override
	public Expr clone() {
		return new BinaryExpr(l.clone(), op, r.clone());
	}
}
