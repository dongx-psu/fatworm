package fatworm.optimize;

import java.util.List;

import fatworm.expr.BinaryExpr;
import fatworm.expr.BinaryOp;
import fatworm.expr.Expr;
import fatworm.expr.ID;
import fatworm.expr.InCall;
import fatworm.expr.QueryCall;
import fatworm.logicplan.ConstPlan;
import fatworm.logicplan.Distinct;
import fatworm.logicplan.FetchTable;
import fatworm.logicplan.Group;
import fatworm.logicplan.Join;
import fatworm.logicplan.None;
import fatworm.logicplan.Order;
import fatworm.logicplan.Plan;
import fatworm.logicplan.Product;
import fatworm.logicplan.Project;
import fatworm.logicplan.Rename;
import fatworm.logicplan.Select;
import fatworm.logicplan.TableRename;
import fatworm.util.Util;

public class SelectCleanup {
	public static final String tablePrefix = "__FATWORM__";
	
	public static void process(Plan plan) {
		if (plan instanceof Distinct) {
			Distinct p = (Distinct)plan;
			process(p.src);
		} else if (plan instanceof FetchTable) {
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			process(p.src);
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			process(p.left);
			process(p.right);
		} else if (plan instanceof None) {
		} else if (plan instanceof ConstPlan) {
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			process(p.src);
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			cleanupProject(p);
			process(p.src);
		} else if(plan instanceof Rename) {
			Rename p = (Rename)plan;
			process(p.src);
		} else if(plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			process(p.src);
		} else if(plan instanceof Select) {
			Select p = (Select)plan;
			process(p.src);
			cleanupSelect(p);
			
		} else if(plan instanceof Join){
			Join p = (Join)plan;
			process(p.left);
			process(p.right);
		} else {
			Util.warn("Err in SelectCleanup");
		}
	}

	private static void cleanupSelect(Select p) {
		if (p.pred instanceof InCall) {
			InCall tmp1 = (InCall) p.pred;
			if (tmp1.right instanceof Project) {
				Project tmp2 = (Project) tmp1.right;
				String name1 = Util.getAttr(tmp2.expr.get(0).toString());
				String name2 = Util.getAttr(tmp1.left.toString());
				if (tmp2.src instanceof FetchTable && tmp2.expr.get(0) instanceof ID && 
					((!name1.equalsIgnoreCase(name2) || tmp1.left.toString().contains(".")))) {
					String tbl = tablePrefix + tmp2.getSchema().tableName;
					p.src = new Product(p.src, new TableRename(tmp2, tbl));
					p.src.parent = p;
					String col = tbl + "." + Util.getAttr(tmp2.expr.get(0).toString());
					p.pred = new BinaryExpr(tmp1.left, BinaryOp.EQ, new ID(col));
				}
			}
		}
	}

	private static void cleanupProject(Project p) {
		List<Expr> expr = p.expr;
		for (int i = 0; i < expr.size(); i++){
			Expr e = expr.get(i);
			if (e instanceof QueryCall && ((QueryCall)e).src instanceof Project) {
				Project tmp = (Project) ((QueryCall)e).src;
				if (tmp.src instanceof ConstPlan)
					expr.set(i, tmp.expr.get(0));
			}
		}
	}
}
