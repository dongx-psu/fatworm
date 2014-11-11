package fatworm.expr;

import java.util.List;

import fatworm.logicplan.Plan;
import fatworm.table.Schema;
import fatworm.type.BOOL;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class AnyAllCall extends Expr {
	public boolean isAll;
	public Plan right;
	public Expr left;
	public BinaryOp op;
	
	public AnyAllCall(Expr left, BinaryOp op, Plan right, boolean isAll) {
		super();
		this.left =left;
		this.right = right;
		this.op = op;
		this.isAll = isAll;
		myAggr.addAll(this.left.getAggr());
		myAggr.addAll(this.right.getAggr());
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public boolean evalPred(Env env) {
		Type l = left.eval(env);
		right.eval(env);
		if (isAll) {
			while (right.hasNext()) {
				Type r = right.next().cols.get(0);
				if (!l.compWith(op, r)) return false;
			}
			return true;
		} else {
			while (right.hasNext()) {
				if(l.compWith(op, right.next().cols.get(0)))return true;
			}
			return false;
		}
	}
	
	public Type eval(Env env) {
		return new BOOL(evalPred(env));
	}
	
	public int getType(Schema schema) {
		return java_type;
	}
	
	public List<String> getRequestedColumns() {
		List<String> ans = right.getRequestedColumns();
		Util.addAllCol(ans, left.getRequestedColumns());
		return ans;
	}
	
	public String toString() {
		return left.toString() + op.toString() + (isAll ? "all " : "any ") + right.toString();
	}
	
	public void rename(String oldName, String newName) {
		
	}
	
	public boolean hasSubquery(){
		return true;
	}
	
	public Expr clone() {
		return new AnyAllCall(left.clone(), op, right, isAll);
	}
}
