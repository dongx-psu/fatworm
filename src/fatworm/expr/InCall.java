package fatworm.expr;

import java.util.List;

import fatworm.logicplan.Plan;
import fatworm.util.Env;
import fatworm.util.Util;
import fatworm.type.BOOL;
import fatworm.type.Type;

public class InCall extends Expr {
	public boolean not;
	public Expr left;
	public Plan right;
	
	public InCall(Expr left, Plan right, boolean not) {
		super();
		this.left = left;
		this.right = right;
		this.not = not;
		myAggr.addAll(this.left.getAggr());
		myAggr.addAll(this.right.getAggr());
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public boolean evalPred(Env env) {
		Type l = left.eval(env);
		right.eval(env);
		boolean ans = false;
		while (right.hasNext())
			if (l.compWith(BinaryOp.EQ, right.next().cols.get(0))) {
				ans = true;
				break;
			}
		
		return not ^ ans;
	}
	
	public Type eval(Env env) {
		return new BOOL(evalPred(env));
	}
	
	public String toString() {
		return left.toString() + (not? " not ":" ") + "in " + right.toString();
	}
	
	public List<String> getRequestedColumns() {
		List<String> ans = right.getRequestedColumns();
		Util.addAllCol(ans, left.getRequestedColumns());
		return ans;
	}
	
	public void rename(String oldName, String newName) {
	}
	
	public boolean hasSubquery() {
		return false;
	}
	
	public Expr clone() {
		return new InCall(left.clone(), right, not);
	}
}
