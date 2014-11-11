package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.logicplan.Plan;
import fatworm.type.NULL;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class QueryCall extends Expr {
	public Plan src;
	public QueryCall(Plan src) {
		super();
		this.src = src;
		myAggr.addAll(this.src.getAggr());
		java_type = src.getSchema().getColumn(0).type;
	}
	
	public boolean evalPred(Env env) {
		return Util.toBoolean(eval(env));
	}
	
	public Type eval(Env env) {
		src.eval(env);
		if (src.hasNext())
			return src.next().cols.get(0);
		else return NULL.getInstance();
	}
	
	public String toString() {
		return "@QueryCall("+src.toString() +")";
	}
	
	public List<String> getRequestedColumns() {
		return new LinkedList<String>();
	}
	
	public void rename(String oldName, String newName) {

	}
	
	public boolean hasSubquery() {
		return true;
	}
	
	public Expr clone() {
		return new QueryCall(src);
	}
}
