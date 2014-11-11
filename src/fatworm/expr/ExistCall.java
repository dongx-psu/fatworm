package fatworm.expr;

import java.util.List;

import fatworm.logicplan.Plan;
import fatworm.type.BOOL;
import fatworm.type.Type;
import fatworm.util.Env;

public class ExistCall extends Expr {
	public boolean not;
	public Plan src;
	
	public ExistCall(Plan src, boolean not) {
		super();
		this.src = src;
		this.not = not;
		myAggr.addAll(this.src.getAggr());
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public boolean evalPred(Env env) {
		src.eval(env);
		return not ^ src.hasNext();
	}
	
	public Type eval(Env env) {
		return new BOOL(evalPred(env));
	}
	
	public String toString() {
		return (not? "not ":"") + "exist " + src.toString();
	}
	
	public List<String> getRequestedColumns() {
		return src.getRequestedColumns();
	}
	
	public void rename(String oldName, String newName) {
		
	}
	
	public boolean hasSubquery() {
		return true;
	}
	
	public Expr clone() {
		return new ExistCall(src, not);
	}
}
