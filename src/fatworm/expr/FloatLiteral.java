package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.type.FLOAT;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class FloatLiteral extends Expr {
	public FLOAT i;
	
	public FloatLiteral(double v) {
		super();
		this.isConst = true;
		this.size = 1;
		this.i = new FLOAT(v);
		this.value = this.i;
		java_type = java.sql.Types.FLOAT;
	}
	
	public String toString() {
		return value.toString();
	}
	
	public boolean evalPred(Env env) {
		return Util.toBoolean(i);
	}
	
	public Type eval(Env env) {
		return i;
	}
	
	public List<String> getRequestedColumns() {
		return new LinkedList<String>();
	}
	
	public void rename(String oldName, String newName) {
	}
	
	public boolean hasSubquery() {
		return false;
	}
	
	public Expr clone() {
		return new FloatLiteral(i.value);
	}
}
