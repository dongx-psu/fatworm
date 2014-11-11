package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.type.Type;
import fatworm.type.VARCHAR;
import fatworm.util.Env;
import fatworm.util.Util;

public class StringLiteral extends Expr {
	public String s;
	
	public StringLiteral(String s) {
		super();
		this.isConst = true;
		this.s = s;
		this.value = new VARCHAR(s);
		this.size = 1;
		java_type = java.sql.Types.VARCHAR;
	}
	
	public String toString() {
		return '"' +  value.toString() + '"';
	}
	
	public boolean evalPred(Env env) {
		return Util.toBoolean(s);
	}

	public Type eval(Env env) {
		return value;
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
		return new StringLiteral(s);
	}

}
