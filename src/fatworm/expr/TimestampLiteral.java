package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.type.DATE;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class TimestampLiteral extends Expr {
	public DATE i;
	
	public TimestampLiteral(java.sql.Timestamp v) {
		super();
		this.isConst = true;
		this.size = 1;
		this.i = new DATE(v);
		this.value = this.i;
		java_type = java.sql.Types.TIMESTAMP;
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
		return new TimestampLiteral(i.value);
	}
}
