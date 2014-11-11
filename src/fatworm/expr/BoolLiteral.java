package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.table.Schema;
import fatworm.type.BOOL;
import fatworm.type.Type;
import fatworm.util.Env;

public class BoolLiteral extends Expr {
	public BOOL i;
	
	public BoolLiteral(boolean v) {
		super();
		this.isConst = true;
		this.size = 1;
		this.i = new BOOL(v);
		value = this.i;
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public String toString() {
		return value.toString();
	}
	
	public boolean evalPred(Env env) {
		return i.value;
	}
	
	public Type eval(Env env) {
		return i;
	}
	
	public int getType(Schema schema) {
		return java_type;
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
		return new BoolLiteral(i.value);
	}
}
