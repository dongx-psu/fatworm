package fatworm.expr;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import fatworm.type.DECIMAL;
import fatworm.type.INT;
import fatworm.type.Type;
import fatworm.util.Env;

public class IntLiteral extends Expr {
	public Type i;
	
	public IntLiteral(BigInteger v) {
		super();
		this.isConst = true;
		this.size = 1;
		this.i = new DECIMAL(v.toString());
		this.value = this.i;
	}
	
	public IntLiteral(int v) {
		super();
		this.isConst = true;
		this.size = 1;
		this.i = new INT(v);
		this.value = this.i;
		java_type = java.sql.Types.INTEGER;
	}
	
	public boolean evalPred(Env env) {
		return i.compWith(BinaryOp.EQ, new INT(0)) ? true : false;
	}
	
	public Type eval(Env env) {
		return i;
	}
	
	public String toString() {
		return value.toString();
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
		return (java_type == java.sql.Types.INTEGER) ? 
				new IntLiteral(((INT)i).value):
				new IntLiteral(((DECIMAL)i).value.toBigIntegerExact());
	}
}
