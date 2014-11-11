package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class VARCHAR extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2215502901711012177L;
	public String value;
	
	public VARCHAR() {
		java_type = java.sql.Types.VARCHAR;
	}
	
	public VARCHAR(String x) {
		this();
		value = Util.trim(x);
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		return Type.cmpString(op, this.toString(), x==null ? "" : x.toString());
	}
	
	public String toString() {
		return "'" + value + "'";
	}
	
	public int hashCode() {
		return value.hashCode();
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		String res = value.substring(0, Math.max(0, Math.min(value.length(), A)));
		b.putString(res);
	}
}
