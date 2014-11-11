package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class CHAR extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1802222419588091754L;
	public String value;
	
	public CHAR() {
		java_type = java.sql.Types.CHAR;
	}
	
	public CHAR(String x) {
		this();
		value = Util.trim(x);
	}
	
	public String toString() {
		return "'" + value + "'";
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		return Type.cmpString(op, this.toString(), x.toString());
	}
	
	public int hashCode() {
		return value.hashCode();
	}
	@Override
	public void pushByte(ByteBuilder b, int A, int B) {
		String z = value.substring(0, Math.max(0, Math.min(value.length(), A)));
		b.putString(z);
	}
}
