package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class DATE extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4471598531992748208L;
	public java.sql.Timestamp value;
	
	public DATE() {
		java_type = java.sql.Types.DATE;
	}
	
	public DATE(java.sql.Timestamp x) {
		this();
		value = x;
	}
	
	public DATE(Long x) {
		this();
		value = new java.sql.Timestamp(x);
	}
	
	public DATE(String x) {
		this();
		value = Util.stringToTimestamp(Util.trim(x));
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		switch (x.java_type) {
		case java.sql.Types.TIMESTAMP:
			return Type.cmpHelper(op, value, ((TIMESTAMP)x).value);
		case java.sql.Types.DATE:
			return Type.cmpHelper(op, value, ((DATE)x).value);
		case java.sql.Types.CHAR:
			return Type.cmpHelper(op, value, java.sql.Timestamp.valueOf(((CHAR)x).value));
		case java.sql.Types.VARCHAR:
			return Type.cmpHelper(op, value, java.sql.Timestamp.valueOf(((VARCHAR)x).value));
		}
		return false;
	}
	
	public String toString() {
		return "'" + value.toString() + "'";
	}
	
	public int hashCode() {
		return value.hashCode();
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		b.putLong(value.getTime());
	}
}
