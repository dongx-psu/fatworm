package fatworm.type;

import java.math.BigDecimal;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class BOOL extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = -958834853571738858L;
	public boolean value;
	public BOOL(boolean x) {
		this.value = x;
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public BOOL(String x) {
		this.value = Util.toBoolean(x);
		java_type = java.sql.Types.BOOLEAN;
	}
	
	public String toString() {
		return value ? "true" : "false";
	}
	
	public int hashCode() {
		return value ? 1 : 0;
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		switch (x.java_type) {
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.DECIMAL:
		case java.sql.Types.FLOAT:
			return Type.cmpHelper(op, toDecimal(), x.toDecimal());
		}
		System.err.println("compWith Type Miss");
		return false;
	}
	
	public BigDecimal toDecimal() {
		return new BigDecimal(value ? 1 : 0).setScale(10);
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		b.putBool(value);
	}
}
