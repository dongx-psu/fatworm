package fatworm.type;

import java.math.BigDecimal;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class FLOAT extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2791977867805154759L;
	public float value;
	
	public FLOAT() {
		java_type = java.sql.Types.FLOAT;
	}
	
	public FLOAT(String x) {
		this();
		value = Float.valueOf(Util.trim(x));
	}
	
	public FLOAT(float x) {
		this();
		value = x;
	}
	
	public FLOAT(double x) {
		this();
		value = (float) x;
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		BigDecimal value = toDecimal();
		switch (x.java_type) {
		case java.sql.Types.DECIMAL:
			return Type.cmpHelper(op, value, ((DECIMAL)x).value);
		case java.sql.Types.INTEGER:
			return Type.cmpHelper(op, value, ((INT)x).toDecimal());
		case java.sql.Types.FLOAT:
			return Type.cmpHelper(op, value, ((FLOAT)x).toDecimal());
		}
		return false;
	}
	
	public BigDecimal toDecimal() {
		return new BigDecimal(value).setScale(10, BigDecimal.ROUND_HALF_EVEN);
	}
	
	public String toString() {
		return "" + value;
	}
	
	public int hashCode() {
		return new Float(value).hashCode();
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		b.putFloat(value);
	}
}
