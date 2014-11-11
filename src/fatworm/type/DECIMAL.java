package fatworm.type;

import java.math.BigDecimal;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class DECIMAL extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6376750750520381670L;
	public BigDecimal value;
	
	public DECIMAL() {
		java_type = java.sql.Types.DECIMAL;
	}
	
	public DECIMAL(String x) {
		this();
		value = new BigDecimal(Util.trim(x)).setScale(10, BigDecimal.ROUND_HALF_EVEN);
	}
	
	public DECIMAL(BigDecimal x) {
		this();
		value = x.setScale(10, BigDecimal.ROUND_HALF_EVEN);
	}
	
	public boolean compWith(BinaryOp op, Type x) {
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
	
	public String toString() {
		return "'" + value.toString() + "'";
	}
	
	public int hashCode() {
		return value.hashCode();
	}
	
	public BigDecimal toDecimal(){
		return value;
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		int os = value.scale();
		b.putInt(os);
		b.putBytes(value.unscaledValue().toByteArray());
	}
}
