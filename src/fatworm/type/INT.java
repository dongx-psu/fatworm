package fatworm.type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;
import fatworm.util.Util;

public class INT extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5312804518553498913L;
	public int value;
	
	public INT(){
		this.java_type = java.sql.Types.INTEGER;
	}
	public INT(int v) {
		this();
		this.value = v;
	}
	
	public INT(ByteBuffer b){
		this();
		value = b.getInt();
	}

	public INT(String x) {
		this();
		value = new BigDecimal(Util.trim(x)).intValueExact();
	}

	public BigDecimal toDecimal() {
		return new BigDecimal(value).setScale(10);
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		BigDecimal v = toDecimal();
		switch (x.java_type) {
		case java.sql.Types.DECIMAL:
			return Type.cmpHelper(op, v, ((DECIMAL)x).value);
		case java.sql.Types.INTEGER:
			return Type.cmpHelper(op, value, ((INT)x).value);
		case java.sql.Types.FLOAT:
			return Type.cmpHelper(op, Float.valueOf(value), ((FLOAT)x).value);
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return Type.cmpHelper(op, v, (new INT(x.toString())).toDecimal());
		}
		return false;
	}
	
	public String toString() {
		return "" + value;
	}
	
	public int hashCode() {
		return value;
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
		b.putInt(value);
	}
}
