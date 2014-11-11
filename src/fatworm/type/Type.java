package fatworm.type;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;

public abstract class Type implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2580873377338747658L;
	public int java_type;
	public Type() {
	}
	
	public boolean equals(Object o) {
		if (o == null) return false;
		return this.toString().equals(o.toString());
	}
	
	public static Type fromString(int type, String x) {
		if (x.equalsIgnoreCase("null"))
			return NULL.getInstance();
		switch (type) {
		case java.sql.Types.BOOLEAN:
			return new BOOL(x);
		case java.sql.Types.CHAR:
			return new CHAR(new String(x));
		case java.sql.Types.DATE:
			return new DATE(x);
		case java.sql.Types.DECIMAL:
			return new DECIMAL(x);
		case java.sql.Types.FLOAT:
			return new FLOAT(x);
		case java.sql.Types.INTEGER:
			return new INT(x);
		case java.sql.Types.NULL:
			return NULL.getInstance();
		case java.sql.Types.TIMESTAMP:
			return new TIMESTAMP(x);
		case java.sql.Types.VARCHAR:
			return new VARCHAR(new String(x));
			default:
				return null;
		}
	}
	
	public static <T> boolean cmpHelper(BinaryOp op, Comparable<T> a, T b) {
		switch(op) {
		case LT:
			return a.compareTo(b) < 0;
		case LEQ:
			return a.compareTo(b) <= 0;
		case EQ:
			return a.compareTo(b) == 0;
		case NEQ:
			return a.compareTo(b) != 0;
		case GT:
			return a.compareTo(b) > 0;
		case GEQ:
			return a.compareTo(b) >= 0;
			default:
				System.err.println("Cmp Missing OP");
		}
		return false;
	}
	
	public static boolean cmpString(BinaryOp op, String a, String b){
		switch(op){
		case LT:
			return a.compareToIgnoreCase(b)<0;
		case LEQ:
			return a.compareToIgnoreCase(b)<=0;
		case EQ:
			return a.compareToIgnoreCase(b)==0;
		case NEQ:
			return a.compareToIgnoreCase(b)!=0;
		case GT:
			return a.compareToIgnoreCase(b)>0;
		case GEQ:
			return a.compareToIgnoreCase(b)>=0;
			default:
				System.err.println("Cmp Missing OP");
		}
		return false;
	}
	
	public abstract boolean compWith(BinaryOp op, Type x);
	
	public BigDecimal toDecimal(){
		return null;
	}
	
	public static Object getObject(Type t) {
		if (t instanceof ContField)
			t = ((ContField) t).getFinalResults();
		if (t == null) return null;
		int type = t.java_type;
		switch(type){
		case java.sql.Types.BOOLEAN:
			return new Boolean(((BOOL)t).value);
		case java.sql.Types.CHAR:
			return new String(((CHAR)t).value);
		case java.sql.Types.DATE:
			return new java.sql.Timestamp(((DATE)t).value.getTime());
		case java.sql.Types.DECIMAL:
			return ((DECIMAL)t).toDecimal();
		case java.sql.Types.FLOAT:
			return new Float(((FLOAT)t).value);
		case java.sql.Types.INTEGER:
			return new Integer(((INT)t).value);
		case java.sql.Types.TIMESTAMP:
			return new java.sql.Timestamp(((TIMESTAMP)t).value.getTime());
		case java.sql.Types.VARCHAR:
			return new String(((VARCHAR)t).value);
		case java.sql.Types.NULL:
			return null;
			default:
				return new String(t.toString());
		}
	}
	
	public static Type fromObject(Object o) {
		if (o instanceof Type)
			return (Type) o;
		else if (o instanceof Boolean)
			return new BOOL((Boolean)o);
		else if (o instanceof String && ((String)o).length() == 1)
			return new CHAR((String)o);
		else if (o instanceof java.sql.Date)
			return new DATE(new java.sql.Timestamp(((java.sql.Date)o).getTime()));
		else if (o instanceof BigDecimal)
			return new DECIMAL((BigDecimal)o);
		else if (o instanceof Float)
			return new FLOAT((Float)o);
		else if (o instanceof Integer)
			return new INT((Integer)o);
		else if (o instanceof java.sql.Timestamp)
			return new TIMESTAMP((java.sql.Timestamp)o);
		else if (o instanceof String)
			return new VARCHAR((String)o);
		else return NULL.getInstance();
	}
	
	public static Type fromBytes(byte[] y){
		ByteBuffer buf = java.nio.ByteBuffer.wrap(y);
		int type = buf.getInt();
		return fromBytes(buf, type);
	}

	public static Type fromBytes(ByteBuffer buf, int type){
		int length = 0;
		switch (type) {
		case java.sql.Types.BOOLEAN:
			return new BOOL(buf.get()==0?false:true);
		case java.sql.Types.CHAR:
			length = buf.getInt();
			byte [] dst = new byte[length];
			buf.get(dst, 0, length);
			return new CHAR(new String(dst));
		case java.sql.Types.DATE:
			return new DATE(buf.getLong());
		case java.sql.Types.DECIMAL:
			int scale = buf.getInt();
			length = buf.getInt();
			dst = new byte[length];
			buf.get(dst, 0, length);
			return new DECIMAL(new BigDecimal(new BigInteger(dst), scale));
		case java.sql.Types.FLOAT:
			return new FLOAT(buf.getFloat());
		case java.sql.Types.INTEGER:
			return new INT(buf.getInt());
		case java.sql.Types.NULL:
			return NULL.getInstance();
		case java.sql.Types.TIMESTAMP:
			return new TIMESTAMP(buf.getLong());
		case java.sql.Types.VARCHAR:
			length = buf.getInt();
			dst = new byte[length];
			try {
				buf.get(dst, 0, length);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return new VARCHAR(new String(dst));
			default:
				return NULL.getInstance();
		}
	}

	public abstract void pushByte(ByteBuilder b, int A, int B);
}
