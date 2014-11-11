package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;

public class NULL extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5731859038315942005L;
	private static NULL instance = null;
	
	public NULL() {
		java_type = java.sql.Types.NULL;
	}
	
	public synchronized static Type getInstance() {
		if (instance == null) instance = new NULL();
		return instance;
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		return false;
	}
	
	public String toString() {
		return "NULL";
	}
	
	public int hashCode() {
		return 0;
	}
	
	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
