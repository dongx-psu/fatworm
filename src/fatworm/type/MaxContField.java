package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;

public class MaxContField extends ContField{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Type res;
	boolean isNull;

	public MaxContField(){
		res = NULL.getInstance();
		isNull = true;
	}

	public void applyWithAggr(Type x){
		if (x instanceof NULL || x.java_type == java.sql.Types.NULL)
			return;
		if (isNull) {
			res = x;
			isNull = false;
			return;
		}
		if (res.compWith(BinaryOp.LT, x))
			res = x;
	}

	public Type getFinalResults(){
		return isNull? NULL.getInstance(): res;
	}

	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
