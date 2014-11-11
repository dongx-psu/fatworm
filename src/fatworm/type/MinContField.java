package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.io.ByteBuilder;

public class MinContField extends ContField {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3747458114006701967L;
	Type res;
	boolean isNull;

	public MinContField(){
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
		if (res.compWith(BinaryOp.GT, x))
			res = x;
	}

	public Type getFinalResults(){
		return isNull? NULL.getInstance(): res;
	}

	@Override
	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
