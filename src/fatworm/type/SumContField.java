package fatworm.type;

import java.math.BigDecimal;

import fatworm.io.ByteBuilder;

public class SumContField extends ContField {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8048548139804865321L;
	BigDecimal res;
	boolean isNull;

	public SumContField(){
		isNull=true;
		res = new BigDecimal(0).setScale(10);
	}

	public void applyWithAggr(Type x){
		if (x instanceof NULL || x.java_type == java.sql.Types.NULL)
			return;
		isNull = false;
		res = res.add(x.toDecimal());
	}

	public Type getFinalResults(){
		return isNull? NULL.getInstance() : 
			new DECIMAL(res);
	}

	@Override
	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
