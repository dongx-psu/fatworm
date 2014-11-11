package fatworm.type;

import java.math.BigDecimal;

import fatworm.io.ByteBuilder;

public class AvgContField extends ContField {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8252499337275693351L;
	BigDecimal res;
	int cnt;
	boolean isNull;
	
	public AvgContField(){
		cnt = 0;
		isNull=true;
		res = new BigDecimal(0).setScale(10);
	}
	
	public void applyWithAggr(Type x){
		if (x instanceof NULL || x.java_type == java.sql.Types.NULL)
			return;
		isNull = false;
		res = res.add(x.toDecimal());
		cnt++;
	}
	
	public Type getFinalResults(){
		return isNull? NULL.getInstance() : new DECIMAL(res.divide(new BigDecimal(cnt), 10, BigDecimal.ROUND_HALF_EVEN));
	}

	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
