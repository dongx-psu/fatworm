package fatworm.type;

import fatworm.io.ByteBuilder;

public class CountContField extends ContField {
	/**
	 * 
	 */
	private static final long serialVersionUID = 670619352594477063L;
	int cnt;

	public CountContField(){
		cnt = 0;
	}

	public void applyWithAggr(Type x){
		cnt++;
	}

	public Type getFinalResults(){
		return new INT(cnt);
	}

	@Override
	public void pushByte(ByteBuilder b, int A, int B) {
	}
}
