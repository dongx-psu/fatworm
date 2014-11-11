package fatworm.type;

import fatworm.expr.BinaryOp;
import fatworm.parser.FatwormParser;

public abstract class ContField extends Type {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6073021756596105001L;

	public ContField() {
		
	}
	
	public boolean compWith(BinaryOp op, Type x) {
		return getFinalResults().compWith(op, x);
	}
	
	public abstract void applyWithAggr(Type x);
	
	public abstract Type getFinalResults();
	
	public static ContField newContField(int f) {
		switch (f) {
		case FatwormParser.AVG:
			return new AvgContField();
		case FatwormParser.COUNT:
			return new CountContField();
		case FatwormParser.MAX:
			return new MaxContField();
		case FatwormParser.MIN:
			return new MinContField();
		case FatwormParser.SUM:
			return new SumContField();
			default:
				System.err.println("ContField never reach.");
				return null;
		}
	}
	
	public String toString(){
		return getFinalResults().toString();
	}
}
