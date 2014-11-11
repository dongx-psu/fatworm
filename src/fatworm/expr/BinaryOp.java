package fatworm.expr;

public enum BinaryOp {
	MUL, DIV, MOD, ADD, MINUS, LT, LEQ, GT, GEQ, EQ, NEQ, AND, OR;
	
	public String toString() {
	    switch(this) {
	      case MUL: return "*";
	      case DIV: return "/";
	      case MOD: return "%";
	      case ADD: return "+";
	      case MINUS: return "-";
	      case LT: return "<";
	      case LEQ: return "<=";
	      case GT: return ">";
	      case GEQ: return ">=";
	      case EQ: return "=";
	      case NEQ: return "<>";
	      case AND: return "and";
	      case OR: return "or";
	      default: throw new IllegalArgumentException();
	    }
	}
	
	public static BinaryOp getBinaryOp(String ops){
		BinaryOp op = null;
		if (ops.equals("+"))
			op = BinaryOp.ADD;
		else if (ops.equals("-"))
			op = BinaryOp.MINUS;
		else if (ops.equals("*"))
			op = BinaryOp.MUL;
		else if (ops.equals("/"))
				op = BinaryOp.DIV;
		else if (ops.equals("%"))
			op = BinaryOp.MOD;
		else if (ops.equals("="))
			op = BinaryOp.EQ;
		else if (ops.equals("<"))
			op = BinaryOp.LT;
		else if (ops.equals(">"))
			op = BinaryOp.GT;
		else if (ops.equals("<="))
			op = BinaryOp.LEQ;
		else if (ops.equals(">="))
			op = BinaryOp.GEQ;
		else if (ops.equals("<>"))
			op = BinaryOp.NEQ;
		return op;
	}
}