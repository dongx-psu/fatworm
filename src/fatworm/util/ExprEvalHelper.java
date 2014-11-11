package fatworm.util;

import java.math.BigDecimal;

import fatworm.expr.BinaryOp;
import fatworm.type.BOOL;
import fatworm.type.DECIMAL;
import fatworm.type.FLOAT;
import fatworm.type.INT;
import fatworm.type.NULL;
import fatworm.type.Type;
import static java.sql.Types.INTEGER;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DECIMAL;

public class ExprEvalHelper {
	public static Type evalHelper(Type left, BinaryOp op, Type right) {
		switch (op) {
		case LT:
		case GT:
		case LEQ:
		case GEQ:
		case EQ:
		case NEQ:
			return new BOOL(left.compWith(op, right));
		case ADD:
			return add(left, right);
		case MINUS:
			return minus(left, right);
		case MUL:
			return mult(left, right);
		case DIV:
			return div(left, right);
		case MOD:
			return mod(left, right);
		case AND:
			return new BOOL(Util.toBoolean(left) && Util.toBoolean(right));
		case OR:
			return new BOOL(Util.toBoolean(left) || Util.toBoolean(right));
		default:
			Util.error("Missing ops in ExprEvalHelper");
		}
		return NULL.getInstance();
	}

	private static Type add(Type left, Type right) {
		if (left.java_type == INTEGER && right.java_type == INTEGER)
			return new INT(((INT)left).value + ((INT)right).value);
		
		BigDecimal ans = left.toDecimal().add(right.toDecimal());
		if (left.java_type == DECIMAL || right.java_type == DECIMAL)
			return new DECIMAL(ans);
		if (left.java_type == FLOAT || right.java_type == FLOAT)
			return new FLOAT(ans.floatValue());
		return NULL.getInstance();
	}
	
	private static Type minus(Type left, Type right) {
		BigDecimal ans = left.toDecimal().subtract(right.toDecimal());
		if (left.java_type == INTEGER && right.java_type == INTEGER)
			return new INT(ans.intValue());
		if (left.java_type == DECIMAL || right.java_type == DECIMAL)
			return new DECIMAL(ans);
		if (left.java_type == FLOAT || right.java_type == FLOAT)
			return new FLOAT(ans.floatValue());
		return NULL.getInstance();
	}
	
	private static Type mult(Type left, Type right) {
		if (left.java_type == INTEGER && right.java_type == INTEGER)
			return new INT(((INT)left).value * ((INT)right).value);
		
		BigDecimal ans = left.toDecimal().multiply(right.toDecimal());
		if (left.java_type == DECIMAL || right.java_type == DECIMAL)
			return new DECIMAL(ans);
		if (left.java_type == FLOAT || right.java_type == FLOAT)
			return new FLOAT(ans.floatValue());
		return NULL.getInstance();
	}
	
	private static Type div(Type left, Type right) {
		BigDecimal ans = left.toDecimal().divide(right.toDecimal(), 9, BigDecimal.ROUND_HALF_EVEN);
		if (left.java_type == INTEGER && right.java_type == INTEGER)
			return new FLOAT(ans.floatValue());
		if (left.java_type == DECIMAL || right.java_type == DECIMAL)
			return new DECIMAL(ans);
		if (left.java_type == FLOAT || right.java_type == FLOAT)
			return new FLOAT(ans.floatValue());
		return NULL.getInstance();
	}
	
	private static Type mod(Type left, Type right) {
		if (left.java_type == INTEGER && right.java_type == INTEGER) {
			int a = ((INT)left).value;
			int b = ((INT)right).value;
			return new INT(a % b);
		}
		return NULL.getInstance();
	}
}
