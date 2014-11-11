package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.parser.FatwormParser;
import fatworm.table.Schema;
import fatworm.type.ContField;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class FuncCall extends Expr{
	public int func;
	public String col;
	public boolean hasEvalCont;
	
	public FuncCall(int func, String col) {
		super();
		this.col = col;
		this.func = func;
		myAggr.add(this);
		hasEvalCont = false;
		java_type = java.sql.Types.NULL;
		if (func == FatwormParser.AVG || func == FatwormParser.SUM)
			java_type = java.sql.Types.DECIMAL;
		if (func == FatwormParser.COUNT)
			java_type = java.sql.Types.INTEGER;
	}
	
	public String toString() {
		return Util.getFuncName(func) + "(" + col + ")";
	}
	
	public boolean evalPred(Env env) {
		return Util.toBoolean(eval(env));
	}

	public Type eval(Env env) {
		if (!hasEvalCont) Util.warn("Do not have anything to aggregate!!!");
		Type f = env.get(this.toString());
		if (f == null) {
			f = ContField.newContField(func);
			env.put(this.toString(), f);
		}
		
		if (f instanceof ContField) {
			return ((ContField) f).getFinalResults();
		}
		return f;
	}
	
	public void evalCont(Env env) {
		hasEvalCont = true;
		Type f = env.get(this.toString());
		if (f == null) {
			f = ContField.newContField(func);
			env.put(this.toString(), f);
		}
		if (f instanceof ContField) {
			((ContField) f).applyWithAggr(env.get(col));
		} else {
			Util.error("evalCont is evaluating something not a contField");
		}
	}
	
	public ContField evalCont(ContField f, Type val) {
		hasEvalCont = true;
		if (f == null){
			f = ContField.newContField(func);
		}
		if (f instanceof ContField){
			((ContField) f).applyWithAggr(val);
		} else {
			Util.error("evalCont is evaluating something not a contField");
		}
		return f;
	}
	
	@Override
	public int hashCode(){
		return toString().hashCode();
	}
	@Override
	public boolean equals(Object o){
		if (o instanceof FuncCall) {
			return o.toString().equals(toString());
		}
		return false;
	}
	
	public boolean canEvalOn(Schema schema){
		return schema.findStrictIndex(col)>=0;
	}
	
	public int getType(Schema schema) {
		if (java_type == java.sql.Types.NULL) {
			java_type = schema.getColumn(col).type;
		}
		return java_type;
	}

	public List<String> getRequestedColumns() {
		List<String> ans = new LinkedList<String> ();
		ans.add(col);
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		if (col.equalsIgnoreCase(oldName))
			col = newName;
	}

	@Override
	public boolean hasSubquery() {
		return false;
	}

	@Override
	public Expr clone() {
		return new FuncCall(func, col);
	}
}
