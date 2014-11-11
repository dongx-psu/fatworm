package fatworm.expr;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fatworm.table.Schema;
import fatworm.type.Type;
import fatworm.util.Env;

public abstract class Expr {
	public Integer size, depth;
	public boolean isConst;
	public Type value;
	public Set<FuncCall> myAggr;
	public int java_type;
	
	public Expr() {
		size = 1;
		depth = 1;
		isConst = false;
		value = null;
		myAggr = new HashSet<FuncCall>();
	}
	
	public abstract boolean evalPred(Env env);
	
	public abstract Type eval(Env env);
	
	public abstract String toString();
	
	public Set<FuncCall> getAggr() {
		return myAggr;
	}
	
	public boolean hasAggr() {
		return !myAggr.isEmpty();
	}
	
	public int getType(Schema schema) {
		return java_type;
	}
	
	public int getType() {
		return java_type;
	}
	
	public abstract List<String> getRequestedColumns();
	
	public abstract void rename(String oldName, String newName);
	
	public abstract boolean hasSubquery();
	
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	public boolean isAnd(){
		return this instanceof BinaryExpr && ((BinaryExpr)this).op == BinaryOp.AND;
	}
	public void collectCondition(Collection<Expr> c){
		if (!isAnd()) {
			c.add(this);
		} else {
			((BinaryExpr)this).l.collectCondition(c);
			((BinaryExpr)this).r.collectCondition(c);
		}
	}
	
	public boolean equals(Object o) {
		if (o instanceof Expr) {
			Expr e = (Expr) o;
			return toString().equalsIgnoreCase(e.toString());
		}
		return false;
	}
	
	public abstract Expr clone();
}
