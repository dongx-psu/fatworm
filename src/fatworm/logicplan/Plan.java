package fatworm.logicplan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fatworm.expr.FuncCall;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;
import fatworm.util.Util;

public abstract class Plan {
	public Plan parent;
	public Set<FuncCall> myAggr;
	public boolean hasEval;
	
	public Plan() {
		this.parent = null;
		hasEval = false;
		myAggr = new HashSet<FuncCall>();
	}
	
	public Plan(Plan parent) {
		this();
		this.parent = parent;
	}
	
	public Set<FuncCall> getAggr() {
		return myAggr;
	}
	
	public abstract void eval(Env env);
	
	public abstract String toString();
	
	public abstract boolean hasNext();
	
	public abstract Record next();
	
	public abstract void reset();
	
	public abstract Schema getSchema();
	
	public abstract void close();
	
	public abstract List<String> getColumns();
	
	public abstract List<String> getRequestedColumns();
	
	public abstract void rename(String oldName, String newName);
	
	public void setSrc(Plan oldChild, Plan newChild) {
		newChild.parent = this;
		assert newChild!=this;
		Plan plan = this;
		if (plan instanceof Distinct) {
			Distinct p = (Distinct)plan;
			p.src = newChild;
		} else if (plan instanceof FetchTable) {
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			p.src = newChild;
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			if (p.left == oldChild) p.left = newChild;
			else if (p.right == oldChild) p.right = newChild;
		} else if (plan instanceof None) {
		} else if (plan instanceof ConstPlan) {
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			p.src = newChild;
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			p.src = newChild;
		} else if (plan instanceof Rename) {
			Rename p = (Rename)plan;
			p.src = newChild;
		} else if (plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			p.src = newChild;
		} else if (plan instanceof Select) {
			Select p = (Select)plan;
			p.src = newChild;
		} else if (plan instanceof Join) {
			Join p = (Join)plan;
			if (p.left == oldChild) p.left = newChild;
			else if (p.right == oldChild) p.right = newChild;
		} else {
			Util.warn("Error in setSrc");
		}
	}
}
