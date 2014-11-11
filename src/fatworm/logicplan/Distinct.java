package fatworm.logicplan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;
import fatworm.util.Util;

public class Distinct extends Plan {
	public Plan src;
	List<Record> results;
	int ptr;
	
	public Distinct(Plan src) {
		super();
		this.src = src;
		src.parent = this;
		
		myAggr.addAll(this.src.getAggr());
	}
	
	public void eval(Env envGlobal) {
		hasEval = true;
		results = new ArrayList<Record>();
		ptr = 0;
		Env env = envGlobal.clone();
		src.eval(env);
		Set<Record> set = new HashSet<Record>();
		while (src.hasNext()) {
			Record r = src.next();
			if (!set.contains(r)) {
				results.add(r);
				set.add(r);
			}
		}
	}
	
	public String toString() {
		return "distinct (from=" + src.toString() + ")";
	}
	
	public boolean hasNext() {
		if (!hasEval) System.err.println("Distinct not eval");
		return ptr != results.size();
	}
	
	public Record next() {
		if (!hasEval) System.err.println("Distinct not eval");
		return results.get(ptr++);
	}
	
	public void reset() {
		ptr = 0;
	}
	
	public Schema getSchema() {
		return src.getSchema();
	}
	
	public void close() {
		src.close();
		results = new ArrayList<Record>();
	}
	
	public List<String> getColumns() {
		return src.getColumns();
	}
	
	public List<String> getRequestedColumns() {
		List<String> ans = src.getRequestedColumns();
		Util.removeAllCol(ans, src.getRequestedColumns());
		return ans;
	}
	
	public void rename(String oldName, String newName) {
		src.rename(oldName, newName);
	}
}
