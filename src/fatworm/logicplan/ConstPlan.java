package fatworm.logicplan;

import java.util.LinkedList;
import java.util.List;

import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;

public class ConstPlan extends Plan {
	public Schema schema;
	int ptr = 0;
	
	public ConstPlan() {
		super();
		schema = new Schema("CONST_SCHEMA");
	}
	
	public void eval(Env env) {
		hasEval = true;
		reset();
	}
	
	public String toString() {
		return "Const Plan";
	}
	
	public boolean hasNext() {
		return ptr == 0;
	}
	
	public Record next() {
		if (ptr > 0) return null;
		ptr++;
		return new Record(schema);
	}
	
	public void reset() {
		ptr = 0;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public void close() {
		ptr = 1;
	}
	
	public List<String> getColumns() {
		return new LinkedList<String>();
	}
	
	public List<String> getRequestedColumns() {
		return new LinkedList<String> ();
	}
	
	public void rename(String oldName, String newName) {

	}
}
