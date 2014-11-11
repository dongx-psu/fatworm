package fatworm.logicplan;

import java.util.LinkedList;
import java.util.List;

import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;
import fatworm.util.Util;

public class Rename extends Plan {
	public List<String> as;
	public Plan src;

	public Schema schema;

	public Rename(Plan src, List<String> as) {
		super();
		this.src = src;
		this.as = as;
		src.parent = this;
		myAggr.addAll(this.src.getAggr());
		this.schema = new Schema(src.getSchema().tableName);
		for (int i = 0; i < src.getSchema().columnName.size(); i++) {
			String old = src.getSchema().columnName.get(i);
			String now = schema.tableName + "." + Util.getAttr(as.get(i));
			schema.columnDef.put(now, src.getSchema().getColumn(old));
			schema.columnName.add(now);
		}
		schema.primaryKey = src.getSchema().primaryKey;
	}

	@Override
	public String toString(){
		return "rename (from="+src.toString()+", as="+Util.deepToString(as)+")";
	}

	@Override
	public void eval(Env env) {
		hasEval = true;
		src.eval(env);
	}

	@Override
	public boolean hasNext() {
		return src.hasNext();
	}

	@Override
	public Record next() {
		Record r = src.next();
		Record ans = new Record(schema);
		ans.cols.addAll(r.cols);
		return ans;
	}

	@Override
	public void reset() {
		src.reset();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		src.close();
	}

	@Override
	public List<String> getColumns() {
		return new LinkedList<String> (as);
	}

	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = src.getRequestedColumns();
		Util.removeAllCol(ans, src.getColumns());
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		src.rename(oldName, newName);
	}
}
