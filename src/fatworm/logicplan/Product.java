package fatworm.logicplan;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;
import fatworm.util.Util;

public class Product extends Plan {
	public Plan left, right;
	public Record currentLeft;
	public Schema schema;
	
	public Product(Plan left, Plan right) {
		super();
		this.left = left;
		this.right = right;
		this.left.parent = this;
		this.right.parent = this;
		currentLeft = null;
		myAggr.addAll(this.left.getAggr());
		myAggr.addAll(this.right.getAggr());
		
		this.schema = new Schema("table" + Env.getNewTemp());
		this.schema.isJoin = true;
		Set<String> common = new HashSet<String> (left.getSchema().columnName);
		common.retainAll(right.getSchema().columnName);
		
		String ltbl = left.getSchema().tableName;
		for (String colName : left.getSchema().columnName) {
			if (common.contains(colName)) {
				this.schema.columnName.add(ltbl + "." + colName);
				this.schema.columnDef.put(ltbl + "." + colName, left.getSchema().columnDef.get(colName));
			} else {
				this.schema.columnName.add(colName);
				this.schema.columnDef.put(colName, left.getSchema().columnDef.get(colName));
			}
		}
		String rtbl = right.getSchema().tableName;
		for (String colName : right.getSchema().columnName) {
			if (common.contains(colName)) {
				this.schema.columnName.add(rtbl + "." + colName);
				this.schema.columnDef.put(rtbl + "." + colName, right.getSchema().columnDef.get(colName));
			} else {
				this.schema.columnName.add(colName);
				this.schema.columnDef.put(colName, right.getSchema().columnDef.get(colName));
			}
		}
		
	}

	@Override
	public void eval(Env envGlobal) {
		hasEval = true;
		currentLeft = null;
		Env env = envGlobal.clone();
		left.eval(env);
		right.eval(env);
	}
	
	@Override
	public String toString() {
		return "Product (left="+left.toString()+", right="+right.toString()+")";
	}

	@Override
	public boolean hasNext() {
		if (!hasEval) Util.error("Join not eval");
		return (currentLeft!=null || left.hasNext()) && right.hasNext();
	}

	@Override
	public Record next() {
		if (!hasEval) Util.error("Join not eval");
		
		Record r = right.next();
		if (currentLeft == null)
			currentLeft = left.next();
		
		Record ans = new Record(schema);
		ans.cols.addAll(currentLeft.cols);
		ans.cols.addAll(r.cols);
		if (!right.hasNext() && left.hasNext()) {
			right.reset();
			currentLeft = left.next();
		}
		
		return ans;
	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
		currentLeft = null;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		left.close();
		right.close();
	}

	@Override
	public List<String> getColumns() {
		List<String> ans = new LinkedList<String> (left.getColumns());
		ans.addAll(right.getColumns());
		return ans;
	}

	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = new LinkedList<String> (left.getRequestedColumns());
		ans.addAll(right.getRequestedColumns());
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		left.rename(oldName, newName);
		right.rename(oldName, newName);
	}
}
