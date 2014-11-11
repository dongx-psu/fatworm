package fatworm.logicplan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import fatworm.expr.BinaryOp;
import fatworm.expr.Expr;
import fatworm.parser.FatwormParser;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class Order extends Plan {
	public Plan src;
	public List<String> orderField;
	public List<Integer> orderType;
	public List<Integer> orderIdx;
	public List<Record> results;
	public List<Expr> expr;
	public List<String> expandedCols = new ArrayList<String>();
	public Schema schema;
	public int ptr;

	public Order(Plan src, List<Expr> func, List<String> a, List<Integer> b) {
		super();
		this.src = src;
		src.parent = this;
		orderField = a;
		orderType = b;
		myAggr.addAll(this.src.getAggr());
		
		this.expr = func;
		this.schema = new Schema();
		this.schema.fromList(func, src.getSchema());
		
		Schema scm = src.getSchema();
		Set<String> neededAttr = new HashSet<String>();
		for (String x : orderField)
			neededAttr.add(Util.getAttr(x).toLowerCase());
		
		for (String colName : scm.columnName){
			if (this.schema.columnDef.containsKey(colName) || !neededAttr.contains(Util.getAttr(colName).toLowerCase()))
				continue;
			this.schema.columnDef.put(colName, scm.getColumn(colName));
			this.schema.columnName.add(colName);
			this.expandedCols.add(colName);
		}
		
		orderIdx = new ArrayList<Integer>();
		for (String col : orderField)
			orderIdx.add(this.schema.findIndex(col));
	}

	@Override
	public void eval(Env envGlobal) {
		hasEval = true;
		ptr = 0;
		results = new ArrayList<Record>();
		Env env = envGlobal.clone();
		src.eval(env);
		while (src.hasNext()) {
			Record r = src.next();
			env.appendFromRecord(r);
			Record pr = new Record(schema);
			pr.addColFromExpr(env, expr);
			for (int i = 0; i < expandedCols.size(); i++)
				pr.addCol(env.get(expandedCols.get(i)));
			
			results.add(pr);
		}
		
		Collections.sort(results, new Comparator<Record>(){
			public int compare(Record a, Record b){
				for (int i = 0; i < orderIdx.size(); i++) {
					Type l = a.getCol(orderIdx.get(i));
					Type r = b.getCol(orderIdx.get(i));
					if (orderType.get(i) == FatwormParser.ASC){
						if (l.compWith(BinaryOp.GT, r)) return 1;
						if (l.compWith(BinaryOp.LT, r))	return -1;
					} else {
						if (l.compWith(BinaryOp.GT, r)) return -1;
						if (l.compWith(BinaryOp.LT, r)) return 1;
					}
				}
				return 0;
			}
		});
	}
	@Override
	public String toString(){
		return "order (from="+src.toString()+", field="+Util.deepToString(orderField)+", type="+Util.deepToString(orderType)+")";
	}

	@Override
	public boolean hasNext() {
		if (!hasEval) Util.error("Order not eval");
		return ptr != results.size();
	}

	@Override
	public Record next() {
		if (!hasEval) Util.error("Order not eval");
		return results.get(ptr++);
	}

	@Override
	public void reset() {
		ptr = 0;
	}

	@Override
	public Schema getSchema() {
		return src.getSchema();
	}

	@Override
	public void close() {
		src.close();
		results = new ArrayList<Record>();
	}

	@Override
	public List<String> getColumns() {
		return src.getColumns();
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
