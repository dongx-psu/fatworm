package fatworm.logicplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import fatworm.driver.DBEngine;
import fatworm.expr.BinaryExpr;
import fatworm.expr.BinaryOp;
import fatworm.expr.ID;
import fatworm.io.Cursor;
import fatworm.table.IOTable;
import fatworm.table.Index;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.table.Table;
import fatworm.type.INT;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class FetchTable extends Plan {
	public String tableName;
	public Table table;
	Schema schema;
	Cursor cursor;
	public List<String> orderField = new LinkedList<String>();
	
	public FetchTable(String table) {
		super();
		tableName = table;
		this.table = DBEngine.getInstance().getTable(table);
		if (table == null) Util.error("Table is null!!");
		
		this.schema = new Schema(this.table.getSchema().tableName);
		for(String old:this.table.getSchema().columnName){
			String now = this.table.schema.tableName + "." + Util.getAttr(old);
			schema.columnDef.put(now, this.table.getSchema().getColumn(old));
			schema.columnName.add(now);
		}
		
		schema.primaryKey = this.table.getSchema().primaryKey;
	}
	
	public void eval(Env env) {
		hasEval = true;
		
		if (parent instanceof Select) {
			List<Condition> condList = new ArrayList<Condition>();
			Map<String, List<Condition>> conditions = new HashMap<String, List<Condition>>();
			Map<String, Interval> intervals = new HashMap<String, Interval>();
			
			Select cur = (Select) parent;	
			while (true){
				if (cur.pred instanceof BinaryExpr) {
					BinaryExpr cond = (BinaryExpr)cur.pred;
					if (cond.op != BinaryOp.NEQ && (cond.l instanceof ID && cond.r.isConst) || (cond.r instanceof ID && cond.l.isConst)) {
						Condition tmp = new Condition((BinaryExpr)cur.pred);
						condList.add(tmp);
						if (!conditions.containsKey(tmp.name))
							conditions.put(tmp.name, new ArrayList<Condition>());
						conditions.get(tmp.name).add(tmp);
					}
				}
				
				if (cur.parent instanceof Select)
					cur = (Select)cur.parent;
				else break;
			}

			boolean nonEmpty = true;
			for (Map.Entry<String, List<Condition>> e : conditions.entrySet()) {
				List<Condition> curConds = e.getValue();
				Interval x = new Interval();
				for (Condition c : curConds) {
					x.intersect(c.getInteval());
					if (x.isEmpty()) {
						nonEmpty = false;
						break;
					}
				}
				if (!nonEmpty) break;
				intervals.put(e.getKey(), x);
			}
			
			if (!nonEmpty) {
				cursor = new EmptyCursor();
				return;
			}

			List<String> condName = new LinkedList<String>(intervals.keySet());
			Interval bestInterval = null;
			int minRange = Integer.MAX_VALUE;
			Index bestIdx = null;
			
			for (String name : condName)
				if (table.hasIndexOn(name)) {
					Index index = table.getIndexOn(name);
					Interval interval = intervals.get(name);
					int tmp = interval.getRange();
					if(tmp < minRange||bestInterval==null){
						minRange = tmp;
						bestInterval = interval;
						bestIdx = index;
					}
				}
			
			if (bestInterval != null) {
				if (table instanceof IOTable) {
					boolean isMaxEQ = false; //margin condition xx <= const
					cur = (Select) parent;
					while(true){
						if(cur.pred instanceof BinaryExpr){
							BinaryExpr cond = (BinaryExpr)cur.pred;
							if ((cond.op == BinaryOp.GEQ || cond.op == BinaryOp.LEQ) &&
								((cond.l instanceof ID && cond.r.isConst) || (cond.r instanceof ID && cond.l.isConst))){
								Condition tmp = new Condition((BinaryExpr)cur.pred);
								if (tmp.name.equalsIgnoreCase(bestIdx.column.name)) {
									cur.flag = true;
									if (!Interval.isNull(bestInterval.max) && tmp.value.compWith(BinaryOp.EQ, bestInterval.max))
										isMaxEQ = true;
								}
							}
						}
						if(cur.parent instanceof Select)
							cur = (Select)cur.parent;
						else 
							break;
					}
					IOTable iot = (IOTable) table;
					cursor = iot.scope(bestIdx, bestInterval.min, bestInterval.max, isMaxEQ);
					return;
				}
			}
		}
		
		cursor = table.open();
	}
	
	public String toString() {
		return "Table (from=" + tableName + ")";
	}
	
	@Override
	public boolean hasNext() {
		return cursor.hasThis();
	}

	@Override
	public Record next() {
		Record ans = cursor.fetchRecord();
		try {
			cursor.next();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return ans;
	}

	@Override
	public void reset() {
		try {
			cursor.reset();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		cursor.close();
	}

	@Override
	public List<String> getColumns() {
		List<String> ans = new LinkedList<String> (schema.columnName);
		return ans;
	}

	@Override
	public List<String> getRequestedColumns() {
		return new LinkedList<String>();
	}
	
	@Override
	public void rename(String oldName, String newName) {
	}
	
	private static class Condition {
		public BinaryOp op;
		public String name;
		public Type value;
		
		public Condition(BinaryExpr e) {
			op = e.op;
			if (e.l instanceof ID) {
				name = Util.getAttr(((ID)e.l).name);
				value = e.r.eval(new Env());
			} else {
				name = Util.getAttr(((ID)e.r).name);
				value = e.l.eval(new Env());
			}
		}
		
		public Interval getInteval() {
			switch (op) {
			case EQ:
				return new Interval(value, value);
			case LT:
			case LEQ:
				return new Interval(null, value);
			case GT:
			case GEQ:
				return new Interval(value, null);
			default:
				return null;
			}
		}
	}

	private static class Interval {
		public Type min;
		public Type max;
		
		public Interval(){
			min = null;
			max = null;
		}
		
		public Interval(Type l, Type r){
			min = l;
			max = r;
		}
		
		public void intersect(Interval x){
			if (isNull(min) || (!isNull(x.min) && min.compWith(BinaryOp.LT, x.min)))
				min = x.min;
			if (isNull(max) || (!isNull(x.max) && max.compWith(BinaryOp.GT, x.max)))
				max = x.max;
		}
		
		public static boolean isNull(Type x){
			return x == null || x.java_type == java.sql.Types.NULL;
		}
		
		public boolean isEmpty(){
			return !isNull(min) && !isNull(max) && min.compWith(BinaryOp.GT, max);
		}
		
		public int getRange(){
			if (min instanceof INT && max instanceof INT) {
				int range = ((INT)max).value - ((INT)min).value;
				if (range >= 0) return range;
			}
			
			if (!isNull(min) && !isNull(max) && min.compWith(BinaryOp.EQ, max))
				return 0;
			return Integer.MAX_VALUE;
		}
	}

	private static class EmptyCursor implements Cursor{
		@Override
		public void reset() throws Throwable {
		}

		@Override
		public void next() throws Throwable {
		}

		@Override
		public void prev() throws Throwable {
		}

		@Override
		public Object fetch(String col) {
			return null;
		}

		@Override
		public Object[] fetch() {
			return null;
		}

		@Override
		public void delete() throws Throwable {
		}

		@Override
		public void close() {
		}

		@Override
		public boolean hasNext() throws Throwable {
			return false;
		}

		@Override
		public Record fetchRecord() {
			return null;
		}

		@Override
		public Integer getIdx() {
			return null;
		}

		@Override
		public boolean hasThis() {
			return false;
		}

	}
}
