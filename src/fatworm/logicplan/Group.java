package fatworm.logicplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fatworm.expr.Expr;
import fatworm.expr.FuncCall;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.type.ContField;
import fatworm.type.NULL;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class Group extends Plan {
	public Plan src;
	public String by;
	public int byIdx = -1;
	
	public Expr having;
	public List<Expr> func;
	
	public int ptr;
	public List<Record> results;
	public List<String> expandedCols = new ArrayList<String>();
	
	List<String> alias;
	boolean hasAlias;
	
	public Schema schema;

	List<String> evalNameList;
	List<Integer> evalColList;
	Set<String> neededAttr;
	
	public Group(Plan src, List<Expr> func, String by, Expr having, List<String> alias, boolean expand, boolean hasAlias) {
		super();
		this.src = src;
		this.by = by;
		this.having = having;
		this.func = func;
		src.parent = this;
		this.hasAlias = hasAlias;
		ptr = 0;
		this.alias = alias;
		neededAttr = new HashSet<String> ();
		
		for (int i = 0; i < func.size(); i++){
			myAggr.addAll(func.get(i).getAggr());
			List<String> tmp = func.get(i).getRequestedColumns();
			for(String s : tmp){
				neededAttr.add(s.toLowerCase());
			}
		}

		if (this.having != null) {
			myAggr.addAll(this.having.getAggr());
			List<String> tmp = having.getRequestedColumns();
			for (String s : tmp) {
				neededAttr.add(s.toLowerCase());
			}
		}
		
		this.schema = new Schema();
		this.schema.fromList(func, src.getSchema());
		
		Schema scm = src.getSchema();
		for (String colName : scm.columnName) {
			String ocol = colName;
			colName = Util.getAttr(colName);
			if (this.schema.columnDef.containsKey(colName) || this.schema.columnDef.containsKey(ocol))
				continue;
			if (!neededAttr.contains(ocol.toLowerCase()) && !neededAttr.contains(colName.toLowerCase()))
				continue;
			this.schema.columnDef.put(colName, scm.getColumn(ocol));
			this.schema.columnName.add(colName);
			this.expandedCols.add(colName);
		}
		if (by != null) byIdx = scm.findIndex(by);
	}
	
	public void eval(Env envGlobal) {
		hasEval = true;
		results = new ArrayList<Record>();
		ptr = 0;
		
		Env env = envGlobal.clone();
		src.eval(env);
		LinkedList<FuncCall> evalAggr = new LinkedList<FuncCall>();
		evalColList = new ArrayList<Integer>();
		for (FuncCall f : myAggr) {
			if (schema.findStrictIndex(f.col) >=0) {
				env.remove(f.toString());
				evalAggr.add(f);
				evalColList.add(src.getSchema().findStrictIndex(f.col));
			}
		}

		LinkedList<Record> tmpTable = new LinkedList<Record>();
		Map<Type, List<ContField>> aggrCont = new HashMap<Type, List<ContField>>();
		while (src.hasNext()) {
			Record r = src.next();
			Type tt = (by == null) ? NULL.getInstance(): r.getCol(byIdx);
			List<ContField> tmp = aggrCont.get(tt);
			if (tmp == null) {
				tmp = new LinkedList<ContField>();
				int i = 0;
				for (FuncCall a : evalAggr) {
					ContField cont = a.evalCont(null, r.cols.get(evalColList.get(i++)));
					tmp.add(cont);
				}
				aggrCont.put(tt, tmp);
			} else {
				int i = 0;
				for (FuncCall f : evalAggr) {
					ContField cont = tmp.get(i);
					f.evalCont(cont, r.cols.get(evalColList.get(i++)));
				}
			}
			tmpTable.addLast(r);
		}

		Map<Type, Env> aggrEnv = new HashMap<Type, Env>();
		for (Map.Entry<Type, List<ContField>> e : aggrCont.entrySet()) {
			Env tmp = new Env();
			int i = 0;
			for (FuncCall f : evalAggr) {
				ContField cont = e.getValue().get(i++);
				tmp.put(f.toString(), cont);
			}
			aggrEnv.put(e.getKey(), tmp);
		}

		evalNameList = new ArrayList<String>();
		evalColList = new ArrayList<Integer>();
		for (String s : neededAttr) {
			evalNameList.add(s);
			evalColList.add(src.getSchema().findStrictIndex(s));
		}
		
		Map<Type, Record> groupRecord = new HashMap<Type, Record>();
		while (!tmpTable.isEmpty()) {
			Record r = tmpTable.pollFirst();
			Type f = (by == null) ? NULL.getInstance(): r.getCol(byIdx);
			
			Record pr = groupRecord.get(f);
			if (pr == null) {
				Env tmp = aggrEnv.get(f);
				tmp.appendFromRecord(evalNameList, evalColList, r.cols);
				pr = new Record(schema);
				groupRecord.put(f, pr);
				pr.addColFromExpr(tmp, func);
				for (int i = 0; i < expandedCols.size(); i++){
					pr.addCol(tmp.get(expandedCols.get(i)));
				}
			}
		}
		
		if(!hasAlias){
			for (Type f : groupRecord.keySet()) {
				Record r = groupRecord.get(f);
				Env tmp = aggrEnv.get(f);
				if (having==null || having.evalPred(tmp))
					results.add(r);
			}
		}else{
			Env tmp = null;
			for(Type f : groupRecord.keySet()){
				Record r = groupRecord.get(f);
				tmp = aggrEnv.get(f);
				tmp.appendAlias(schema.tableName, func, alias);
				if (having==null || having.evalPred(tmp)) {
					results.add(r);
				}
			}
			env.appendFrom(tmp);
		}
		if (results.size() == 0 && (having == null || having.evalPred(env))) {
			Record pr = new Record(schema);
			pr.addColFromExpr(new Env(), func);
			for (int i = 0; i < expandedCols.size(); i++)
				pr.addCol(env.get(expandedCols.get(i)));
			results.add(pr);
		}
	}
	
	public String toString(){
		return "Group (from="+src.toString()+", by=" + by + ", having=" + (having == null? "null": having.toString())+", expr="+Util.deepToString(func)+", myAggr="+Util.deepToString(myAggr)+")";
	}
	
	@Override
	public boolean hasNext() {
		if (!hasEval) Util.error("Group not eval");
		return ptr != results.size();
	}

	@Override
	public Record next() {
		if (!hasEval) Util.error("Group not eval");
		return results.get(ptr++);
	}

	@Override
	public void reset() {
		ptr = 0;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		src.close();
		results = new ArrayList<Record>();
	}

	@Override
	public List<String> getColumns() {
		return new LinkedList<String> (schema.columnName);
	}

	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = src.getRequestedColumns();
		Util.removeAllCol(ans, src.getColumns());
		if (having!=null)
			Util.addAllCol(ans, having.getRequestedColumns());
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		// FIXME
		src.rename(oldName, newName);
	}
}
