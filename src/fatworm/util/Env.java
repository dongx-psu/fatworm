package fatworm.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fatworm.expr.Expr;
import fatworm.table.Record;
import fatworm.type.NULL;
import fatworm.type.Type;

public class Env {
	public Map<String, Type> res;
	private static int cnt = 0;
	
	public synchronized static int getNewTemp() {
		return cnt++;
	}
	
	public Env() {
		res = new HashMap<String, Type>();
	}
	
	public Env(Map<String, Type> r) {
		res = r;
	}
	
	public void put(String a, Type b) {
		res.put(a.toLowerCase(), b);
	}
	
	public Type get(String a) {
		return res.get(a.toLowerCase());
	}
	
	public Type get(String tbl, String col) {
		Type res = get(col);
		if (res == null) res = get(tbl + "." + Util.getAttr(col));
		if (res == null) res = NULL.getInstance();
		return res;
	}
	
	public Type remove(String a) {
		return res.remove(a.toLowerCase());
	}
	
	public Env clone() {
		return new Env(new HashMap<String, Type>(res));
	}
	
	public Type remove(String tbl, String col) {
		Type ret = null;
		String key = tbl + "." + Util.getAttr(col);
		if (res.containsKey(col.toLowerCase()))
			ret = res.remove(col.toLowerCase());
		if (res.containsKey(key.toLowerCase()))
			ret = res.remove(key.toLowerCase());
		return ret;
	}
	
	
	public void appendFromRecord(Record x){
		if(x == null) return;
		List<String> name1 = x.schema.getCName1();
		List<String> name2 = x.schema.getCName2();
		for (int i = 0; i < x.cols.size(); i++){
			put(name1.get(i), x.cols.get(i));
			put(name2.get(i), x.cols.get(i));
		}
	}
	public void appendFromRecord(List<String> name, List<Integer> offset, List<Type> cols){
		for (int i = 0;i < name.size(); i++){
			int j = offset.get(i);
			if (j < 0) continue;
			put(name.get(i), cols.get(j));
		}
	}
	
	public void appendFromRecord(List<String> name1, List<String> name2, Record x){
		if (x == null)return;
		for (int i = 0; i < x.cols.size(); i++){
			put(name1.get(i), x.cols.get(i));
			put(name2.get(i), x.cols.get(i));
		}
	}
	
	public void appendAlias(String tbl, List<Expr> expr, List<String> alias){
		for (int i = 0; i < alias.size(); i++) {
			String colName = expr.get(i).toString();
			put(alias.get(i), get(tbl, colName));
			put(tbl + "." + alias.get(i), get(colName));
		}
	}
	
	public void appendFrom(Env x){
		if (x == null)return;
		res.putAll(x.res);
	}
	
	@Override
	public String toString(){
		assert(res != null);
		return Util.deepToString(res);
	}
}
