package fatworm.expr;

import java.util.LinkedList;
import java.util.List;

import fatworm.table.Column;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.type.NULL;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class ID extends Expr {
	public String name;
	public int idx = -1;
	
	public ID(String s) {
		super();
		this.name = s;
		this.size = 1;
	}
	
	public String toString() {
		return name;
	}
	
	public boolean evalPred(Env env) {
		return Util.toBoolean(eval(env));
	}
	
	public Type eval(Env env) {
		if (name.equalsIgnoreCase("null"))
			return NULL.getInstance();
		Type res = env.get(name);
		if (res == null) {
			res = env.get(Util.getAttr(name));
			if (res == null)
				Util.warn("Id not found:" + name + "," + env.toString());
		}
		return res;
	}
	
	public int getType(Schema schema) {
		Column c = schema.getColumn(name);
		if (c != null)
			return schema.getColumn(name).type;
		return java.sql.Types.NULL;
	}
	
	public List<String> getRequestedColumns() {
		List<String> ans = new LinkedList<String>();
		ans.add(name);
		return ans;
	}
	
	public void rename(String oldName, String newName) {
		if ((!name.contains(".") && Util.getAttr(oldName).equalsIgnoreCase(Util.getAttr(name))) || oldName.equalsIgnoreCase(name))
			name = newName;
	}
	
	public boolean hasSubquery() {
		return false;
	}
	
	public Expr clone() {
		return new ID(name);
	}
	
	public void fillIndex(Schema schema) {
		idx = schema.findStrictIndex(name);
	}
	
	public Type evalByIndex(Record r) {
		return r.cols.get(idx);
	}
}
