package fatworm.table;

import java.io.Serializable;

import fatworm.type.Type;

public class Column implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3475154961393230209L;
	public String name;
	public int type;
	public boolean notNull, autoIncrement, primaryKey;
	public int ai;
	public Object defaultValue;
	public int A, B;
	
	public Column(String n, int ty, boolean notNull, boolean autoIncrement, Type defaultValue) {
		this.name = n;
		this.type = ty;
		this.notNull = notNull;
		this.autoIncrement = autoIncrement;
		this.defaultValue = defaultValue;
		this.ai = 1;
	}
	
	public Column(String n, int ty) {
		this(n, ty, false, false, null);
	}
	
	public boolean isAutoInc() {
		return autoIncrement;
	}
	
	public int getAutoInc() {
		return ai++;
	}
	
	public Type getDefault() {
		return Type.fromObject(defaultValue);
	}
	
	public boolean hasDefault() {
		return defaultValue != null;
	}
	
	public String toString(){
		return name;
	}
	
	public boolean equals(Object o){
		if (o instanceof Column)
			return name.equalsIgnoreCase(((Column)o).name);
		else return name.equalsIgnoreCase(o.toString());
	}
}
