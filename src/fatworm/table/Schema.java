package fatworm.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import fatworm.expr.Expr;
import fatworm.parser.FatwormParser;
import fatworm.type.Type;
import fatworm.util.Util;

public class Schema implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7480301916138990739L;
	public String tableName;
	public List<String> columnName = new ArrayList<String>();
	
	public Column primaryKey;
	public Map<String, Column> columnDef = new HashMap<String, Column>();
	public boolean isJoin = false;
	
	private List<String> cName1 = null;
	private List<String> cName2 = null;
	
	public Schema() {
		tableName = "SCHEMA";
	}
	
	public Schema(String s) {
		this();
		tableName = s;
	}
	
	public Schema(CommonTree t){
		tableName = t.getChild(0).getText();
		for (int i = 1; i < t.getChildCount(); i++){
			Tree c = t.getChild(i);
			String colName = c.getChild(0).getText();
			switch (c.getType()) {
			case FatwormParser.CREATE_DEFINITION:
				columnName.add(colName);
				Tree def = c.getChild(1);
				Column col = null;
				switch (def.getType()) {
				case FatwormParser.BOOLEAN:
					col = new Column(colName, java.sql.Types.BOOLEAN);
					break;
				case FatwormParser.CHAR:
					col = new Column(colName, java.sql.Types.CHAR);
					col.A = Integer.parseInt(def.getChild(0).getText());
					break;
				case FatwormParser.DATETIME:
					col = new Column(colName, java.sql.Types.DATE);
					break;
				case FatwormParser.DECIMAL:
					col = new Column(colName, java.sql.Types.DECIMAL);
					break;
				case FatwormParser.FLOAT:
					col = new Column(colName, java.sql.Types.FLOAT);
					break;
				case FatwormParser.INT:
					col = new Column(colName, java.sql.Types.INTEGER);
					break;
				case FatwormParser.TIMESTAMP:
					col = new Column(colName, java.sql.Types.TIMESTAMP);
					break;
				case FatwormParser.VARCHAR:
					col = new Column(colName, java.sql.Types.VARCHAR);
					col.A = Integer.parseInt(def.getChild(0).getText());
					break;
					default:
						System.err.println("Schema undefined def.");
				}
				columnDef.put(colName, col);
				for (int j = 2; j < c.getChildCount(); j++){
					Tree opt = c.getChild(j);
					switch (opt.getType()) {
					case FatwormParser.AUTO_INCREMENT:
						col.autoIncrement = true;
						break;
					case FatwormParser.DEFAULT:
						col.defaultValue = Type.getObject(Util.getField(col, opt.getChild(0)));
						break;
					case FatwormParser.NULL:
						col.notNull = opt.getChildCount() != 0;
						break;
					}
				}
				break;
			case FatwormParser.PRIMARY_KEY:
				break;
				default:
					System.err.println("Schema undefined manipulation.");
			}
		}
		for(int i = 1; i < t.getChildCount(); i++){
			Tree c = t.getChild(i);
			String colName = c.getChild(0).getText();
			switch (c.getType()) {
			case FatwormParser.CREATE_DEFINITION:
				break;
			case FatwormParser.PRIMARY_KEY:
				primaryKey = getColumn(colName);
				primaryKey.primaryKey = true;
				break;
			}

		}
	}
	
	public Column getColumn(String col){
		if (columnDef.containsKey(col)) return columnDef.get(col);
		String attr = Util.getAttr(col);
		if (columnDef.containsKey(attr)) return columnDef.get(attr);
		String tmp1 = tableName + "." + attr;
		if (columnDef.containsKey(tmp1)) return columnDef.get(tmp1);
		int i = findIndex(col);
		if (i < 0) return null;
		String tmp2 = columnName.get(i);
		return columnDef.get(tmp2);
	}
	
	public void fromList(List<Expr> expr, Schema src) {
		tableName = src.tableName;
		if (expr.size() == 0 || Util.trim(expr.get(0).toString()).equals("*")) {
			columnName.addAll(src.columnName);
			columnDef.putAll(src.columnDef);
		} else {
			for(Expr e : expr){
				String colName = e.toString();
				Column col = src.getColumn(colName);
				if (col == null) col = new Column(colName, e.getType(src));
				columnDef.put(colName, col);
				columnName.add(colName);
			}
		}
	}
	
	public String toString(){
		return "[" + tableName + "]" + Util.deepToString(columnName);
	}
	
	public boolean equals(Object o){
		if(o instanceof Schema){
			Schema z = (Schema ) o;
			return z.tableName.equalsIgnoreCase(this.tableName);
		}
		return false;
	}
	
	public int findIndex(String x) {
		if (x == null) return -1;
		for(int i = 0; i < columnName.size(); i++){
			String y = columnName.get(i);
			if (x.equalsIgnoreCase(y) || x.equalsIgnoreCase(this.tableName + "." + y))
				return i;
		}
		for (int i = 0; i < columnName.size(); i++){
			String y = columnName.get(i);
			if (!y.contains(".")) continue;
			if (Util.getAttr(x).equalsIgnoreCase(Util.getAttr(y)))
				return i;
		}
		return -1;
	}
	
	public int findStrictIndex(String x) {
		for (int i = 0; i < columnName.size(); i++) {
			String y = columnName.get(i);
			if (x.equalsIgnoreCase(y) || Util.getAttr(x).equalsIgnoreCase(y) || x.equalsIgnoreCase(Util.getAttr(y)))
				return i;
		}
		return -1;
	}
	
	public Column getColumn(int i) {
		return getColumn(columnName.get(i));
	}

	public List<String> getCName1() {
		if (cName1 == null) {
			cName1 = new ArrayList<String>();
			for (String x : columnName)
				cName1.add(Util.getAttr(x));
		}
		return cName1;
	}

	public List<String> getCName2() {
		if (cName2 == null) {
			cName2 = new ArrayList<String>();
			for (String x : columnName)
				cName2.add(x.contains(".")?x:(tableName + "." + Util.getAttr(x)));
		}
		return cName2;
	}
}
