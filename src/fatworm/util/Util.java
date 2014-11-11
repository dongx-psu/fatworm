package fatworm.util;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.Tree;

import fatworm.driver.DBEngine;
import fatworm.expr.BinaryOp;
import fatworm.logicplan.LogicPlanner;
import fatworm.parser.FatwormParser;
import fatworm.table.Column;
import fatworm.type.DATE;
import fatworm.type.INT;
import fatworm.type.TIMESTAMP;
import fatworm.type.Type;

public class Util {
	public static final String tablePrefix = "__FATWORM__";
	
	public static void error(String x){
		warn("[ERROR]" + x);
		if (DBEngine.getInstance().debugFlag)
			throw new RuntimeException(x);
	}

	public static void warn(String x){
		if (DBEngine.getInstance().debugFlag)
			System.err.println("[WARNING]" + x);
	}
	
	public static String getMetaFile(String file) {
		return file + ".meta";
	}

	public static String getFreeFile(String file) {
		return file + ".free";
	}

	public static String getBTreeFile(String file) {
		return file + ".btree";
	}

	public static String getRecordFile(String file) {
		return file + ".record";
	}
	
	public static String getPKIndexName(String primaryKey) {
		return "__PrimaryIndex__" + primaryKey;
	}
	
	public static String getFuncName(int func) {
		switch(func){
		case FatwormParser.SUM:
			return "sum";
		case FatwormParser.AVG:
			return "avg";
		case FatwormParser.COUNT:
			return "count";
		case FatwormParser.MAX:
			return "max";
		case FatwormParser.MIN:
			return "min";
		}
		return "errorFunc";
	}
	
	public static String trim(String s){
		if (((s.startsWith("'") && s.endsWith("'"))||(s.startsWith("\"") && s.endsWith("\""))) && s.length() >= 2)
			return s.substring(1,s.length()-1);
		else return s;
	}
	
	public static java.sql.Timestamp stringToTimestamp(String x){
		try {
			return new java.sql.Timestamp(Long.valueOf(x));
		} catch (NumberFormatException e){
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			try {
				return new java.sql.Timestamp(format.parse(x).getTime());
			} catch (ParseException ee) {
				ee.printStackTrace();
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public static byte[] intToByte(int k) {
		byte[] ans = new byte[Integer.SIZE / Byte.SIZE];
		ByteBuffer buf = ByteBuffer.wrap(ans);
		buf.putInt(k);
		return buf.array();
	}
	public static byte[] longToByte(long k) {
		byte[] ans = new byte[Long.SIZE / Byte.SIZE];
		ByteBuffer buf = ByteBuffer.wrap(ans);
		buf.putLong(k);
		return buf.array();
	}
	
	public static boolean isCommutative(BinaryOp op) {
		switch (op) {
		case LT:
		case GT:
		case LEQ:
		case GEQ:
		case MINUS:
		case DIV:
		case MOD:
			return false;
		case EQ:
		case NEQ:
		case ADD:
		case MUL:
		case AND:
		case OR:
			return true;
			default:
				return false;
		}
	}

	public static <T> String deepToString(Collection<T> s){
		StringBuffer ans = new StringBuffer();
		ans.append("{");
		for (T x : s){
			ans.append(x == null ? "null" : x.toString() + ", ");
		}
		ans.append("}");
		return ans.toString();
	}
	
	public static <T> boolean deepEquals(List<T> a, List<T> b){
		if(a.size() != b.size())
			return false;

		for (int i = 0; i < a.size(); i++)
			if (!a.get(i).equals(b.get(i)))
				return false;

		return true;
	}

	public static <T> int deepHashCode(List<T> a){
		int hash = 0;
		for (T x : a){
			hash ^= x.hashCode();
		}
		return hash;
	}
	
	public static <K,V> String deepToString(Map<K, V> res) {
		StringBuffer ans = new StringBuffer();
		for (Map.Entry<K, V> e : res.entrySet()) {
			K x = e.getKey();
			V y = e.getValue();
			ans.append(x == null ? "null" : x.toString());
			ans.append("=");
			ans.append(y == null ? "null" : y.toString());
			ans.append(", ");
		}
		ans.append("}");
		return ans.toString();
	}
	
	public static Type getField(Column column, Tree c) {
		if (c == null || c.getText().equalsIgnoreCase("default") || c.getText().equalsIgnoreCase("null")) {
			if (column.hasDefault()) {
				return column.getDefault();
			} else if (column.isAutoInc()) {
				return new INT(column.getAutoInc());
			} else if(column.type == java.sql.Types.DATE) {
				return new DATE(new java.sql.Timestamp((new GregorianCalendar()).getTimeInMillis()));
			} else if(column.type == java.sql.Types.TIMESTAMP) {
				return new TIMESTAMP(new java.sql.Timestamp((new GregorianCalendar()).getTimeInMillis()));
			}
		}
		return Type.fromString(column.type, LogicPlanner.getExpr(c).eval(new Env()).toString());
	}

	public static String strip(String s) {
		return s.substring(1, s.length() - 1);
	}

	public static String getAttr(String x) {
		return x.contains(".") ? x.substring(x.indexOf('.')+1) : x;
	}
	
	public static boolean subsetof(List<String> x, List<String> y) {
		for (String xx : x){
			boolean found = false;
			for (String yy : y) {
				String tmp1 = yy.toLowerCase();
				String tmp2 = xx.toLowerCase();
				if (tmp1.endsWith(tmp2)) {
					if (tmp1.indexOf(tmp2)==0 || tmp1.indexOf(tmp2) == tmp1.indexOf(".")+1) {
						found = true;
						break;
					}
				}
			}
			if (!found) return false;
		}
		return true;
	}
	
	public static void removeAllCol(Collection<String> a, Collection<String> b){
		Set<String> tmp = new HashSet<String> ();
		for (String x : a) {
			for (String y : b) {
				if (x.toLowerCase().endsWith(y.toLowerCase()) || y.toLowerCase().endsWith(x.toLowerCase()))
					tmp.add(x);
			}
		}
		a.removeAll(tmp);
	}
	
	public static void addAllCol(Collection<String> a, Collection<String> b){
		for (String y : b) {
			boolean flag = false;
			for(String x:a){
				if(x.equalsIgnoreCase(y))
					flag = true;
			}
			if(!flag)
				a.add(y);
		}
	}

	public static boolean toBoolean(Type c) {
		return toBoolean(c.toString());
	}

	public static boolean toBoolean(String s) {
		s = trim(s);
		if (s.equalsIgnoreCase("false") || s.equals("0"))
			return false;
		return true;
	}
}
