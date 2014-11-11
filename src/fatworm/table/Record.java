package fatworm.table;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import fatworm.expr.Expr;
import fatworm.io.ByteBuilder;
import fatworm.type.ContField;
import fatworm.type.NULL;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class Record implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2225456101460472650L;
	public List<Type> cols = new ArrayList<Type>();
	public Schema schema;
	
	public Record() {
	}
	
	public Record(Schema scm) {
		schema = scm;
	}
	
	public Type getCol(String by) {
		int idx = schema.findIndex(by);
		return getCol(idx);
	}
	
	public Type getCol(int idx) {
		if (idx < 0)
			return NULL.getInstance();
		return cols.get(idx);
	}
	
	public void addCol(Type x){
		cols.add(x);
	}
	
	public void addColFromExpr(Env env, List<Expr> func) {
		if (func.size()==0 || Util.trim(func.get(0).toString()).equals("*")) {
			for (String col : schema.columnName){
				Type tt = env.get(col);
				if (tt == null)
					tt = env.get(Util.getAttr(col));
				cols.add(tt);
			}
		} else {
			for(Expr e : func){
				Type tt = env.get(e.toString());
				if (tt == null || tt instanceof ContField)
					tt= e.eval(env);
				cols.add(tt);
			}
		}
	}

	public boolean equals(Object o) {
		if (!(o instanceof Record))
			return false;
		Record x = (Record) o;
		return Util.deepEquals(cols, x.cols);
	}

	public int hashCode() {
		return Util.deepHashCode(cols);
	}

	public String toString(){
		return "[\r\n" + schema.toString() + "\r\n]" + Util.deepToString(cols);
	}
	
	public void autoFill() {
		for (String colName : schema.columnName){
			Column col = schema.getColumn(colName);
			if (col.hasDefault() || col.type == java.sql.Types.TIMESTAMP) {
				cols.add(Util.getField(col, null));
			} else cols.add(null);
		}
	}
	
	public void setField(String colName, Type field) {
		cols.set(schema.findIndex(colName), field);
	}
	
	public static Record fromByte(Schema scm, byte[] byteArray) {
		Record r = new Record(scm);
		ByteBuffer b = ByteBuffer.wrap(byteArray);
		Integer nullMap = b.getInt();
		for (int i = 0;i < scm.columnName.size(); i++){
			if ((nullMap & 1) == 1) r.cols.add(NULL.getInstance());
			else r.cols.add(Type.fromBytes(b, scm.getColumn(i).type));
			nullMap >>= 1;
		}
		return r;
	}
	
	public static byte[] toByte(Record r){
		ByteBuilder b = new ByteBuilder();
		Integer nullMap = 0;
		
		for (int i = r.schema.columnName.size() - 1; i >= 0; i--){
			if (r.cols.get(i) == null || r.cols.get(i).java_type == java.sql.Types.NULL)
				nullMap += 1;
			nullMap <<= 1;
		}
		
		b.putInt(nullMap);
		for (int i = 0; i < r.schema.columnName.size(); i++){
			Column c = r.schema.getColumn(i);
			if (r.cols.get(i) != null && r.cols.get(i).java_type != java.sql.Types.NULL)
				r.cols.get(i).pushByte(b, c.A, c.B);
		}
		
		byte[] ans = new byte[b.getSize()];
		byte[] tmp = b.getByteArray();
		System.arraycopy(tmp, 0, ans, 0, b.getSize());
		return ans;
	}
}
