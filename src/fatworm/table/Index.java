package fatworm.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Index implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7184404645165295167L;
	public String indexName;
	public Table table;
	public Column column;
	public int colIdx;
	public boolean unique;
	public Integer pid;
	public List<List<Integer>> buckets = new ArrayList<List<Integer>>();
	
	public Index(String a, Table b, Column c){
		indexName = a;
		table = b;
		column = c;
		unique = false;
		colIdx = b.schema.findIndex(c.name);
	}
	
	public Index(String a, Table b, Column c, boolean u) {
		this(a, b, c);
		unique = u;
	}
	
	@Override
	public boolean equals(Object o){
		if (o instanceof Index) {
			Index x = (Index)o;
			return x.indexName.equals(indexName) && x.table.equals(table);
		}
		return false;
	}

	@Override
	public int hashCode(){
		return pid ^ indexName.hashCode();
	}

	@Override
	public String toString(){
		return indexName + " on " +table.toString()+" on " + column.toString() +" with pageID="+pid;
	}
}
