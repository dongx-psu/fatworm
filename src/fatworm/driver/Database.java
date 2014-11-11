package fatworm.driver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fatworm.btree.BKey;
import fatworm.btree.BTree;
import fatworm.btree.BTreePage.BCursor;
import fatworm.io.Cursor;
import fatworm.table.Index;
import fatworm.table.Record;
import fatworm.table.Table;
import fatworm.util.Util;

public class Database implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4594194262519149049L;
	public String name;
	public Map<String, Table> tableList = new HashMap<String, Table>();
	public Map<String, Index> indexList = new HashMap<String, Index>();
	
	public Database(String name){
		this.name = name;
	}
	
	public void addTable(String tblName, Table table) {
		tableList.put(tblName, table);
	}
	
	public void delTable(String tblName) {
		tableList.remove(tblName);
	}
	
	public Table getTable(String tblName) {
		return tableList.get(tblName);
	}
	
	public void createIndex(String idx, String tbl, String col, boolean unique) {
		Table table = tableList.get(tbl);
		createIndexWithTable(idx, col, unique, table);
	}
	
	public void createIndexWithTable(String idx, String col, boolean unique, Table table) {
		Index index = new Index(idx, table, table.schema.getColumn(col), unique);
		try {
			BTree b = new BTree(DBEngine.getInstance().btreeManager, index.column.type, index.table);
			for (Cursor c = table.open(); c.hasThis(); c.next()){
				Record r = c.fetchRecord();
				createIndexForRecord(index, b, c, r);
			}
			index.pid = b.root.getID();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		indexList.put(idx, index);
		table.tableIndex.add(index);
	}
	
	public static void createIndexForRecord(Index index, BTree b, Cursor c, Record r) throws Throwable {
		BKey key = b.newBKey(r.getCol(index.colIdx));
		BCursor bc = b.root.lookup(key);
		if (index.unique) {
			if (bc.valid() && bc.getKey().equals(key))
				Util.warn("duplicated keys in unique index!!!");
			bc.insert(key, c.getIdx());
		} else {
			if (bc.valid() && bc.getKey().equals(key)) {
				index.buckets.get(bc.getValue()).add(c.getIdx());
			} else {
				List<Integer> tmp = new ArrayList<Integer>();
				tmp.add(c.getIdx());
				bc.insert(key, index.buckets.size());
				index.buckets.add(tmp);
			}
		}
	}
	public void dropIndex(String idx) {
		Index index = indexList.remove(idx);
		try {
			new BTree(DBEngine.getInstance().btreeManager, index.pid, index.column.type, index.table).root.delete();
		} catch (Throwable e) {
			System.err.println(e.getMessage());
		}
		index.table.tableIndex.remove(index);
	}

	public boolean checkKiller() {
		if (name.contains("killer"))
			return true;
			
		return false;
	}
	
}
