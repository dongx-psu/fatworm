package fatworm.table;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.CommonTree;

import fatworm.btree.BTree;
import fatworm.btree.BTreePage.BCursor;
import fatworm.driver.DBEngine;
import fatworm.driver.Database;
import fatworm.expr.Expr;
import fatworm.io.BufferManager;
import fatworm.io.Cursor;
import fatworm.io.File;
import fatworm.util.Env;
import fatworm.util.Util;
import fatworm.page.RecordPage;
import fatworm.type.Type;

public class IOTable extends Table {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3424335700313903922L;
	public static final Integer MODOOFFSET = (int) (File.recordPageSize / 4);

	public IOTable() {
		firstPageID = -1;
	}
	
	public IOTable(CommonTree t) {
		this();
		schema = new Schema(t);
		if (schema.primaryKey != null)
			DBEngine.getInstance().getDatabase().createIndexWithTable(Util.getPKIndexName(schema.primaryKey.name), schema.primaryKey.name, true, this);
	}

	public SimpleCursor open() {
		return new SimpleCursor();
	}
	
	public IndexCursor scope(Index index, Type l, Type r, boolean isMaxEQ){
		try {
			BTree b = new BTree(DBEngine.getInstance().btreeManager, index.pid, index.column.type, index.table);
			BCursor head = (l == null || l.java_type==java.sql.Types.NULL) ? null : b.root.lookup(b.newBKey(l));
			if (head != null) head = head.adjust();
			
			BCursor last = (r == null) || r.java_type==java.sql.Types.NULL ? null : b.root.lookup(b.newBKey(r));
			if (last != null) {
				last = isMaxEQ ? last.adjust():last.adjustLeft();
				if (isMaxEQ && last.getKey().compareTo(b.newBKey(r)) > 0)
					last = last.prev();
			}
			return new IndexCursor(index, head, last);
		} catch (Throwable e) {
			e.printStackTrace();
			return null;
		}
	}
	public IndexCursor order(Index index){
		try {
			return new IndexCursor(index);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	@Override
	public int update(List<String> colName, List<Expr> expr, Expr e) {
		SimpleCursor c = open();
		int ans = 0;
		try {
			for (; c.hasThis(); c.next()) {
				Record r = c.fetchRecord();
				Env env = new Env();
				env.appendFromRecord(r);
				if (e != null && !e.evalPred(env))
					continue;
				for (int i = 0; i < expr.size(); i++) {
					Type res = expr.get(i).eval(env);
					int idx = r.schema.findIndex(colName.get(i));
					r.cols.set(idx, Type.fromString(r.schema.getColumn(idx).type, res.toString()));
					env.put(colName.get(i), res);
				}
				c.updateWithRecord(r);
				ans++;
			}
		} catch (Throwable e1) {
			e1.printStackTrace();
		}
		return ans;
	}

	@Override
	public void addRecord(Record r) {
		SimpleCursor c = open();
		try {
			if (!c.hasThis()) {
				BufferManager bm = DBEngine.getInstance().recordManager;
				firstPageID = bm.newPage();
				RecordPage curPage = bm.getRecordPage(firstPageID, true);
				curPage.tryAppendRecord(r);
				curPage.beginTransaction();
				curPage.dirty = true;
				curPage.nextPID = curPage.prevPID = curPage.getID();
				curPage.commit();
				createIndexForRecord(r, open());
				return;
			}
			c.prev();
			c.appendThisPage(r);
			c.next();
			createIndexForRecord(r, c);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	private void createIndexForRecord(Record r, SimpleCursor c) {
		for (Index idx : tableIndex) {
			try {
				BTree b = new BTree(DBEngine.getInstance().btreeManager, idx.pid, idx.column.type, this);
				Database.createIndexForRecord(idx, b, c, r);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void deleteAll() {
		SimpleCursor c = open();
		while (c.hasThis()) {
			try {
				c.delete();
			} catch (Throwable e) {
				Util.error(e.getMessage());
			}
		}
	}
	
	public class SimpleCursor implements Cursor {
		Integer pid;
		int offset;
		List<Record> cache = new ArrayList<Record>();
		boolean end;
		
		public SimpleCursor(){
			reset();
		}
		@Override
		public void reset() {
			pid = IOTable.this.firstPageID;
			offset = 0;
			cache = getRecords(pid);
			end = false;
		}

		@Override
		public void next() throws Throwable {
			if (offset < cache.size() - 1) offset++;
			else{
				end = !hasNext();
				pid = getNextPage();
				offset = 0;
				cache = getRecords(pid);
			}
		}

		private List<Record> getRecords(Integer pid) {
			return new ArrayList<Record>(DBEngine.getInstance().recordManager.getRecords(pid, schema));
		}
		
		private Integer getNextPage() throws Throwable {
			return DBEngine.getInstance().recordManager.getNextPage(pid);
		}

		private Integer getPrevPage() throws Throwable {
			return DBEngine.getInstance().recordManager.getPrevPage(pid);
		}
		
		public boolean appendThisPage(Record r){
			try {
				boolean flag = DBEngine.getInstance().recordManager.getRecordPage(pid, false).tryAppendRecord(r);
				if (flag) cache = getRecords(pid);
				return flag;
			} catch (Throwable e) {
				e.printStackTrace();
				return false;
			}
		}
		@Override
		public void prev() throws Throwable {
			if (offset > 0) offset--;
			else {
				pid = getPrevPage();
				cache = getRecords(pid);
				offset = cache.size() - 1;
			}
		}

		@Override
		public Object fetch(String col) {
			return fetchRecord().getCol(col);
		}

		@Override
		public Object[] fetch() {
			return fetchRecord().cols.toArray();
		}

		@Override
		public void delete() throws Throwable {
			RecordPage rp = DBEngine.getInstance().recordManager.getRecordPage(pid, false);
			rp.delRecord(schema, offset);
			if (offset >= cache.size() - 1) {
				end = !hasNext();
				pid = getNextPage();
				offset = 0;
				cache = getRecords(pid);
			} else {
				cache.remove(offset);
			}
			boolean flag = rp.canReclaim();
			if (flag && !pid.equals(firstPageID)) {
				rp.tryReclaim();
				firstPageID = pid;
			}
		}
		
		public void updateWithRecord(Record r) throws Throwable {
			DBEngine.getInstance().recordManager.getRecordPage(pid, false).delRecord(schema, offset);
			DBEngine.getInstance().recordManager.getRecordPage(pid, false).addRecord(r, offset);
		}

		@Override
		public void close() {
		}

		@Override
		public boolean hasNext() throws Throwable {
			return !getNextPage().equals(firstPageID);
		}

		@Override
		public Record fetchRecord() {
			return cache.get(offset);
		}
		@Override
		public Integer getIdx() {
			int ret = pid * MODOOFFSET + offset;
			assert(ret >= 0);
			return ret;
		}
		@Override
		public boolean hasThis() {
			return (pid >= 0 && !end && offset < cache.size());
		}
		
	}
	
	public class IndexCursor implements Cursor {
		BTree btree;
		BCursor bc;
		BCursor head;
		BCursor last;
		Index index;
		int idx;
		
		public IndexCursor(Index index) throws Throwable{
			this.btree = new BTree(DBEngine.getInstance().btreeManager, index.pid, index.column.type, index.table);
			this.index = index;
			reset();
		}
		public IndexCursor(Index index, BCursor head, BCursor last) throws Throwable{
			this.btree = new BTree(DBEngine.getInstance().btreeManager, index.pid, index.column.type, index.table);
			this.index = index;
			this.head = head;
			this.last = last;
			reset();
		}
		public void head() {
			bc = head;
			if (!index.unique) idx = 0;
		}
		@Override
		public void reset() throws Throwable {
			if(head == null)head = btree.root.head();
			if(last == null)last = btree.root.last();
			head();
		}
		
		@Override
		public void next() throws Throwable {
			if (index.unique) {
				if(bc.hasNext()) nextBc();
				else bc = null;
			} else {
				idx++;
				if (idx >= index.buckets.get(bc.getValue()).size()) {
					if (bc.hasNext()) {
						nextBc();
						idx = 0;
					} else bc = null;
				}
			}
		}

		private void nextBc() throws Throwable {
			bc = bc.equals(last) ? null: bc.next();
		}
		
		@Override
		public void prev() throws Throwable {
			if (index.unique) {
				if (bc.hasPrev()) prevBc();
				else bc = null;
			} else {
				idx--;
				if (idx < 0) {
					if (bc.hasPrev()) {
						prevBc();
						idx = index.buckets.get(bc.getValue()).size() - 1;
					} else bc = null; 
				}
			}
		}

		private void prevBc() throws Throwable {
			bc = bc.equals(head) ? null : bc.prev();
		}
		
		@Override
		public Object fetch(String col) {
			return fetchRecord().getCol(col);
		}

		@Override
		public Object[] fetch() {
			return fetchRecord().cols.toArray();
		}

		@Override
		public void delete() throws Throwable {
			// TODO how to do this at all
		}

		@Override
		public void close() {
		}

		@Override
		public boolean hasNext() {
			return bc!=null && bc!=last && (!index.unique && idx < index.buckets.get(bc.getValue()).size()-1) || bc.hasNext();
		}

		@Override
		public Record fetchRecord() {
			Integer encodedOffset = index.unique ? bc.getValue() : index.buckets.get(bc.getValue()).get(idx);
			Integer pid = encodedOffset / MODOOFFSET;
			Integer offset = encodedOffset % MODOOFFSET;
			if (offset<0)
				Util.error("err in fetchrecord @ index cursor");
			return getRecords(pid).get(offset);
		}
		
		private List<Record> getRecords(Integer pid) {
			return DBEngine.getInstance().recordManager.getRecords(pid, schema);
		}

		@Override
		public Integer getIdx() {
			assert(false);
			return null;
		}

		@Override
		public boolean hasThis() {
			return (bc != null&& bc.valid());
		}
	}
}
