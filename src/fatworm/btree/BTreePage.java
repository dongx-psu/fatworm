package fatworm.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import fatworm.driver.DBEngine;
import fatworm.io.File;
import fatworm.page.RawPage;
import fatworm.util.Util;

public class BTreePage extends RawPage {
	private static final int ROOTNODE = 1;
	private static final int INTERNALNODE = 2;
	private static final int LEAFNODE = 3;
	private static final int ROOTLEAFNODE = 4;
	public static final int LS = Long.SIZE / Byte.SIZE;
	public static final int IS = Integer.SIZE / Byte.SIZE;

	private int nodeType;
	private int parentPID;
	public ArrayList<Integer> childList;
	public ArrayList<BKey> key;
	public int keyType;
	public BTree btree;
	public int fanout;
	
	public BTreePage(BTree btree, File f, int pageid, int keyType, boolean create) throws Throwable {
		lastTime = System.currentTimeMillis();
		dataFile = f;
		pid = pageid;
		byte [] tmp = new byte[(int) File.recordPageSize];
		this.keyType = keyType;
		this.btree = btree;
		fanout = fanoutSize(keyType);
		if (!create) {
			if (pid < 0) Util.warn("BTree pageID < 0!!");
			dataFile.read(tmp, pid);
			childList = new ArrayList<Integer>();
			key = new ArrayList<BKey>();
			fromBytes(tmp);
		} else {
			nextPID = -1;
			prevPID = -1;
			parentPID = -1;
			childList = new ArrayList<Integer>();
			key = new ArrayList<BKey>();
			dirty = true;
			childList.add(-1);
			nodeType=ROOTLEAFNODE;
			buf = ByteBuffer.wrap(tmp);
		}
	}
	
	public synchronized void insert(Integer idx, BKey k, int val) throws Throwable {
		dirty = true;
		if (!isFull()) add(idx, k, val);
		else {
			boolean isLeaf = isLeaf();
			boolean isRoot = isRoot();
			beginTransaction();
			BTreePage newPage = btree.bm.getBTreePage(btree, btree.bm.newPage(), keyType, true);
			newPage.beginTransaction();
			newPage.nodeType = nodeType = isLeaf ? LEAFNODE : INTERNALNODE;
			newPage.parentPID = parentPID;
			newPage.nextPID = nextPID;
			newPage.prevPID = pid;
			if (hasNext()) {
				BTreePage p = next();
				p.prevPID = newPage.getID();
				p.dirty = true;
				p = null;
			}
			nextPID = newPage.pid;
			newPage.dirty = true;
			dirty = true;
			
			int mid = (int) Math.ceil(fanout/2.0 - (1e-3));
			int tmpidx = indexOf(k);
			key.add(tmpidx, k);
			childList.add(tmpidx+1, val);
			
			if (DBEngine.getInstance().debugFlag) {
				for (int i = 0; i < key.size() - 1; i++){
					assert(key.get(i).compareTo(key.get(i+1)) < 0);
				}
			}
			
			ArrayList<BKey> tmplist1 = new ArrayList<BKey> ();
			ArrayList<BKey> tmplist2 = new ArrayList<BKey> ();
			BKey toParent = key.get(mid);
			if (isLeaf) {
				for (int i = 0; i < mid; i++)
					tmplist1.add(key.get(i));
				for (int i = mid; i < key.size(); i++)
					tmplist2.add(key.get(i));
			} else {
				for (int i = 0; i < mid - 1; i++)
					tmplist1.add(key.get(i));
				for (int i = mid; i < key.size(); i++)
					tmplist2.add(key.get(i));
			}
			key = tmplist1;
			newPage.key = tmplist2;
			
			ArrayList<Integer> tmpchild1 = new ArrayList<Integer> ();
			ArrayList<Integer> tmpchild2 = new ArrayList<Integer> ();
			if (isLeaf) {
				for (int i = 0; i < mid + 1; i++)
					tmpchild1.add(childList.get(i));
				tmpchild2.add(-1);
				for (int i = mid + 1; i < childList.size(); i++) {
					Integer cpid = childList.get(i);
					tmpchild2.add(cpid);
				}
			} else {
				for (int i = 0; i < mid; i++)
					tmpchild1.add(childList.get(i));
				for (int i = mid; i < childList.size(); i++) {
					Integer cpid = childList.get(i);
					tmpchild2.add(cpid);
					BTreePage cp = getPage(cpid);
					cp.beginTransaction();
					cp.parentPID = newPage.pid;
					cp.commit();
				}
			}
			childList = tmpchild1;
			newPage.childList = tmpchild2;
			
			if (isRoot) {
				BTreePage newRoot = btree.bm.getBTreePage(btree, btree.bm.newPage(), keyType, true);
				parentPID = newRoot.pid;
				commit();
				newPage.parentPID = newRoot.pid;
				newPage.commit();
				newRoot.beginTransaction();
				newRoot.nodeType = ROOTNODE;
				newRoot.key.add(toParent);
				newRoot.childList = new ArrayList<Integer>();
				newRoot.childList.add(pid);
				newRoot.childList.add(newPage.pid);
				btree.table.announceNewRoot(btree.root.getID(), newRoot.getID());
				btree.root = newRoot;
				newRoot.dirty = true;
				newRoot.commit();
			} else {
				commit();
				newPage.commit();
				int pidx = -1;
				BTreePage parent = parent();
				for(int i=0;i<parent.childList.size();i++){
					if(pid==parent.childList.get(i)){
						pidx = i;
						break;
					}
				}
				parent.beginTransaction();
				parent.insert(pidx, toParent, newPage.pid);
				parent.commit();
			}
		}
	}
	
	public synchronized void add(Integer idx, BKey k, int val) {
		key.add(idx, k);
		assert(val>=0);
		childList.add(idx + 1, val);
		dirty = true;
	}

	public int indexOf(BKey k){
		int idx = Collections.binarySearch(key, k, new Comparator<BKey>() {
			public int compare(BKey arg0, BKey arg1) {
				return arg0.compareTo(arg1);
			}
		});
		return idx < 0 ? -1 - idx : idx;
	}
	public BCursor lookup(BKey k) throws Throwable {
		int idx = indexOf(k);
		return isLeaf() ? new BCursor(idx) : getPage(childList.get(idx)).lookup(k);
	}

	public synchronized void remove(Integer idx) throws Throwable {
		beginTransaction();
		dirty = true;
		key.remove(idx);
		childList.remove(idx);
		commit();
	}
	
	public void fromBytes(byte[] b) throws Throwable {
		ByteBuffer buf = ByteBuffer.wrap(b);
		nodeType = buf.getInt();
		parentPID = buf.getInt();
		prevPID = buf.getInt();
		nextPID = buf.getInt();
		int childcnt = buf.getInt();
		for (int i = 0; i < childcnt; i++)
			childList.add(buf.getInt());
		
		for (int i = 0;i < childcnt - 1; i++)
			if(keySize(keyType) == IS)
				key.add(btree.getBKey(buf.getInt(), keyType));
			else
				key.add(btree.getBKey(buf.getLong(), keyType));
		
		this.buf = buf;
	}
	
	public byte[] toBytes() throws Throwable {
		buf.position(0);
		buf.putInt(nodeType);
		buf.putInt(parentPID);
		buf.putInt(prevPID);
		buf.putInt(nextPID);
		buf.putInt(childList.size());
		for (int i = 0; i < childList.size(); i++)
			buf.putInt(childList.get(i));
		
		for (int i = 0; i < key.size(); i++)
			buf.put(key.get(i).toByte());
		
		return buf.array();
	}
	

	public boolean isLeaf() {
		return nodeType == LEAFNODE || nodeType == ROOTLEAFNODE;
	}
	
	public boolean isRoot() {
		return nodeType == ROOTNODE || nodeType == ROOTLEAFNODE;
	}
	
	public boolean isInternal() {
		return nodeType == INTERNALNODE;
	}
	
	public boolean isFull() {
		assert fanout >= childList.size();
		return fanout == childList.size();
	}
	
	public static int keySize(int type) {
		switch(type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.DECIMAL:
			return Integer.SIZE / Byte.SIZE;
		case java.sql.Types.DATE:
		case java.sql.Types.TIMESTAMP:
			return Long.SIZE / Byte.SIZE;
			default:
				Util.error("meow@BTreePage");
		}
		return 4;
	}
	
	public static int fanoutSize(int type) {
		switch(type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
			switch((int)File.btreePageSize){
			case 1024:
				return 126;
			case 2048:
				return 254;
			case 4096:
				return 510;
			case 8192:
				return 1022;
			}
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.DECIMAL:
			switch((int)File.btreePageSize){
			case 1024:
				return 126;
			case 2048:
				return 254;
			case 4096:
				return 510;
			case 8192:
				return 1022;
			}
		case java.sql.Types.DATE:
		case java.sql.Types.TIMESTAMP:
			switch((int)File.btreePageSize){
			case 1024:
				return 84;
			case 2048:
				return 168;
			case 4096:
				return 340;
			case 8192:
				return 680;
			}
			default:
				Util.error("meow@BTreePage");
		}
		return 340;
	}
	
	public int headerSize() {
		return 5 * Byte.SIZE;
	}
	

	public boolean hasNext() {
		return nextPID != -1;
	}
	
	public boolean hasPrev() {
		return prevPID != -1;
	}

	public synchronized BCursor head() throws Throwable {
		if (isLeaf()) return new BCursor(0);
		return getPage(childList.get(0)).head();
	}

	public synchronized BCursor last() throws Throwable {
		if (isLeaf())
			return new BCursor(key.size());
		return getPage(childList.get(key.size())).last();
	}
	
	public synchronized BTreePage getPage(Integer pid) throws Throwable {
		return btree.bm.getBTreePage(btree, pid, keyType, false);
	}

	public synchronized BTreePage next() throws Throwable {
		return getPage(nextPID);
	}
	
	public synchronized BTreePage prev() throws Throwable {
		return getPage(prevPID);
	}
	
	public synchronized BTreePage parent() throws Throwable {
		if (parentPID < 0) Util.warn("Parent PID must >= 0");
		return getPage(parentPID);
	}

	public void delete() throws Throwable {
		if(!isLeaf()){
			for(int i=0;i<childList.size();i++){
				getPage(childList.get(i)).delete();
			}
		}
		for(int i=0;i<key.size();i++){
			key.get(i).delete();
		}
		btree.bm.releasePage(pid);
	}

	public final class BCursor {
		private final int idx;
		
		public BCursor(int idx){
			this.idx = idx;
		}
		
		public BKey getKey(){
			assert(valid());
			return key.get(idx);
		}
		
		@Override
		public BCursor clone(){
			return new BCursor(idx);
		}
		
		public int getValue() {
			assert(valid());
			return childList.get(idx+1);
		}
		
		public boolean hasNext(){
			return idx + 1 < key.size() || BTreePage.this.hasNext();
		}
		
		public BCursor next() throws Throwable{
			assert(valid());
			if (idx + 1 < key.size())
				return new BCursor(idx+1);
			else return BTreePage.this.next().head();
		}
		
		public boolean hasPrev(){
			return idx > 0 || BTreePage.this.hasPrev();
		}
		
		public BCursor prev() throws Throwable{
			if (idx > 0)
				return new BCursor(idx - 1);
			else return BTreePage.this.prev().last().prev();
		}
		
		public void insert(BKey k, int val) throws Throwable{
			assert(valid() || idx == key.size());
			BTreePage.this.beginTransaction();
			BTreePage.this.insert(idx, k, val);
			BTreePage.this.commit();
		}
		
		public void remove() throws Throwable{
			assert(valid());
			BTreePage.this.beginTransaction();
			BTreePage.this.remove(idx);
			BTreePage.this.commit();
		}
		
		public boolean valid(){
			return idx >=0 && idx < key.size();
		}

		public BCursor adjust() throws Throwable {
			if (valid()) return this;
			if (idx==key.size() && BTreePage.this.hasNext())
				return BTreePage.this.next().head();
			
			return null;
		}

		public BCursor adjustLeft() {
			if (valid()) return this;
			if (idx == key.size() && idx > 0)
				return new BCursor(idx-1);
			
			return null;
		}
		
		@Override
		public boolean equals(Object o){
			if (o instanceof BCursor) {
				BCursor b = (BCursor) o;
				return getPageID()==b.getPageID() && idx==b.idx;
			}
			return false;
		}

		private int getPageID() {
			return BTreePage.this.getID();
		}
	}
}
