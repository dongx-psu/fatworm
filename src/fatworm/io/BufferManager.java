package fatworm.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Comparator;

import fatworm.btree.BTree;
import fatworm.btree.BTreePage;
import fatworm.driver.DBEngine;
import fatworm.page.Page;
import fatworm.page.RawPage;
import fatworm.page.RecordPage;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Util;

public class BufferManager {
	public File dataFile;
	public FreeList freeList;
	public Map<Integer, Page> pages = new TreeMap<Integer, Page>();
	
	public TreeSet<Page> victimQueue = new TreeSet<Page>
		(new Comparator<Page>() {
			public int compare(Page a, Page b) {
				return a.getTime().compareTo(b.getTime());
			}
		});
		
	
	public String fileName = null;
	
	public BufferManager(String file, int type) throws IOException, ClassNotFoundException {
		dataFile = new File(file, type);
		try{
			freeList = FreeList.read(file);
		} catch (java.io.FileNotFoundException e) {
			freeList = new FreeList();
			FreeList.write(file, freeList);
		}
		fileName = file;
	}
	
	public synchronized BTreePage getBTreePage(BTree btree, int pid, int type, boolean create) throws Throwable {
		Page ans = getPageHelper(pid);
		if (ans != null)
			return (BTreePage) ans;
		BTreePage p = new BTreePage(btree, dataFile, pid, type, create);
		pages.put(pid, p);
		victimQueue.add(p);
		return p;
	}
	
	public synchronized RecordPage getRecordPage(int pid, boolean create) throws Throwable {
		Page ans = getPageHelper(pid);
		if (ans != null)
			return (RecordPage) ans;
		RecordPage p = new RecordPage(this, dataFile, pid, create);
		pages.put(pid, p);
		victimQueue.add(p);
		return p;
	}
	
	public synchronized RawPage getRawPage(int pid, boolean create) throws Throwable{
		Page ans = getPageHelper(pid);
		if (ans != null)
			return (RawPage) ans;
		RawPage p = new RawPage(dataFile, pid, create);
		pages.put(pid, p);
		victimQueue.add(p);
		return p;
	}
	
	private synchronized Page getPageHelper(int pid) throws Throwable {
		if (pages.containsKey(pid))
			return pages.get(pid);
		
		while (DBEngine.getInstance().nearOOM())
			DBEngine.getInstance().fireOther(this);
		
		return null;
	}
	
	public synchronized boolean fireMeOne() throws Throwable{
		synchronized (victimQueue) {
			if (victimQueue.isEmpty())
				return false;
			Page p = victimQueue.pollFirst();
			Collection<Page> tmp = new LinkedList<Page> ();
			while (p.isInTransaction() && !victimQueue.isEmpty()) {
				tmp.add(p);
				p = victimQueue.pollFirst();
			}
			
			if (p.isInTransaction())
				Util.warn("flushing out a page still in transaction, will try to recover.");
			
			p.flush();
			pages.remove(p.getID());
			victimQueue.addAll(tmp);
			return true;
		}
	}

	public void close() throws Throwable {
		LinkedList<Page> tmp = new LinkedList<Page>(pages.values());
		
		for (Page p : tmp) {
			assert(!p.isInTransaction());
			p.flush();
			pages.remove(p.getID());
			victimQueue.remove(p);
		}
		
		for (Page p : pages.values())
			p.flush();

		dataFile.close();
		FreeList.write(fileName, freeList);
	}
	
	public void flush() throws Throwable {
		LinkedList<Page> tmp = new LinkedList<Page>(pages.values());
		
		for (Page p : tmp) {
			assert(!p.isInTransaction());
			p.flush();
			pages.remove(p.getID());
			victimQueue.remove(p);
		}
		
		for (Page p : pages.values())
			p.flush();

		//dataFile.close();
		FreeList.write(fileName, freeList);
	}
	
	public synchronized void releasePage(Integer pageID) {
		freeList.add(pageID);
		Page p = pages.get(pageID);
		if (p != null) {
			p.markFree();
			victimQueue.remove(p);
			pages.remove(pageID);
		}
	}
	
	public synchronized Integer newPage() {
		return freeList.poll();
	}

	public synchronized RawPage newRawPage(int size) throws Throwable {
		Integer ans = RawPage.newPoolPage(size);
		if (ans == null) {
			ans = newPage();
			return getRawPage(ans, true);
		} else {
			RawPage rp = getRawPage(ans, false);
			rp.newEntry();
			return rp;
		}
	}

	public List<Record> getRecords(Integer pageID, Schema schema) {
		ArrayList<Record> ans = new ArrayList<Record>();
		if (pageID < 0)
			return ans;
		try {
			RecordPage rp = getRecordPage(pageID, false);
			if(!rp.isPartial())
				return rp.getRecords(schema);
			ByteBuilder b = new ByteBuilder();
			while(true){
				b.putByteArray(rp.getPartialBytes());
				if(rp.endOfPartial())break;
				rp = getRecordPage(rp.nextPID, false);
			}
			ans.add(Record.fromByte(schema, b.getByteArray()));
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public Integer getNextPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).nextPID;
	}

	public Integer getPrevPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).prevPID;
	}
}
