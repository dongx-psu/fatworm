package fatworm.page;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeSet;

import fatworm.io.File;

public class RawPage implements Page {
	public static TreeSet<RawPageEntry> pool = new TreeSet<RawPageEntry> ();
	
	protected int pid;
	public int nextPID;
	public int prevPID;
	public boolean dirty = false;
	protected File dataFile;
	protected Long lastTime;
	public int size;
	public ByteBuffer buf = ByteBuffer.wrap(new byte[(int) File.btreePageSize]);
	public int cnt = 0;
	public boolean hasFlushed = false;
	public int inTransaction = 0;
	
	public RawPage() {
	}
	
	public RawPage(File f, int pageid, boolean create) throws Throwable {
		lastTime = System.currentTimeMillis();
		dataFile = f;
		pid = pageid;
		if (!create) loadPage();
		else {
			nextPID = -1;
			prevPID = -1;
			size = 4;
			synchronized (pool){
				pool.add(new RawPageEntry(remainingSize(), pid));
			}
		}
	}
	
	public synchronized void loadPage() throws Throwable {
		dataFile.read(buf, pid);
		fromBytes(buf.array());
	}
	
	public synchronized void write() throws Throwable {
		if (!dirty) return;
		toBytes();
		
		if (isInTransaction()) {
			System.err.println("PID "+pid +" in transaction, fake flushed");
			return;
		}
		
		hasFlushed = true;
		dataFile.write(buf, pid);
		dirty = false;
	}

	@Override
	public void fromBytes(byte[] b) throws Throwable {
		buf = ByteBuffer.wrap(b);
		buf.position(0);
		size = buf.getInt();
		cnt = buf.getInt();
		buf.position(0);
	}
	
	@Override
	public byte[] toBytes() throws Throwable {
		buf.position(0);
		buf.putInt(size);
		buf.putInt(cnt);
		return buf.array();
	}
	
	public synchronized static Integer newPoolPage(int x) {
		Iterator<RawPageEntry> it = pool.iterator();
		while (it.hasNext()) {
			RawPageEntry p = it.next();
			if (p.remain * 4 < File.btreePageSize)
				it.remove();
			if (p.remain >= x)
				return p.pid;
		}
		return null;
	}
	
	public synchronized void updateSize(int o){
		synchronized (pool) {
			pool.remove(new RawPageEntry(remainingSize(), pid));
			if (size < o) {
				dirty = true;
				size = o;
			}
			pool.add(new RawPageEntry(remainingSize(), pid));
		}
	}
	
	@Override
	public boolean equals(Object o){
		if (o instanceof RawPage)
			return (pid == ((RawPage)o).pid);
		return false;
	}
	
	public static int getSize(BigDecimal v){
		return 4 + getSize(v.unscaledValue().toByteArray());
	}
	
	public static int getSize(byte[] v) {
		return 4 + v.length;
	}
	
	public static int getSize(String v){
		return getSize(v.getBytes());
	}

	public void newEntry() {
		cnt++;
		dirty = true;
	}
	
	@Override
	public void beginTransaction() {
		inTransaction++;
	}
	
	@Override
	public synchronized void commit() throws Throwable {
		if (inTransaction > 0) inTransaction--;
		if (hasFlushed)	write();
	}
	
	@Override
	public boolean isInTransaction() {
		return inTransaction != 0;
	}
	
	@Override
	public synchronized void flush() throws Throwable {
		write();
	}
	
	@Override
	public Long getTime() {
		return lastTime;
	}
	
	@Override
	protected void finalize() throws Throwable {
		flush();
	}
	
	@Override
	public Integer getID() {
		return pid;
	}
	
	@Override
	public boolean isPartial() {
		return false;
	}
	
	@Override
	public int headerSize() {
		return -1;
	}
	
	@Override
	public int remainingSize() {
		return (int) (File.btreePageSize - size);
	}
	
	@Override
	public void markFree() {
		dirty = false;
	}

	

	public synchronized void append() {
		dataFile.append(buf);
	}

	public synchronized int getInt(int offset) {
		buf.position(offset);
		return buf.getInt();
	}
	
	public synchronized long getLong(int offset) {
		buf.position(offset);
		return buf.getLong();
	}
	
	public synchronized int putInt(int offset, int val) {
		buf.position(offset);
		buf.putInt(val);
		dirty = true;
		updateSize(buf.position());
		return Integer.SIZE / Byte.SIZE;
	}

	public synchronized int putLong(int offset, long val) {
		buf.position(offset);
		buf.putLong(val);
		dirty = true;
		updateSize(buf.position());
		return Long.SIZE / Byte.SIZE;
	}
	
	public synchronized String getString(int offset) {
		buf.position(offset);
		int len = buf.getInt();
		byte[] byteval = new byte[len];
		buf.get(byteval);
		return new String(byteval);
	}

	public synchronized int putString(int offset, String val) {
		dirty = true;
		return putBytes(offset, val.getBytes());
	}
	
	public synchronized byte[] getBytes(int offset) {
		buf.position(offset);
		int len = buf.getInt();
		byte[] byteval = new byte[len];
		buf.get(byteval);
		return byteval;
	}
	
	public synchronized int putBytes(int offset, byte[] byteval) {
		buf.position(offset);
		buf.putInt(byteval.length);
		buf.put(byteval);
		dirty = true;
		updateSize(buf.position());
		return byteval.length + Integer.SIZE / Byte.SIZE;
	}
	
	public synchronized int putByteArray(int offset, byte[] byteval) {
		buf.position(offset);
		buf.put(byteval);
		dirty = true;
		updateSize(buf.position());
		return byteval.length;
	}
	
	public synchronized BigDecimal getDecimal(int offset) {
		int scale = getInt(offset);
		offset += 4;
		byte[] bval = getBytes(offset);
		return new BigDecimal(new BigInteger(bval), scale);
	}
	
	public synchronized int putDecimal(int offset, BigDecimal val) {
		int ans = 0;
		ans += putInt(offset, val.scale());
		offset += 4;
		ans += putBytes(offset, val.unscaledValue().toByteArray());
		dirty = true;
		return ans;
	}
	
	private static class RawPageEntry implements Comparable<RawPageEntry>{
		public int remain, pid;
		
		RawPageEntry(int rem, int pid){
			this.remain = rem;
			this.pid = pid;
		}
		
		@Override
		public int compareTo(RawPageEntry o) {
			Integer a = remain;
			Integer b = o.remain;
			Integer p1 = pid;
			Integer p2 = o.pid;
			return 0==a.compareTo(b) ? p1.compareTo(p2) : a.compareTo(b);
		}
		
		@Override
		public boolean equals(Object o){
			if (o instanceof RawPageEntry)
				return (pid == ((RawPageEntry)o).pid);
			return false;
		}
	}
}
