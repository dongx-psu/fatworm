package fatworm.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import fatworm.io.BufferManager;
import fatworm.io.File;
import fatworm.table.Record;
import fatworm.table.Schema;

public class RecordPage extends RawPage {
	private Integer count = 0;
	private List<Integer> offsets = new ArrayList<Integer>();
	private List<Record> records = null;
	private byte [] pBytes;
	private BufferManager bm;
	
	public RecordPage(BufferManager bm, File f, int pageid, boolean create) throws Throwable {
		lastTime = System.currentTimeMillis();
		dataFile = f;
		pid = pageid;
		byte [] tmp = new byte[(int) File.recordPageSize];
		this.bm = bm;
		if (!create) {
			dataFile.read(tmp, pid);
			fromBytes(tmp);
		} else {
			nextPID = pid;
			prevPID = pid;
			dirty = true;
			this.buf = ByteBuffer.wrap(tmp);
			int length = offsets.size() > 0 ? offsets.get(offsets.size()-1) : 0;
			pBytes = new byte[length];
			buf.position((int) (File.recordPageSize - length));
			buf.get(pBytes);
			ArrayUtils.reverse(pBytes);
			records = new ArrayList<Record>();
		}
	}

	@Override
	public void fromBytes(byte[] b) throws Throwable {
		ByteBuffer buf = ByteBuffer.wrap(b);
		count = buf.getInt();
		prevPID = buf.getInt();
		nextPID = buf.getInt();
		for (int i = 0; i < count; i++)
			offsets.add(buf.getInt());
		
		if (isPartial()) {
			int length = buf.getInt();
			pBytes = new byte[length];
			buf.get(pBytes);
		} else {
			int length = (offsets.size() > 0) ? offsets.get(offsets.size()-1) : 0;
			pBytes = new byte[length];
			buf.position((int) (File.recordPageSize - length));
			buf.get(pBytes);
			ArrayUtils.reverse(pBytes);
		}
		this.buf = buf;
	}
	
	@Override
	public byte[] toBytes() throws Throwable {
		if (pBytes.length + headerSize() <= File.recordPageSize) {
			dirty = true;
			buf.position(0);
			buf.putInt(count);
			buf.putInt(prevPID);
			buf.putInt(nextPID);
			for (int i = 0; i < count; i++)
				buf.putInt(offsets.get(i));
			
			if (!isPartial()) {
				byte[] tmp = expandAndReverse(pBytes);
				buf.position((int) (File.recordPageSize - tmp.length));
				buf.put(tmp);
			} else {
				buf.putInt(pBytes.length);
				buf.put(pBytes);
			}
		} else {
			int now = pid;
			int curOffset = 0;
			int realNext = nextPID;
			List<Integer> tmpOffset = new ArrayList<Integer>(offsets);
			List<Record> tmpRecord = new ArrayList<Record>(records);
			int tmpCount = count;
			while (curOffset < tmpCount) {
				int cntFit = 1;
				for (; cntFit + curOffset < tmpCount && canThisFit(tmpOffset, curOffset, cntFit); cntFit++);
				cntFit = Math.min(tmpCount - curOffset, cntFit);
				if (!canThisFit(tmpOffset, curOffset, cntFit)) cntFit--;
				
				if (curOffset == 0) {
					count = Math.max(1, cntFit);
					offsets = offsets.subList(0, count);
					records = records.subList(0, count);
				}
				
				if(cntFit >= 1){
					now = fitRecords(now, tmpRecord.subList(curOffset, curOffset + cntFit));
					curOffset += cntFit;
				} else {
					int recordSize = tmpOffset.get(curOffset) - (curOffset>0 ? tmpOffset.get(curOffset - 1) : 0);
					now = fitPartial(bm, pBytes, now, curOffset, recordSize);
					curOffset += 1;
				}
			}
			
			RecordPage rp = bm.getRecordPage(realNext, false);
			rp.beginTransaction();
			rp.dirty = rp.prevPID!=now;
			rp.prevPID = now;
			rp.commit();
			rp = bm.getRecordPage(now, false);
			rp.beginTransaction();
			rp.dirty = rp.nextPID!=realNext;
			rp.nextPID = realNext;
			rp.commit();
		}
		return buf.array();
	}

	private boolean canThisFit(List<Integer> bakOffset, int curOffset, int cntFit) {
		return bakOffset.get(curOffset + cntFit - 1) - (curOffset > 0 ? bakOffset.get(curOffset - 1) : 0) + getHeaderSize(cntFit) <= File.recordPageSize;
	}

	private Integer fitRecords(int now, List<Record> records)	throws Throwable {
		int prev;
		prev = now;
		now = bm.newPage();
		if (prev >= 0) {
			RecordPage pp = bm.getRecordPage(prev, false);
			pp.beginTransaction();
			pp.dirty = (pp.nextPID != now);
			pp.nextPID = now;
			pp.commit();
		}
		
		RecordPage curPage = bm.getRecordPage(now, true);
		curPage.beginTransaction();
		curPage.dirty = true;
		curPage.prevPID = prev;
		int cntFit = records.size();
		for (int j = 0; j < cntFit; j++) {
			boolean flag = curPage.tryAppendRecord(records.get(j));
			assert flag;
		}
		
		curPage.commit();
		return now;
	}
	
	private Integer fitRecords(Integer now, Record r) throws Throwable {
		Integer prev;
		prev = now;
		now = bm.newPage();
		if (prev != null) {
			RecordPage pp = bm.getRecordPage(prev, false);
			pp.beginTransaction();
			pp.dirty = true;
			pp.nextPID = now;
			pp.commit();
		}
		RecordPage curPage = bm.getRecordPage(now, true);
		curPage.beginTransaction();
		curPage.dirty = true;
		curPage.prevPID = prev;
		boolean flag = curPage.tryAppendRecord(r);
		assert flag;
		curPage.commit();
		return now;
	}

	private static Integer fitPartial(BufferManager bm, byte[] bytes, Integer now, int startOffset, int recordSize) throws Throwable {
		Integer prev;
		int fittedSize = 0;
		int maxFitSize = getMaxFitSize();
		while (fittedSize < recordSize) {
			prev = now;
			now = bm.newPage();
			if (prev != null) {
				RecordPage prevPage = bm.getRecordPage(prev, false);
				prevPage.beginTransaction();
				prevPage.dirty = true;
				prevPage.nextPID = now;
				prevPage.commit();
			}
			
			RecordPage curPage = bm.getRecordPage(now, true);
			curPage.beginTransaction();
			curPage.dirty = true;
			curPage.prevPID = prev;
			curPage.putPartial(bytes, startOffset + fittedSize, maxFitSize, fittedSize + maxFitSize >= recordSize);
			fittedSize += maxFitSize;
			curPage.commit();
		}
		return now;
	}
	
	private static int getMaxFitSize(){
		return (int) (File.recordPageSize - getHeaderSize(1) - Integer.SIZE / Byte.SIZE);
	}
	
	private void putPartial(byte[] b, int s, int maxFitSize, boolean end) throws Throwable {
		beginTransaction();
		dirty = true;
		count = 1;
		records = new ArrayList<Record>();
		int length = Math.min(maxFitSize, b.length - s);
		pBytes = new byte[length];
		System.arraycopy(b, s, pBytes, 0, length);
		offsets = new ArrayList<Integer>();
		offsets.add(end ? -2 : -1);
		commit();
	}

	private byte[] expandAndReverse(byte[] z) {
		byte[] tmp = new byte[(int) (File.recordPageSize - headerSize())];
		System.arraycopy(z, 0, tmp, 0, z.length);
		ArrayUtils.reverse(tmp);
		return tmp;
	}

	public boolean isPartial(){
		return (count == 1 && offsets.get(0) == -1);
	}
	
	public boolean endOfPartial(){
		return (count == 1 && offsets.get(0) == -2);
	}
	
	public int headerSize(){
		return getHeaderSize(count);
	}
	
	public static int getHeaderSize(int cnt){
		return (3 + cnt) * Integer.SIZE / Byte.SIZE;
	}
	
	public List<Record> getRecords(Schema schema){
		parseRecord(schema);
		return records;
	}
	
	private void parseRecord(Schema schema) {
		if (records != null) return;
		records = new ArrayList<Record>();
		buf.position(headerSize());
		int lastOffset = 0;
		for (int i = 0; i < count; i++) {
			int length = offsets.get(i) - lastOffset;
			byte [] tmp = new byte[length];
			System.arraycopy(pBytes, lastOffset, tmp, 0, length);
			records.add(Record.fromByte(schema, tmp));
			lastOffset = offsets.get(i);
		}
	}
	
	public void addRecord(Record r, int idx){
		dirty = true;
		parseRecord(r.schema);
		records.add(idx, r);
		byte[] recordBytes = Record.toByte(r);
		int lengthOfRecord = recordBytes.length;
		int totalLength = offsets.size() > 0 ? offsets.get(offsets.size() - 1) : 0;
		byte[] newb = new byte[totalLength + lengthOfRecord];
		int lastOffset = idx > 0 ? offsets.get(idx - 1) : 0;
		System.arraycopy(pBytes, 0, newb, 0, lastOffset);
		System.arraycopy(recordBytes, 0, newb, lastOffset, lengthOfRecord);
		System.arraycopy(pBytes, lastOffset, newb, lastOffset + lengthOfRecord, totalLength - lastOffset);
		pBytes = newb;
		offsets.add(idx, lastOffset + lengthOfRecord);
		for (int i = idx + 1; i<offsets.size(); i++)
			offsets.set(i, offsets.get(i) + lengthOfRecord);
		count++;
	}
	
	public boolean tryAppendRecord(Record r){
		parseRecord(r.schema);
		byte[] recordBytes = Record.toByte(r);
		int lengthOfRecord = recordBytes.length;
		
		if (lengthOfRecord > getMaxFitSize())
			return appendPartialRecord(recordBytes, r, true);
		else if (pBytes.length + lengthOfRecord + getHeaderSize(count+1) > File.recordPageSize)
			return appendPartialRecord(recordBytes, r, false);
		addRecord(r, count);
		return true;
	}
	
	private boolean appendPartialRecord(byte[] recordBytes, Record r, boolean split) {
		try {
			dirty = true;
			Integer realNext = nextPID;
			Integer now = split ? fitPartial(bm, recordBytes, pid, 0, recordBytes.length) : fitRecords(pid, r);
			RecordPage rp = bm.getRecordPage(realNext, false);
			rp.beginTransaction();
			rp.dirty = rp.prevPID!=now;
			rp.prevPID = now;
			rp.commit();
			rp = bm.getRecordPage(now, false);
			rp.beginTransaction();
			rp.dirty = rp.nextPID!=realNext;
			rp.nextPID = realNext;
			rp.commit();
			return true;
		} catch (Throwable e) {
			e.printStackTrace();
			return false;
		}
	}

	public void delRecord(Schema schema, int idx){
		dirty = true;
		parseRecord(schema);
		records.remove(idx);
		int totalLength = offsets.size() > 0 ? offsets.get(offsets.size() - 1) : 0;
		int lastOffset = (idx > 0 ? offsets.get(idx - 1) : 0);
		int lengthOfRecord = offsets.get(idx) - lastOffset;
		byte[] tmp = new byte[totalLength - lengthOfRecord];
		System.arraycopy(pBytes, 0, tmp, 0, lastOffset);
		System.arraycopy(pBytes, offsets.get(idx), tmp, lastOffset, totalLength - offsets.get(idx));
		pBytes = tmp;
		offsets.remove(idx);
		for(int i = idx; i < offsets.size(); i++)
			offsets.set(i, offsets.get(i) - lengthOfRecord);
		count--;
	}
	
	public void delete(){
		count = null;
		pBytes = null;
		offsets = null;
		records = null;
	}
	
	public boolean tryReclaim() {
		if (!canReclaim()) return false;
		try {
			RecordPage rp = bm.getRecordPage(prevPID, false);
			rp.beginTransaction();
			rp.dirty = true;
			rp.nextPID = nextPID;
			rp.commit();
			rp = bm.getRecordPage(nextPID, false);
			rp.beginTransaction();
			rp.dirty = true;
			rp.prevPID = prevPID;
			rp.commit();
			delete();
			bm.releasePage(pid);
			return true;
		} catch (Throwable e) {
			e.printStackTrace();
			return false;
		}
	}

	public byte [] getPartialBytes() {
		return pBytes;
	}

	public void setPartialBytes(byte [] partialBytes) {
		this.pBytes = partialBytes;
		dirty = true;
	}

	public boolean canReclaim() {
		return (count <= 0 && nextPID!=pid);
	}
}
