package fatworm.io;

import java.nio.ByteBuffer;

import fatworm.io.File;

public class ByteBuilder {
	ByteBuffer buf;
	int size;
	public ByteBuilder(){
		buf = ByteBuffer.allocate((int) File.recordPageSize);
	}
	public void putBool(boolean v) {
		putByte(v ? (byte)1 : (byte)0);
	}

	public void putByte(byte b) {
		ensureCapacity(1);
		buf.put(b);
		updateSize();
	}
	
	private void updateSize() {
		size = Math.max(buf.position(), size);
	}

	public void putChar(char v){
		ensureCapacity(2);
		buf.putChar(v);
		updateSize();
	}
	
	public void putInt(int v) {
		ensureCapacity(4);
		buf.putInt(v);
		updateSize();
	}

	public void putString(String v) {
		putBytes(v.getBytes());
	}

	public void putBytes(byte[] b) {
		putInt(b.length);
		putBytes(b, 0, b.length);
	}
	
	public void putByteArray(byte[] b) {
		putBytes(b, 0, b.length);
	}

	public void putBytes(byte[] b, int i, int length) {
		ensureCapacity(length);
		buf.put(b, i, length);
		updateSize();
	}

	public void putLong(long v) {
		ensureCapacity(8);
		buf.putLong(v);
		updateSize();
	}

	public void putDouble(double v) {
		ensureCapacity(8);
		buf.putDouble(v);
		updateSize();
	}
	
	public byte[] getByteArray(){
		return buf.array();
	}
	
	public void putFloat(float v) {
		ensureCapacity(4);
		buf.putFloat(v);
		updateSize();
	}
	
	private void ensureCapacity(int i) {
		if (buf.capacity() >= size + i)
			return;
		int ptr = buf.position();
		byte[] newBuff = new byte[buf.capacity() * 2];
		byte[] oldBuff = buf.array();
		System.arraycopy(oldBuff, 0, newBuff, 0, size);
		buf = ByteBuffer.wrap(newBuff);
		buf.position(ptr);
	}

	public int getSize(){
		return size;
	}
}