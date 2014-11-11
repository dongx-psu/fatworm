package fatworm.btree;

public abstract class BKey implements Comparable<BKey> {
	public abstract void delete() throws Throwable;
	
	public abstract int compareTo(BKey o);
	
	public int keySize() {
		return Integer.SIZE / Byte.SIZE;
	}
	
	public abstract byte[] toByte() throws Throwable;
}