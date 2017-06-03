package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.StringHasher;

/**
 * Created by musteryu on 2017/5/29.
 */
public class BloomFilter implements Filter {
    private BitSet bitset;
    private StringHasher hasher;

    public BloomFilter(int numBits, StringHasher hasher) {
        this.bitset = new BitSet(numBits);
        this.hasher = hasher;
    }

    public BloomFilter(long[] words, StringHasher hasher) {
        this.bitset = new BitSet(words);
        this.hasher = hasher;
    }

    public boolean isPresent(String key) {
       return bitset.get(bitOffset(key));
    }

    public void add(String key) {
        bitset.set(bitOffset(key));
    }

    private int bitOffset(String key) {
        int hash = hasher.hash(key);
        return hash % bitset.size();
    }

    public long[] toLongs() {
        return bitset.toLongs();
    }
}
