package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;

/**
 * Created by musteryu on 2017/6/3.
 */
public class MurMurHasher implements StringHasher {
    public MurMurHasher() {}
    @Override
    public int hash(String key) {
        //TODO: implement murmur hashing
        return 0;
    }
}
