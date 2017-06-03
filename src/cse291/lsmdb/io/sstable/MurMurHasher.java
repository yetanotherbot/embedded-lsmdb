package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;

/**
 * Created by musteryu on 2017/6/3.
 */
public class MurMurHasher implements StringHasher {
    public MurMurHasher() {}
    @Override
    // The murmurhash implementation borrows the idea from https://sites.google.com/site/murmurhash/
    // It generates a 64 bit long as the hash value
    public long hash(String key) {
        //TODO: implement murmur hashing
        byte[] data = key.getBytes();
        int length = data.length;
        int seed = 42;

        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (seed&0xffffffffl)^(length*m);

        int length8 = length/8;

        for (int i=0; i<length8; i++) {
            final int i8 = i*8;
            long k =  ((long)data[i8]&0xff)    +(((long)data[i8+1]&0xff)<<8)
                +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        switch (length%8) {
            case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
            case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
            case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
            case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
            case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
            case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
            case 1: h ^= (long)(data[length&~7]&0xff);
                h *= m;
        };

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h;
    }
}
