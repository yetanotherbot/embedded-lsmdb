package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;

/**
 * Created by musteryu on 2017/6/3.
 */
public class MurMurHasher implements StringHasher {
    public MurMurHasher() {
    }

    /**
     * MurmurHash algorithm to hash a string into a 32-bit integer
     * Idea from https://sites.google.com/site/murmurhash/
     *
     * @param key the string to be hashed
     * @return the hash value 32-bit int casted as a 64-bit long
     */
    @Override
    public long hash(String key) {
        byte[] data = key.getBytes();
        int length = data.length;
        int seed = 42;

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = length & 0xfffffffc;  // round down to 4 byte block

        for (int i = 0; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (length & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= length;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }


}
