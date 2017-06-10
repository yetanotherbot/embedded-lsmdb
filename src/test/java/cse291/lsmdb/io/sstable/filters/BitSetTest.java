package cse291.lsmdb.io.sstable.filters;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class BitSetTest {

    @Test
    public void set() throws Exception {
        BitSet bs = new BitSet(1024);
        long[] longs = new long[1024 / Long.SIZE];
        for (int i = 0; i < 1024 / Long.SIZE; i++) {
            longs[i] = RandomUtils.nextLong();
        }
        for (int j = 0; j < 1024 / Long.SIZE; j++) {
            long l = longs[j];
            for (int i = 0; i < Long.SIZE; i++) {
                if ((l & 1) != 0) {
                    bs.set(j * Long.SIZE + i);
                }
                l >>= 1;
            }
        }
        assertArrayEquals(longs, bs.toLongs());
    }

    @Test
    public void clear() throws Exception {
        BitSet bs = new BitSet(1024);
        for (int i = 0; i < 1024; i++) {
            assertFalse(bs.get(i));
            bs.set(i);
            assertTrue(bs.get(i));
        }
        for (int i = 0; i < 1024; i++) {
            bs.clear(i);
            assertFalse(bs.get(i));
        }
    }

    @Test
    public void get() throws Exception {
        BitSet bs = new BitSet(1024);
        for (int i = 0; i < 1024; i++) {
            assertFalse(bs.get(i));
            bs.set(i);
            assertTrue(bs.get(i));
        }
    }

    @Test
    public void size() throws Exception {
        BitSet bs = new BitSet(1024);
        assertEquals(bs.numLongs(), 1024 / Long.SIZE);
    }

    @Test
    public void toLongs() throws Exception {
        long[] longs = new long[1024];
        assertEquals(longs.length, new BitSet(longs).numLongs());
    }

}