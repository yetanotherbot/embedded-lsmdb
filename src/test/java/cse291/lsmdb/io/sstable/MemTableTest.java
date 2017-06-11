package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Modifications;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by musteryu on 2017/6/9.
 */
public class MemTableTest {
    private static Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    private SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void put() throws Exception {
        MemTable mt = new MemTable(desc, "col", config);
        for (int i = 0; i < 1000; i++) {
            String r = RandomStringUtils.randomAlphanumeric(50);
            String v = RandomStringUtils.randomAlphanumeric(50);
            mt.put(r, v);
            assertEquals(mt.get(r), v);
        }
    }

    @Test
    public void remove() throws Exception {
        MemTable mt = new MemTable(desc, "col", config);
        int j = 0;
        for (int i = 0; i < 1000; i++) {
            String r = RandomStringUtils.randomAlphanumeric(50);
            String v = RandomStringUtils.randomAlphanumeric(50);
            mt.put(r, v);
            assertEquals(mt.get(r), v);
            mt.remove(r);
            try {
                mt.get(r);
            } catch (NoSuchElementException e) {
                j++;
            }
        }
        assertEquals(j, 1000);
    }

    @Test
    public void getWithoutException() {
        MemTable mt = new MemTable(desc, "col", config);
        String[] row = new String[1000];
        String[] val = new String[1000];
        for (int i = 0; i < 1000; i++) {
            row[i] = RandomStringUtils.randomAlphanumeric(50);
            val[i] = RandomStringUtils.randomAlphanumeric(50);
            try {
                mt.put(row[i], val[i]);
            } catch (MemTable.MemTableFull memTableFull) {
                memTableFull.printStackTrace();
            }
        }
        for (int i = 999; i >= 0; --i) {
            assertEquals(mt.get(row[i]), val[i]);
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void getWithException() {
        MemTable mt = new MemTable(desc, "col", config);
        mt.get(RandomStringUtils.randomAlphanumeric(500));
    }

    @Test
    public void stealModifications() throws Exception {
        MemTable mt = new MemTable(desc, "col", config);
        String[] row = new String[1000];
        String[] val = new String[1000];
        for (int i = 0; i < 1000; i++) {
            row[i] = RandomStringUtils.randomAlphanumeric(50);
            val[i] = RandomStringUtils.randomAlphanumeric(50);
            try {
                mt.put(row[i], val[i]);
            } catch (MemTable.MemTableFull memTableFull) {
                memTableFull.printStackTrace();
            }
        }
        Modifications mod = mt.stealModifications();
        for (int i = 0; i < 1000; i++) {
            String r = row[i];
            String v = val[i];
            assertEquals(mod.get(r).getIfPresent().get(), v);
        }

        int ec = 0;
        for (int i = 0; i < 1000; i++) {
            String r = row[i];
            try {
                mt.get(r);
            } catch (Exception e) {
                ++ec;
            }
        }
        assertEquals(ec, 1000);
    }

    @Test
    public void existLimitNoEdit() throws Exception {
        MemTable mt = new MemTable(desc, "col", config);
        int bytesLimit = config.getBlockBytesLimit();
        int bytesInserted = 0;
        try {
            do {
                String row = RandomStringUtils.randomAlphanumeric(100);
                String value = RandomStringUtils.randomAlphabetic(100);
                bytesInserted += value.getBytes().length + Long.BYTES;
                mt.put(row, value);
            } while (bytesInserted < bytesLimit);
        } catch (MemTable.MemTableFull e) {
            System.out.println(bytesInserted);
            System.out.println(bytesLimit);
            assertTrue(bytesInserted >= bytesLimit);
        }
    }
}