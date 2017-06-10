package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Timed;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class DataBlockDumperTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void dump() throws Exception {
        TempDataBlock t = new TempDataBlock(desc, "col", 0, 5, 3, config);
        t.requireFileExists();
        DataBlockDumper d = new DataBlockDumper(t, config.getPerBlockBloomFilterBits());
        Modifications mods1 = new Modifications(config.getBlockBytesLimit());
        BloomFilter f = new BloomFilter(config.getPerBlockBloomFilterBits(), config.getHasher());
        for (int i = 0; i < 1000; i++) {
            String r = RandomStringUtils.randomAlphanumeric(50);
            String v = RandomStringUtils.randomAlphanumeric(50);
            if (i % 2 == 0) mods1.put(r, Modification.put(Timed.now(v)));
            else mods1.put(r, Modification.remove(System.currentTimeMillis()));
            f.add(r);
        }
        d.dump(mods1, f);
        DataBlock db = new DataBlock(desc, "col", 0, 3, config);
        db.requireFileExists();
        System.out.println("rename: " + t.getFile().renameTo(db.getFile()));
        DataBlockLoader loader = new DataBlockLoader(
                db, config.getPerBlockBloomFilterBits(), config.getHasher());
        Modifications mods2 = loader.extractModifications(config.getBlockBytesLimit());
        assertEquals(mods1.size(), mods2.size());
        Iterator<String> itr1 = mods1.rows().iterator();
        Iterator<String> itr2 = mods2.rows().iterator();
        while (itr1.hasNext() && itr2.hasNext()) {
            String s1 = itr1.next();
            String s2 = itr2.next();
            System.out.println(itr1.next() + " = " + itr2.next());
            Modification m1 = mods1.get(s1);
            Modification m2 = mods2.get(s2);
            if (m1.isPut()) {
                System.out.println(
                        "put: " + m1.getTimestamp() + m1.getIfPresent().get() + " = " +
                                m2.getTimestamp() + m2.getIfPresent().get());
            } else {
                System.out.println(
                        "remove: " + m1.getTimestamp() + " = " + m2.getTimestamp());
            }
        }
        assertTrue(mods1.equals(mods2));
        assertEquals(mods1, mods2);
    }

}