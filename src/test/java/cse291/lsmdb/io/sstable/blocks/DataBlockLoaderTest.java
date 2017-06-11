package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by musteryu on 2017/6/9.
 */
public class DataBlockLoaderTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void get() throws Exception {
        DataBlock block = new DataBlock(desc, "col", 0, 3, config);
        DataBlockLoader loader = new DataBlockLoader(
                block, config.getPerBlockBloomFilterBits(), config.getHasher());
        Modifications mod = loader.extractModifications(config.getBlockBytesLimit());
        for (String row : mod.rows()) {
            Modification fetch = loader.get(row);
            if (mod.get(row).isPut()) {
                System.out.println(fetch.getIfPresent().get() + " = " + mod.get(row).getIfPresent().get());
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            } else {
                System.out.println("delete");
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            }
            assertTrue(fetch.equals(mod.get(row)));
        }
    }

}