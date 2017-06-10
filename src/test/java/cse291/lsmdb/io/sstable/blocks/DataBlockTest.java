package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.utils.Pair;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class DataBlockTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void getIndex() throws Exception {
        for (int i = 0; i < 1000; i++) {
            DataBlock d = new DataBlock(desc, "col", 0, i, config);
            assertEquals(d.getIndex(), i);
        }
    }

    @Test
    public void getFile() throws Exception {
        DataBlock d = new DataBlock(desc, "col", 0, 10, config);
        d.requireFileExists();
        assertTrue(d.getFile().exists());
    }

    @Test(expected = RuntimeException.class)
    public void notIncludedCol() {
        DataBlock d = new DataBlock(desc, RandomStringUtils.random(50), 0, 0, config);
    }

    @Test
    public void compareTo() throws Exception {
        DataBlock d1 = new DataBlock(desc, "col", 0, 0, config);
        DataBlock d2 = new DataBlock(desc, "col", 0, 2, config);
        assertTrue(d1.compareTo(d2) < 0);
    }

    @Test
    public void isDataBlock() throws Exception {
        String name = DataBlock.buildFilename(desc, "col", 0, 1, config);
        System.out.println(name);
        assertTrue(DataBlock.isDataBlock(name, config));
    }

    @Test
    public void fromFileName() throws Exception {
        String name = DataBlock.buildFilename(desc, "col", 0, 1, config);
        Optional<DataBlock> opt = DataBlock.fromFileName(desc, "col", name, config);
        assertTrue(opt.isPresent());
        DataBlock block = new DataBlock(desc, "col", 0, 1, config);
        assertTrue(opt.get().compareTo(block) == 0);
    }

    @Test
    public void buildFilename() throws Exception {

    }

}