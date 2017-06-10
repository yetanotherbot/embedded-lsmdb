package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class SSTableTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col1", "col2"});
    SSTableConfig config = SSTableConfig.builder()
            .setMemTablesLimit(4)
            .setMemTablesFlushStrategy(n -> n / 2)
            .build();

    SSTable sst = new SSTable(desc, "col1", config);

    @Test
    public void get() throws Exception {

    }

//    @Ignore
    @Test
    public void put() throws Exception {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            String row = "test" + RandomStringUtils.randomAlphabetic(50) + RandomUtils.nextInt(100) + RandomStringUtils.randomAlphabetic(2);
            String val = "test" + RandomStringUtils.randomAlphabetic(50) + RandomUtils.nextInt(100) + RandomStringUtils.randomAlphabetic(2);
            if (i % 2 == 0) {
                sst.put(row, val);
            } else {
                sst.put(row, null);
            }
        }
    }

}