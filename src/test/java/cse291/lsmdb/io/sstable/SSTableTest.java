package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class SSTableTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col1", "col2"});
    SSTableConfig config = SSTableConfig.builder()
            .setMemTablesLimit(4)
            .build();

    SSTable sst = new SSTable(desc, "col1", config);

    @Test
    public void get() throws Exception {
        System.out.println(new Date());
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            String row = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            String val = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            Optional<String> v = sst.get(row);
            if (i % 2 == 0) {
                assertTrue(v.isPresent());
                System.out.println("put: " + row + ", " + v.get());
            } else {
                assertFalse(v.isPresent());
                System.out.println("delete: " + row);
            }
        }
        System.out.println(new Date());
    }

//    @Ignore
    @Test
    public void put() throws Exception {
        System.out.println(new Date());
        for (int i = 0; i < Integer.MAX_VALUE / 16; i++) {
            String row = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            String val = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            if (i % 2 == 0) {
                sst.put(row, val);
            } else {
                sst.put(row, null);
            }
        }
        System.out.println(new Date());
    }

}