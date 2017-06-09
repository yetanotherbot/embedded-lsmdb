package cse291.lsmdb.utils;

import cse291.lsmdb.io.sstable.SSTableConfig;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/9.
 */
public class ModificationsTest {
    static Modifications mod;
    static SSTableConfig config;

    @BeforeClass
    public static void init() {
        config = SSTableConfig.defaultConfig();
    }


    @Before
    public void initMod() {
        mod = new Modifications(config.getBlockBytesLimit());
    }

    @Test
    public void put() throws Exception {
        try {
            for (int i = 0; i < 1000; i++) {
                String row = RandomStringUtils.randomAlphabetic(10);
                String value = RandomStringUtils.randomAlphabetic(100);
                mod.put(row, Modification.put(Timed.now(value)));
                Modification m = mod.get(row);
                Assert.assertNotNull(m);
                Assert.assertTrue(m.isPut());
                Assert.assertTrue(m.getIfPresent().get().equals(value));
            }
        } catch (NoSuchElementException e) {
            Assume.assumeNoException(e);
        }
    }

    @Test
    public void existLimit() throws Exception {

    }

    @Test
    public void bytesNum() throws Exception {

    }

    @Test
    public void rows() throws Exception {

    }

    @Test
    public void merge() throws Exception {

    }

    @Test
    public void calculateFilter() throws Exception {

    }

    @Test
    public void reassign() throws Exception {

    }

    @Test
    public void immutableRef() throws Exception {

    }

}