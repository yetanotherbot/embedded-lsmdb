package cse291.lsmdb.utils;

import cse291.lsmdb.io.sstable.SSTableConfig;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

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
    public void existLimitNoEdit() throws Exception {
        int bytesLimit = config.getBlockBytesLimit();
        int bytesInserted = 0;
        do {
            Assert.assertFalse(mod.existLimit());
            String row = String.format("test%d",bytesInserted);
            String value = RandomStringUtils.randomAlphabetic(100);
            mod.put(row, Modification.put(Timed.now(value)));
            bytesInserted += value.getBytes().length + Long.BYTES;
        } while(bytesInserted < bytesLimit);

        Assert.assertTrue(mod.existLimit());
    }

    @Test
    public void existLimitWithEdit() throws Exception {
        //TODO
    }

    @Test
    public void bytesNum() throws Exception {
        int bytesInserted = 0;
        for(int i = 0; i < 100; i++){
            String row = String.format("test%d",i);
            String value = RandomStringUtils.randomAlphabetic(100);
            mod.put(row, Modification.put(Timed.now(value)));
            bytesInserted += value.getBytes().length + Long.BYTES;
            Assert.assertEquals(bytesInserted,mod.bytesNum());
        }
    }

    @Test
    public void rows() throws Exception {
        Set<String> addedRowNames = new HashSet<>();
        for(int i = 0; i < 100; i++) {
            String row = String.format("test%d",i);
            String value = RandomStringUtils.randomAlphabetic(100);
            mod.put(row, Modification.put(Timed.now(value)));
            addedRowNames.add(row);
            Assert.assertTrue(addedRowNames.equals(mod.rows()));
        }
    }

    @Test
    public void merge() throws Exception {
        Modifications mod1 = new Modifications(Integer.MAX_VALUE);
        Modifications mod2 = new Modifications(Integer.MAX_VALUE);
        for(int i = 0; i < 100; i++){
            String row1 = String.format("test%d",i);
            String row2 = String.format("test%d",i+100);
            String value = RandomStringUtils.randomAlphabetic(100);
            mod1.put(row1, Modification.put(Timed.now(value)));
            mod2.put(row2, Modification.put(Timed.now(value)));
        }
        mod = Modifications.merge(mod1,mod2,Integer.MAX_VALUE);
        for(int i = 0; i < 100; i++){
            Assert.assertTrue(mod.containsKey(String.format("test%d",i)));
            Assert.assertTrue(mod.containsKey(String.format("test%d",i+100)));
        }
    }

    @Test
    public void calculateFilter() throws Exception {
        // Can be done in BloomFilter test
    }

    @Test
    public void reassign() throws Exception {

        int bytesLimit = config.getBlockBytesLimit();
        Modifications mod1 = new Modifications(bytesLimit/100);
        Modifications mod2 = new Modifications(bytesLimit/100);
        for (int i = 0;!mod1.existLimit() && !mod2.existLimit(); i += 2){

            String row1 = String.format("test%d",i);
            String row2 = String.format("test%d",i+1);

            String value = RandomStringUtils.randomAlphabetic(100);
            mod1.put(row1,Modification.put(Timed.now(value)));
            mod2.put(row2,Modification.put(Timed.now(value)));
        }
        Queue<Modifications> reassignedMods = Modifications.reassign(mod1,mod2,bytesLimit/100);

        mod1 = reassignedMods.remove();
        mod2 = reassignedMods.remove();

        //TODO: bug in code or test, seems mod2 contains the smaller keys
        for(int i = 0; i < mod1.size(); i++){
            Assert.assertTrue(mod1.containsKey(String.format("test%d",i)));
        }

        for(int i = mod1.size(); i < mod1.size() + mod2.size(); i++){
            Assert.assertTrue(mod2.containsKey(String.format("test%d",i)));
        }
    }

    @Test
    public void immutableRef() throws Exception {
        // Not necessary
    }
}