package cse291.lsmdb.utils;

import cse291.lsmdb.io.sstable.SSTableConfig;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

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
        int bytesLimit = config.getBlockBytesLimit();
        int bytesInserted = 0;
    }

    @Test
    public void bytesNum() throws Exception {
        int bytesInserted = 0;
        for(int i = 0; i < 100; i++){
            String row = String.format("test%d",i);
            String value = RandomStringUtils.randomAlphabetic(100);
            mod.put(row, Modification.put(Timed.now(value)));
            bytesInserted += value.getBytes().length + Long.BYTES;
            Assert.assertEquals(bytesInserted, mod.bytesNum());
        }

        for(int i = 0; i < 100; i++){
            String row = String.format("test%d",i);
            String value = RandomStringUtils.randomAlphabetic(200);
            String last = RandomStringUtils.randomAlphabetic(100);
            mod.put(row, Modification.put(Timed.now(value)));
            bytesInserted += value.getBytes().length - last.getBytes().length;
            Assert.assertEquals(bytesInserted, mod.bytesNum());
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
        Modifications mod1 = new Modifications(bytesLimit);
        Modifications mod2 = new Modifications(bytesLimit);

        for (int i = 0;!mod1.existLimit() && !mod2.existLimit(); i += 2){

            String row1 = String.format("test%d",i);
            String row2 = String.format("test%d",i+1);

            String value = RandomStringUtils.randomAlphabetic(100);
            mod1.put(row1,Modification.put(Timed.now(value)));
            mod2.put(row2,Modification.put(Timed.now(value)));
        }
        
        List<String> keys = new ArrayList<>();
        keys.addAll(mod1.keySet());
        keys.addAll(mod2.keySet());
        Collections.sort(keys);

        Queue<Modifications> reassignedMods = Modifications.reassign(mod1,mod2,bytesLimit);

        mod1 = reassignedMods.remove();
        mod2 = reassignedMods.remove();

        for(int i = 0; i < mod1.size(); i++){
            Assert.assertTrue(mod1.containsKey(keys.get(i)));
        }

        for(int i = mod1.size(); i < mod1.size() + mod2.size(); i++){
            Assert.assertTrue(mod2.containsKey(keys.get(i)));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void immutableRef() throws Exception {
        Modifications imMod = Modifications.immutableRef(mod);
        imMod.put("good", Modification.put(Timed.now("bad")));
    }

    @Test
    public void offer() throws Exception {
        Modifications mod = new Modifications(config.getBlockBytesLimit());
        while (!mod.existLimit()) {
            String v = RandomStringUtils.randomAlphabetic(50);
            mod.put(RandomStringUtils.randomAlphabetic(50), Modification.put(Timed.now(v)));
        }
        Modifications pool = new Modifications(config.getBlockBytesLimit());
        Assert.assertFalse(pool.existLimit());
        Assert.assertTrue(pool.offer(mod));
    }

    @Test
    public void poll() throws Exception {
        Modifications mod = new Modifications(config.getBlockBytesLimit() * 4);
        while (!mod.existLimit()) {
            String v = RandomStringUtils.randomAlphabetic(50);
            mod.put(RandomStringUtils.randomAlphabetic(50), Modification.put(Timed.now(v)));
        }
        Modifications pool = new Modifications(config.getBlockBytesLimit());
        pool.offer(mod);
        Modifications res = new Modifications(config.getBlockBytesLimit() * 4);
        int i = 0;
        while (!pool.isEmpty()) {
            if (i < 3) Assert.assertFalse(res.offer(pool.poll()));
            else Assert.assertTrue(res.offer(pool.poll()));
            i++;
        }
        Assert.assertTrue(pool.isEmpty());
        Assert.assertEquals(res, mod);
    }
}