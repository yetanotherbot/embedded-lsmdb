package cse291.lsmdb.utils;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class ModificationTest {
    @Test
    public void getIfPresent() throws Exception {
        Modification m = Modification.put(Timed.now("row"));
        Assert.assertTrue(m.getIfPresent() != null);
        Modification r = Modification.remove(System.currentTimeMillis());
        Assert.assertNull(r.getIfPresent());
    }

    @Test
    public void getTimestamp() throws Exception {
        for (int i = 0; i < 1000; i++) {
            long ts = RandomUtils.nextLong();
            Assert.assertEquals(Modification.put(new Timed<String>("aa", ts)).getTimestamp(), ts);
            Assert.assertEquals(Modification.remove(ts).getTimestamp(), ts);
        }
    }

    @Test
    public void put() throws Exception {
        Modification m = Modification.put(Timed.now("row"));
        Assert.assertEquals(m.getIfPresent().get(), "row");
        Assert.assertEquals(m.getTimestamp(), System.currentTimeMillis());
        Assert.assertTrue(m.isPut());
        Assert.assertFalse(m.isNothing());
    }

    @Test(expected = NullPointerException.class)
    public void remove() throws Exception {
        Modification r = Modification.remove(System.currentTimeMillis());
        Assert.assertNull(r.getIfPresent());
        Assert.assertEquals(r.getTimestamp(), System.currentTimeMillis());
        Assert.assertTrue(r.isRemove());
        Assert.assertFalse(r.isNothing());

        // should throw exception
        Assert.assertNull(r.getIfPresent().get());
    }

    @Test
    public void nothing() throws Exception {
        Assert.assertTrue(Modification.nothing().isNothing());
    }

    @Test
    public void select() throws Exception {
        long l1 = RandomUtils.nextLong(), l2 = RandomUtils.nextLong();
        Modification m1, m2;

        m1 = Modification.put(new Timed<String>("row", l1));
        m2 = Modification.put(new Timed<String>("row", l2));
        assertEquals(Modification.select(m1, m2).getTimestamp(), Math.max(l1, l2));

        m1 = Modification.put(new Timed<String>("row", l1));
        m2 = Modification.remove(l2);
        assertEquals(Modification.select(m1, m2).getTimestamp(), Math.max(l1, l2));

        m1 = Modification.remove(l1);
        m2 = Modification.put(new Timed<String>("row", l2));
        assertEquals(Modification.select(m1, m2).getTimestamp(), Math.max(l1, l2));

        m1 = Modification.remove(l1);
        m2 = Modification.remove(l2);
        assertEquals(Modification.select(m1, m2).getTimestamp(), Math.max(l1, l2));
    }

}