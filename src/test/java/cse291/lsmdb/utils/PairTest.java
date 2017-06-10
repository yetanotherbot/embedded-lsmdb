package cse291.lsmdb.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class PairTest {
    @Test
    public void equals() throws Exception {
        Pair<String, String> p1 = new Pair<>("l1", "r1");
        Pair<String, String> p2 = new Pair<>("l2", "r2");
        Pair<String, String> p3 = new Pair<>("l1", "r1");
        Assert.assertNotEquals(p1, p2);
        Assert.assertEquals(p1, p3);
    }

    @Test
    public void comparator() throws Exception {
        Comparator<Pair<String, String>> c = Pair.<String, String>comparator();
        for (int i = 0; i < 1000; i++) {
            String l1 = RandomStringUtils.randomAlphanumeric(1000);
            String l2 = RandomStringUtils.randomAlphanumeric(1000);
            String r1 = RandomStringUtils.randomAlphanumeric(1000);
            String r2 = RandomStringUtils.randomAlphanumeric(1000);
            Pair<String, String> p1 = new Pair<>(l1, r1);
            Pair<String, String> p2 = new Pair<>(l2, r2);
            Assert.assertEquals(c.compare(p1, p2), comp(l1, r1, l2, r2));
            if (c.compare(p1, p2) == 0) {
                Assert.assertEquals(l1.compareTo(l2), 0);
                Assert.assertEquals(r1.compareTo(r2), 0);
            }
            if (c.compare(p1, p2) < 0) {
                Assert.assertTrue((l1.compareTo(l2) < 0) || ((l1.equals(l2) && (r1.compareTo(r2) < 0))));
            }
            if (c.compare(p1, p2) > 0) {
                Assert.assertTrue((l1.compareTo(l2) > 0) || ((l1.equals(l2) && (r1.compareTo(r2) > 0))));
            }
        }
    }

    private static int comp(String l1, String r1, String l2, String r2) {
        if (l1.compareTo(l2) < 0) return -1;
        if (l1.compareTo(l2) > 0) return 1;
        if (r1.compareTo(r2) < 0) return -1;
        if (r1.compareTo(r2) > 0) return 1;
        return 0;
    }

}