package cse291.lsmdb.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by musteryu on 2017/6/9.
 */
public class TimedTest {
    @Test
    public void get() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String a = RandomStringUtils.randomAlphanumeric(100);
            assertEquals(new Timed<String>(a).get(), a);
        }
    }

    @Test
    public void getTimestamp() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String a = RandomStringUtils.randomAlphanumeric(100);
            long ts = RandomUtils.nextLong();
            assertEquals(new Timed<String>(a, ts).getTimestamp(), ts);
        }
    }

    @Test
    public void now() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String a = RandomStringUtils.randomAlphanumeric(100);
            assertTrue(Math.abs(new Timed<String>(a).getTimestamp() - System.currentTimeMillis()) <= 5);
        }
    }

}