package cse291.lsmdb.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by musteryu on 2017/6/2.
 */
public class Counter<T> {
    private Map<T, Integer> cnt;
    public Counter() {
        cnt = new HashMap<T, Integer>();
    }

    public void inc(T t) {
        if (!cnt.containsKey(t)) {
            cnt.put(t, 0);
        }
        cnt.put(t, cnt.get(t) + 1);
    }

    public void dec(T t) {
        cnt.put(t, cnt.get(t) - 1);
    }

    public int getCnt(T t) {
        if (!cnt.containsKey(t)) {
            return 0;
        }
        return cnt.get(t);
    }
}
