package cse291.lsmdb.utils;


import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.WritableFilter;

import java.util.*;

/**
 * Created by musteryu on 2017/6/6.
 */
public class Modifications extends TreeMap<String, Modification> {
    private final int bytesLimit;
    private int bytesNum;
    private boolean immtable;

    public Modifications(int limit) {
        bytesLimit = limit;
        bytesNum = 0;
        immtable = false;
    }

    public Modifications(Modifications other) {
        this.bytesLimit = other.bytesLimit;
        this.bytesNum = other.bytesNum;
        immtable = false;
        putAll(other);
    }

    @Override
    public Modification put(String row, Modification curr) {
        if (immtable) throw new UnsupportedOperationException();
        if (containsKey(row)) {
            Modification last = get(row);
            if (last.getTimestamp() > curr.getTimestamp()) return null;
            if (last.isPut()) {
                bytesNum -= byteLen(last.getIfPresent().get());
            }
            if (curr.isPut()) {
                bytesNum += byteLen(curr.getIfPresent().get());
            }
        } else {
            if (curr.isPut()) bytesNum += byteLen(curr.getIfPresent().get());
            bytesNum += Long.BYTES;
            bytesNum += byteLen(row);
        }
        super.put(row, curr);
        return curr;
    }

    public void putAll(Modifications m) {
        for (Map.Entry<String, Modification> entry: m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    private static int byteLen(String s) {
        return s.getBytes().length;
    }

    public boolean existLimit() {
        return bytesNum >= bytesLimit;
    }

    public int bytesNum() {
        return bytesNum;
    }

    public Set<String> rows() {
        return super.keySet();
    }

    public String firstRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.first();
    }

    public String lastRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.last();
    }

    public static Modifications merge(Modifications m1, Modifications m2, int limit) {
        Modifications m = new Modifications(limit);
        for (String r: m1.rows()) m.put(r, m1.get(r));
        for (String r: m2.rows()) m.put(r, m2.get(r));
        return m;
    }

    public Filter calculateFilter(WritableFilter f) {
        for (String row: rows()) {
            f.add(row);
        }
        return f;
    }

    /**
     * Merge two Modifications together, and split them again with the first one full of entries
     * @param m1 first Modifications
     * @param m2 second Modifications
     * @param limit the byte limit for each Modifications
     * @return array of Modifications
     */

    public static Queue<Modifications> reassign(Modifications m1, Modifications m2, int limit) {
        Queue<Modifications> mods = new LinkedList<>();
        Modifications total = merge(m1, m2, Integer.MAX_VALUE);
        Modifications m = new Modifications(limit);
        for (Map.Entry<String, Modification> entry : total.entrySet()) {
            if (m.existLimit()) {
                mods.offer(m);
                m = new Modifications(limit);
            }
            m.put(entry.getKey(), entry.getValue());
        }
        mods.offer(m);
        return mods;
    }

    public boolean offer(Modifications modifications) {
        putAll(modifications);
        return existLimit();
    }

    @Override
    public void clear() {
        bytesNum = 0;
        immtable = false;
        super.clear();
    }

    public Modifications poll() {
        if (!existLimit()) {
            Modifications ret = new Modifications(this);
            clear();
            return ret;
        } else {
            Modifications ret = new Modifications(bytesLimit);
            List<String> shouldClean = new ArrayList<>();
            for (String r: rows()) {
                ret.put(r, get(r));
                shouldClean.add(r);
                if (ret.existLimit()) {
                    break;
                }
            }
            for (String sc: shouldClean) {
                remove(sc);
            }
            return ret;
        }
    }

    /**
     * Gets the immutable reference of the modifications
     * @param mods the mods to refer
     * @return an immutable reference of the modifications
     */
    public static Modifications immutableRef(Modifications mods) {
        Modifications ref = new Modifications(mods);
        ref.immtable = true;
        return ref;
    }
}
