package cse291.lsmdb.utils;


import java.util.Set;
import java.util.TreeMap;

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
            bytesNum += byteLen(curr.getIfPresent().get());
            bytesNum += Long.BYTES;
        }
        return curr;
    }

    private static int byteLen(String s) {
        return s.getBytes().length;
    }

    public boolean existLimit() {
        return bytesNum > bytesLimit;
    }

    public int bytesNum() {
        return bytesNum;
    }

    public Set<String> rows() {
        return keySet();
    }

    public static Modifications merge(Modifications m1, Modifications m2, int limit) {
        Modifications m = new Modifications(limit);
        for (String r: m1.rows()) m.put(r, m1.get(r));
        for (String r: m2.rows()) m.put(r, m2.get(r));
        return m;
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
