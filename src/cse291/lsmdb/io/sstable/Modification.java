package cse291.lsmdb.io.sstable;

import cse291.lsmdb.utils.Timed;

import java.util.Optional;

/**
 * Created by musteryu on 2017/6/3.
 */
public final class Modification {

    private final boolean isPut;
    private final boolean isNothing;
    private Timed<String> val;

    private static final Modification NOTHING = new Modification();


    public Modification(Timed<String> put) {
        isPut = true;
        isNothing = false;
        val = put;
    }

    public Modification(long timestamp) {
        isPut = false;
        isNothing = false;
        val = new Timed<>(null, timestamp);
    }

    private Modification() {
        isNothing = true;
        isPut = false;
        val = null;
    }

    public Timed<String> getIfPresent() {
        return isPut && !isNothing ? val : null;
    }

    public long getTimestamp() {
        return !isNothing ? val.getTimestamp() : -1;
    }

    public static Modification put(Timed<String> put) {
        return new Modification(put);
    }

    public static Modification remove(long timestamp) {
        return new Modification(timestamp);
    }

    public static Modification nothing() {
        return NOTHING;
    }

    public boolean isPut() {
        return !this.isNothing && this.isPut;
    }

    public boolean isRemove() {
        return !this.isNothing && !this.isPut;
    }

    public boolean isNothing() {
        return this.isNothing;
    }

    public static Modification select(Modification a, Modification b) {
        if (a.isNothing()) return b;
        if (b.isNothing()) return a;
        return a.getTimestamp() < b.getTimestamp() ? b : a;
    }

}
