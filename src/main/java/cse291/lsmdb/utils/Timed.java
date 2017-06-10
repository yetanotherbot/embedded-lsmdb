package cse291.lsmdb.utils;


import java.util.Objects;

/**
 * Created by musteryu on 2017/6/1.
 */
public class Timed<T> {
    private T val;
    private long timestamp;

    public Timed(T val) {
        this(val, System.currentTimeMillis());
    }

    public Timed(T val, long timestamp) {
        this.val = val;
        this.timestamp = timestamp;
    }

    public T get() {
        return this.val;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public static <T> Timed<T> now(T t) {
        return new Timed<T>(t);
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof Timed) {
            final Timed<T> that = (Timed<T>) o;
            return Objects.equals(this.val, that.val) && this.timestamp == that.timestamp;
        }
        return false;
    }
}
