package cse291.lsmdb.io.interfaces;

/**
 * Created by musteryu on 2017/6/4.
 */
public interface Hasher<T> {
    long hash(T t);
}
