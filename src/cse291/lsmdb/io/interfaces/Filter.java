package cse291.lsmdb.io.interfaces;

/**
 * Created by musteryu on 2017/5/29.
 */
public interface Filter extends ReadableFilter {
    void add(String key);
}
