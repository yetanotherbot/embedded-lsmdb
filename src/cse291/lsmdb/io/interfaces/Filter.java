package cse291.lsmdb.io.interfaces;

/**
 * Created by musteryu on 2017/5/29.
 */
public interface Filter {
    boolean isPresent(String key);
    void add(String key);
}
