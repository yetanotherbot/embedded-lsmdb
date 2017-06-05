package cse291.lsmdb.io.interfaces;

/**
 * Created by musteryu on 2017/6/3.
 */
public interface Filter {
    boolean isPresent(String key);
}
