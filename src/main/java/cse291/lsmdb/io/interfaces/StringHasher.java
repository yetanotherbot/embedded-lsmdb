package cse291.lsmdb.io.interfaces;

/**
 * Created by musteryu on 2017/5/29.
 */
public interface StringHasher extends Hasher<String> {
    long hash(String key);
}
