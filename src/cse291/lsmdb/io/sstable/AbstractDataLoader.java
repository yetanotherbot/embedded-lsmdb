package cse291.lsmdb.io.sstable;

import cse291.lsmdb.utils.Timed;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/3.
 */
public abstract class AbstractDataLoader {
    abstract public Timed<String> get(String row, String col) throws NoSuchElementException, IOException;
}
