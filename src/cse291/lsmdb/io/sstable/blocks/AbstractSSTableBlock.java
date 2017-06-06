package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.io.sstable.MurMurHasher;
import cse291.lsmdb.utils.RowCol;

import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/4.
 */
public abstract class AbstractSSTableBlock {
    public static final int DEFAULT_BLOOM_FILTER_BITS = 1024;
    public static StringHasher DEFAULT_HASHER = new MurMurHasher();

    public abstract Modification get(String row) throws NoSuchElementException;
}
