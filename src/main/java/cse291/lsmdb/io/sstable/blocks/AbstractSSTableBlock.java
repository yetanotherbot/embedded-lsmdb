package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Modification;

import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/4.
 */
public abstract class AbstractSSTableBlock {

    public abstract Modification get(String row) throws NoSuchElementException;
}
