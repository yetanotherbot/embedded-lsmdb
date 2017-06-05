package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.utils.Modification;

import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DiskSSTableBlock extends AbstractSSTableBlock {
    private final DataBlockLoader loader;

    public DiskSSTableBlock(
            Descriptor desc,
            int level,
            int index,
            int bloomfilterLen,
            StringHasher hasher
    ) {
        DataBlock block = new DataBlock(desc, level, index);
        loader = new DataBlockLoader(block, bloomfilterLen, hasher);
    }

    public DiskSSTableBlock(Descriptor desc, int level, int index) {
        this(desc, level, index, DEFAULT_BLOOM_FILTER_LEN, DEFAULT_HASHER);
    }

    @Override
    public Modification get(String row, String col) throws NoSuchElementException {
        return get(row, col);
    }
}
