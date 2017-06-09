package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Timed;

import java.io.*;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlockLoader extends AbstractSSTableBlock {
    private final DataBlock dataBlock;
    private final int bloomFilterBits;
    private final StringHasher hasher;

    public DataBlockLoader(DataBlock block, int bloomFilterBits, StringHasher hasher) {
        dataBlock = block;
        this.bloomFilterBits = bloomFilterBits;
        this.hasher = hasher;
    }

    @Override
    public Modification get(String row) throws NoSuchElementException {
        ComponentFile c = null;
        try {
            c = dataBlock.getReadableComponentFile();
            Filter filter = c.readFilter(bloomFilterBits / Long.SIZE, longs -> new BloomFilter(longs, hasher));
            if (!filter.isPresent(row)) {
                throw new NoSuchElementException("no such element");
            }
            while (c.getFilePointer() < c.length()) {
                String crow = c.readLine();
                String cval = c.readLine();
                long timestamp = c.readLong();
                if (crow.equals(row)) {
                    if (cval.length() == 0) {
                        return Modification.remove(timestamp);
                    } else {
                        return Modification.put(new Timed<>(cval));
                    }
                }
            }
            throw new NoSuchElementException("no such element");
        } catch (IOException ioe) {
            throw new NoSuchElementException("could not find element due to IOException " + ioe.getMessage());
        } finally {
            ComponentFile.tryClose(c);
        }
    }

    public Modifications extractModifications(int limit) throws IOException {
        ComponentFile c = null;
        try {
            c = dataBlock.getReadableComponentFile();
            for (int i = 0; i < bloomFilterBits / Long.SIZE; i++) {
                c.readLong();
            }
            Modifications mods = new Modifications(limit);
            while (c.getFilePointer() < c.length()) {
                String crow = c.readLine();
                String cval = c.readLine();
                long timestamp = c.readLong();
                if (cval.length() == 0) {
                    mods.put(crow, Modification.remove(timestamp));
                } else {
                    mods.put(crow, Modification.put(new Timed<>(cval, timestamp)));
                }
            }

            return mods;
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
