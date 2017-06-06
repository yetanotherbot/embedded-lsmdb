package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.Modification;

import java.io.*;
import java.util.*;
import java.util.function.Function;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlockDumper {
    private final TempDataBlock tmpDataBlock;
    private final int filterBits;

    public DataBlockDumper(TempDataBlock tmpDataBlock, int filterBits) {
        this.tmpDataBlock = tmpDataBlock;
        this.filterBits = filterBits;
    }

    public void dump(
            Map<String, Modification> modifications,
            BloomFilter filter,
            Function<Filter, long[]> toLongs
    ) throws IOException {
        ComponentFile c = null;
        try {
            c = tmpDataBlock.getWritableComponentFile();
            long[] longs = toLongs.apply(filter);
            if (longs.length != filterBits) throw new IOException("filter length mismatch");
            c.writeFilter(filter, toLongs);
            SortedSet<String> rowset = new TreeSet<>(modifications.keySet());
            for (String row: rowset) {
                c.writeChars(row + "\n");
                Modification mod = modifications.get(row);
                if (mod.isPut()) {
                    c.writeChars(mod.getIfPresent().get() + "\n");
                } else {
                    c.writeChar('\n');
                }
                c.writeLong(mod.getTimestamp());
            }
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
