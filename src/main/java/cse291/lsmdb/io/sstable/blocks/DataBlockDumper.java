package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;

import java.io.*;

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

    /**
     * Dumps the modifications into the current temporary block. The number of longs in
     * the filter should match the filterBits.
     * @param modifications modifications to dump
     * @param filter the bloom filter to dump in the file
     * @throws IOException
     */
    public void dump(Modifications modifications, Filter filter) throws IOException {
        tmpDataBlock.requireFileExists();
        ComponentFile c = null;
        try {
            c = tmpDataBlock.getWritableComponentFile();
            long[] longs = filter.toLongs();
            if (longs.length * Long.SIZE != filterBits) throw new IOException("filter length mismatch");
            c.writeFilter(filter);
            for (String row: modifications.rows()) {
//                System.out.println(row);
                c.writeUTF(row);
                Modification mod = modifications.get(row);
                if (mod.isPut()) {
                    c.writeUTF(mod.getIfPresent().get());
                } else {
                    c.writeUTF("");
                }
                c.writeLong(mod.getTimestamp());
            }
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
