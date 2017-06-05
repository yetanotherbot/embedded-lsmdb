package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.sstable.filters.BloomFilter;

import java.io.IOException;

/**
 * Created by musteryu on 2017/6/3.
 */
public class FilterBlockLoader implements Filter {
    private final FilterBlock filterBlock;
    private final StringHasher hasher;

    public FilterBlockLoader(FilterBlock filterBlock, StringHasher hasher) {
        this.filterBlock = filterBlock;
        this.hasher = hasher;
    }

    @Override
    public boolean isPresent(String row) {
        try {
            return getFilter().isPresent(row);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("failed to load filter from Filter file");
            return false;
        }
    }

    public Filter getFilter() throws IOException {
        ComponentFile c = new ComponentFile(filterBlock.getFile());
        int len = ((int) (c.length() / Long.BYTES));
        long[] words = new long[len];
        int idx = 0;
        while (c.length() - c.getFilePointer() < Long.BYTES) {
            words[idx++] = c.readLong();
        }
        c.close();
        return new BloomFilter(words, hasher);
    }
}
