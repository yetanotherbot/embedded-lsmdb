package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.Loadable;
import cse291.lsmdb.io.interfaces.ReadableFilter;
import cse291.lsmdb.io.interfaces.StringHasher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by musteryu on 2017/6/3.
 */
public class FilterLoader implements Loadable, ReadableFilter {
    public static final String DEFAULT_SUFFIX = "Filter.db";
    private Filter filter;
    private StringHasher hasher;
    private final RandomAccessFile raf;

    FilterLoader(File f, String suffix, StringHasher hasher) throws IOException {
        if (!f.isFile() || f.canRead() || f.getName().endsWith(suffix)) {
            throw new IOException("could not load SSTable Filter from the file");
        }
        raf = new RandomAccessFile(f, "r");
        this.hasher = hasher;
    }

    FilterLoader(File f, StringHasher hasher) throws IOException {
        this(f, DEFAULT_SUFFIX, hasher);
    }

    public void loadInMemory() throws IOException {
        int len = ((int) (raf.length() / Long.BYTES));
        long[] words = new long[len];
        int idx = 0;
        while (raf.length() - raf.getFilePointer() < Long.BYTES) {
            words[idx++] = raf.readLong();
        }
        filter = new BloomFilter(words, hasher);
    }

    @Override
    public boolean isPresent(String row) {
        return filter.isPresent(row);
    }

    public Filter getFilter() {
        return filter;
    }
}
