package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.Loader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by musteryu on 2017/6/3.
 */
public class IndexLoader {
    public static final String DEFAULT_SUFFIX = "Index.db";
    private final RandomAccessFile raf;

    public IndexLoader(File f, String suffix) throws IOException {
        if (!f.isFile() || !f.canRead() || f.getName().endsWith(suffix)) {
            throw new IOException("could not load SSTable Index from the file");
        }
        raf = new RandomAccessFile(f, "r");
    }

    public IndexLoader(File f) throws IOException {
        this(f, DEFAULT_SUFFIX);
    }
}
