package cse291.lsmdb.io.sstable;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/5/30.
 */
public class SSTableReader {
    private File dir;

    public SSTableReader(File base, String namespace, String columnFamily) throws IOException {
        dir = new File(base, namespace);
        if (!dir.isDirectory()) {
            throw new IOException("could not read SSTable from directory");
        }
    }

    public String getByLevel(String row, String col, int level) {
        String[] children = dir.list((d, name) -> name.startsWith(level + "_"));
        return null;
    }
}
