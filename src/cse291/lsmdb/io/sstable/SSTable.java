package cse291.lsmdb.io.sstable;

import java.io.File;

/**
 * Created by musteryu on 2017/5/30.
 */
public class SSTable {
    private final File file;

    public SSTable(File file) {
        this.file = file;
    }
}
