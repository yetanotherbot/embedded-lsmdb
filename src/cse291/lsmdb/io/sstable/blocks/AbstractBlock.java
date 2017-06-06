package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Block;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
abstract class AbstractBlock implements Block {
    public static final String DEFAULT_SUFFIX = ".db";
    public abstract File getFile() throws IOException;
    public ComponentFile getReadableComponentFile() throws IOException {
        return new ComponentFile(getFile());
    }
}
