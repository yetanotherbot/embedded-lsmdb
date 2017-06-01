package cse291.lsmdb.io.sstable;

import com.sun.istack.internal.NotNull;

import java.io.File;
import java.io.IOError;
import java.io.IOException;

/**
 * Created by musteryu on 2017/5/30.
 */
public class Descriptor {
    public final File directory;

    public final String rowKey;

    public Descriptor(@NotNull File directory, @NotNull String rowKey) {
        assert directory != null;
        assert rowKey != null;
        try {
            this.directory = directory.getCanonicalFile();
            this.rowKey = rowKey;
        } catch (IOException ioe) {
            throw new IOError(ioe);
        }
    }
}
