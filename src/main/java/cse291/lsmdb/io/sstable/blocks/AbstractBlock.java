package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Block;
import cse291.lsmdb.io.sstable.SSTableConfig;
import org.apache.commons.compress.compressors.CompressorException;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
abstract class AbstractBlock implements Block {
    protected final SSTableConfig config;

    public AbstractBlock(SSTableConfig config) {
        this.config = config;
    }

    public abstract File getFile() throws IOException;

    public ComponentFile getReadableComponentFile() throws IOException {
        return new ComponentFile(getFile(), config.getFileBufferSize());
    }

    public ComponentFile getCompressibleReadableComponentFile() throws IOException, CompressorException {
        return new ComponentFile(
                getFile(),
                config.getFileBufferSize(),
                config.getCompressorProvider(),
                config.getCompressorType()
        );
    }

    public void requireFileExists() throws IOException {
        File file = getFile();
        if (!file.exists()) {
            String parent = file.getParent();
            File dir = new File(parent);
            if (!dir.exists()) dir.mkdirs();
            file.createNewFile();
        }
    }

    public void requireFileWritable() throws IOException {
        requireFileExists();
        File file = getFile();
        if (!file.canWrite())
            file.setWritable(true);
    }
}
