package cse291.lsmdb.io.sstable.blocks;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlock extends AbstractBlock {
    private final Descriptor desc;
    private int level, index;

    public DataBlock(Descriptor desc, int level, int index) {
        this.desc = desc;
        this.level = level;
        this.index = index;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        String filename = String.format(
                "%d_%d_Data%s", level, index, DEFAULT_SUFFIX
        );
        return new File(dir, filename);
    }
}
