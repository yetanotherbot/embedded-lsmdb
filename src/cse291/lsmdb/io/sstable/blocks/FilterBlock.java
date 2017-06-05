package cse291.lsmdb.io.sstable.blocks;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class FilterBlock extends AbstractBlock {
    private final Descriptor desc;
    private int level;

    public FilterBlock(Descriptor desc, int level) {
        this.desc = desc;
        this.level = level;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        String filename = String.format(
                "%d_Filter%s", level, DEFAULT_SUFFIX
        );
        return new File(dir, filename);
    }
}
