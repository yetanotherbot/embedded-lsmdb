package cse291.lsmdb.io.sstable.blocks;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlock extends AbstractBlock implements Comparable<DataBlock> {
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

    @Override
    public int compareTo(DataBlock that) {
        if (this.level < that.level) return -1;
        if (this.level > that.level) return 1;
        return Integer.compare(this.index, that.index);
    }

    public static boolean isDataBlock(String filename) {
        String[] parts = filename.split("_");
        if (parts.length != 3) return false;
        if (parts[2].endsWith("Data" + DEFAULT_SUFFIX)) {
            return false;
        }
        return true;
    }

    public static Optional<DataBlock> fromFileName(Descriptor desc, String filename) {
        String[] parts = filename.split("_");
        if (parts.length != 3) return Optional.empty();
        if (parts[2].endsWith("Data" + DEFAULT_SUFFIX)) {
            return Optional.empty();
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int index = Integer.parseInt(parts[1]);
            return Optional.of(new DataBlock(desc, level, index));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }
}
