package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.sstable.SSTableConfig;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlock extends AbstractBlock implements Comparable<DataBlock> {
    private final Descriptor desc;
    private String column;
    private int level, index;

    public DataBlock(Descriptor desc, String column, int level, int index, SSTableConfig config) {
        super(config);
        this.desc = desc;
        this.level = level;
        this.index = index;
        if (!desc.hasColumn(column)) {
            throw new RuntimeException("should not happen");
        }
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        File colDir = new File(dir, column);
        if (!colDir.exists()) {
            colDir.mkdirs();
        }
        String filename = buildFilename(desc, column, level, index, config);
        return new File(colDir, filename);
    }

    @Override
    public int compareTo(DataBlock that) {
        int colCmp = this.column.compareTo(that.column);
        if (colCmp < 0) return -1;
        if (colCmp > 0) return 1;
        if (this.level < that.level) return -1;
        if (this.level > that.level) return 1;
        return Integer.compare(this.index, that.index);
    }

    public static boolean isDataBlock(String filename, SSTableConfig config) {
        // <level>_<index>_Data.db
        String[] parts = filename.split("_");
        if (parts.length != 3) return false;
        if (!parts[2].endsWith("Data" + config.getBlockFilenameSuffix())) {
            return false;
        }
        return true;
    }

    public static boolean isDataBlockForLevel(String filename, SSTableConfig config, int level) {
        // <level>_<index>_Data.db
        String[] parts = filename.split("_");
        if (parts.length != 3) return false;
        if (Integer.parseInt(parts[0]) != level) return false;
        if (!parts[2].endsWith("Data" + config.getBlockFilenameSuffix())) {
            return false;
        }
        return true;
    }


    public static Optional<DataBlock> fromFileName(Descriptor desc, String column, String filename, SSTableConfig config) {
        String[] parts = filename.split("_");
        if (parts.length != 3) return Optional.empty();
        if (!parts[2].endsWith("Data" + config.getBlockFilenameSuffix())) {
            return Optional.empty();
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int index = Integer.parseInt(parts[1]);
            return Optional.of(new DataBlock(desc, column, level, index, config));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    public static String buildFilename(Descriptor desc, String column, int level, int index, SSTableConfig config) {
        String filename = String.format(
                "%d_%d_Data%s", level, index, config.getBlockFilenameSuffix()
        );
        return filename;
    }
}
