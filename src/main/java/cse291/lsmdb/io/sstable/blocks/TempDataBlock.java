package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.sstable.SSTableConfig;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Created by musteryu on 2017/6/4.
 */
public class TempDataBlock extends AbstractBlock implements Comparable<TempDataBlock> {
    private final Descriptor desc;
    private int level, index, originIndex;
    private final String column;

    public TempDataBlock(
            Descriptor desc,
            String column,
            int level,
            int index,
            int originIndex,
            SSTableConfig config
    ) {
        super(config);
        this.desc = desc;
        this.level = level;
        this.index = index;
        this.originIndex = originIndex;
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

    public int getOriginIndex() {
        return originIndex;
    }

    @Override
    public File getFile() throws IOException {
        File dir = desc.getDir();
        File colDir = new File(dir, column);
        // <level>_<originIndex>_Data.db.tmp_<index>
        String filename = String.format(
            "%d_%d_Data%s_%d", level, originIndex, config.getTempBlockFilenameSuffix(), index
        );
        return new File(colDir, filename);
    }

    public ComponentFile getWritableComponentFile() throws IOException {
        if (!getFile().exists())
            getFile().createNewFile();
        if (!getFile().canWrite())
            getFile().setWritable(true);
        return new ComponentFile(getFile(), "rw");
    }

    public static boolean isTempDataBlock(String filename, SSTableConfig config) {
        String[] parts = filename.split("_");
        // <level>_<originIndex>_Data.db.tmp_<index>
        if (parts.length != 4) return false;
        if (!parts[2].equals("Data" + config.getTempBlockFilenameSuffix())) {
            return false;
        }
        try {
            int level = Integer.parseInt(parts[0]);
            int originIndex = Integer.parseInt(parts[1]);
            int index = Integer.parseInt(parts[3]);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static Optional<TempDataBlock> fromFileName(
            Descriptor desc,
            String column,
            String filename,
            SSTableConfig config
    ) {
        String[] parts = filename.split("_");
        // <level>_<originIndex>_Data.db.tmp_<index>
        if (parts.length != 4) return Optional.empty();
        if (!parts[2].equals("Data" + config.getTempBlockFilenameSuffix())) {
            return Optional.empty();
        }
        try {
            // <level>_<originIndex>_Data.db.tmp_<index>
            int level = Integer.parseInt(parts[0]);
            int originIndex = Integer.parseInt(parts[1]);
            int index = Integer.parseInt(parts[3]);
            return Optional.of(new TempDataBlock(desc, column, level, index, originIndex, config));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    @Override
    public int compareTo(@NotNull TempDataBlock that) {
        if (this.level < that.level) return -1;
        if (this.level > that.level) return  1;
        if (this.originIndex < that.originIndex) return -1;
        if (this.originIndex > that.originIndex) return  1;
        if (this.index < that.index) return -1;
        if (this.index > that.index) return  1;
        return 0;
    }
}
