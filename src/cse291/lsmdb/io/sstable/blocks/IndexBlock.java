package cse291.lsmdb.io.sstable.blocks;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/3.
 * A loader for Index file. It is used to know the key range of each data block.
 * The format of the Index file for now is:
 * ----------------------------------------------------------------------------
 *                       n: #offset of Indices in this file (32)
 * ----------------------------------------------------------------------------
 *  offset 1 (32) |  offset 2 (32) |  offset 3 (32) |   ....   |  offset n (32)
 * ----------------------------------------------------------------------------
 *  First row length (16) | name (var) | First col length (16) | name (var)    |
 *                                                                              > Block 1
 *  Last row length (16)  | name (var) | Last col length (16)  | name (var)    |
 * ----------------------------------------------------------------------------
 *                                     ...
 * ----------------------------------------------------------------------------
 *  First row length (16) | name (var) | First col length (16) | name (var)    |
 *                                                                              > Block n
 *  Last row length (16)  | name (var) | Last col length (16)  | name (var)    |
 * ----------------------------------------------------------------------------
 * The numbers in parentheses indicate the length of bits of the field.
 * If the index file is small enough (in most case), user could load it in memory.
 */

public class IndexBlock extends AbstractBlock {
    private final Descriptor desc;
    private final int level;

    public IndexBlock(Descriptor desc, int level) {
        this.desc = desc;
        this.level = level;
    }

    @Override
    public File getFile() throws IOException {
        String filename = String.format(
                "%d_Index%s", level, DEFAULT_SUFFIX
        );
        return new File(desc.getDir(), filename);
    }
}
