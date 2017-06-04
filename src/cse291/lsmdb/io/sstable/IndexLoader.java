package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.Loadable;
import cse291.lsmdb.utils.Pair;
import static cse291.lsmdb.utils.RandomAccessUtils.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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

public class IndexLoader implements Loadable {
    public static final String DEFAULT_SUFFIX = "Index.db";
    private final RandomAccessFile raf;
    private Pair<RowCol, RowCol>[] ranges;

    public IndexLoader(File f, String suffix) throws IOException {
        if (!f.isFile() || !f.canRead() || f.getName().endsWith(suffix)) {
            throw new IOException("could not load SSTable Index from the file");
        }
        raf = new RandomAccessFile(f, "r");
    }

    public IndexLoader(File f) throws IOException {
        this(f, DEFAULT_SUFFIX);
    }

    public void loadInMemory() throws IOException {
        int offsetsNum = raf.readInt();
        int[] offsets = readOffsets(offsetsNum);
        ranges = new Pair[offsetsNum];
        for (int i = 0; i < offsetsNum; i++) {
            RowCol rc1 = readRowCol();
            RowCol rc2 = readRowCol();
            if (rc1.compareTo(rc2) > 0) {
                throw new RuntimeException("should not happen, first row col is larger than the last one");
            }
            ranges[i] = new Pair<>(rc1, rc2);
        }
    }

    /**
     * Get the range of Data block i.
     * @param i the index of Data block to get the range
     * @return the range of the selected Data block
     */
    public Pair<RowCol, RowCol> rangeOf(int i) {
        if (i < 0 || i >= ranges.length) {
            throw new IndexOutOfBoundsException(
                    "data block index out of bound: [0, " + ranges.length + ")"
            );
        }
        return ranges[i];
    }

    /**
     * Lookups a RowCol key by binary searching the Index file. If no Data block matches the
     * RowCol key, return -1.
     * @param rc RowCol key to lookup
     * @return Data block index or -1
     */
    public int lookup(RowCol rc) {
        int lo = 0, hi = ranges.length;
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            Pair<RowCol, RowCol> range = ranges[mid];
            if (rc.compareTo(range.right) > 0) {
                lo = mid + 1;
            } else if (rc.compareTo(range.left) < 0) {
                hi = mid;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * Reads offsets from the file. The file pointer will be pushed forward.
     * @param size number of offsets
     * @return offsets
     * @throws IOException if I/O error happens or EOF
     */
    private int[] readOffsets(int size) throws IOException {
        int[] offsets = new int[size];
        for (int i = 0; i < size; ++i) {
            offsets[i] = raf.readInt();
        }
        return offsets;
    }

    private RowCol readRowCol() throws IOException {
        byte[] rowBytes = readVarLength(raf, VarLengthType.VAR_LENGTH_16);
        String row = new String(rowBytes);

        byte[] colBytes = readVarLength(raf, VarLengthType.VAR_LENGTH_16);
        String col = new String(colBytes);

        return new RowCol(row, col);
    }
}
