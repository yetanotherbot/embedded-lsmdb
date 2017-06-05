package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Pair;
import cse291.lsmdb.utils.RowCol;

import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class IndexBlockLoader {
    private IndexBlock indexBlock;

    public IndexBlockLoader(IndexBlock indexBlock) {
        this.indexBlock = indexBlock;
    }

    /**
     * Get the range of Data block i.
     * @param i the index of Data block to get the range
     * @return the range of the selected Data block
     */
    public Pair<RowCol, RowCol> rangeOf(int i) throws IOException {
        int rangeLength = getRangeLength();
        if (i < 0 || i >= rangeLength) {
            throw new IndexOutOfBoundsException(
                    "data block index out of bound: [0, " + rangeLength + ")"
            );
        }
        Pair<RowCol, RowCol>[] ranges = getRanges();
        return ranges[i];
    }

    /**
     * Lookups a RowCol key by binary searching the Index file. If no Data block matches the
     * RowCol key, return -1.
     * @param rc RowCol key to lookup
     * @return Data block index or -1
     */
    public int lookup(RowCol rc) {
        try {
            Pair<RowCol, RowCol>[] ranges = getRanges();
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
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return -1;
    }

    public int getRangeLength() throws IOException {
        ComponentFile c = null;
        try {
            c = new ComponentFile(indexBlock.getFile());
            return c.readInt();
        } finally {
            if (c != null) c.close();
        }
    }

    public Pair<RowCol, RowCol>[] getRanges() throws IOException {
        ComponentFile c = new ComponentFile(indexBlock.getFile());
        int offsetsNum = c.readInt();
        int[] offsets = c.readOffsets(offsetsNum);
        Pair<RowCol, RowCol>[] ranges = new Pair[offsetsNum];
        for (int i = 0; i < offsetsNum; i++) {
            RowCol rc1 = c.readRowNameColName();
            RowCol rc2 = c.readRowNameColName();
            if (rc1.compareTo(rc2) > 0) {
                throw new RuntimeException("should not happen, first row col is larger than the last one");
            }
            ranges[i] = new Pair<>(rc1, rc2);
        }
        c.close();
        return ranges;
    }
}
