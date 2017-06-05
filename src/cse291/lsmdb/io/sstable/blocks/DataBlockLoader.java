package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.utils.Modification;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlockLoader {
    private final DataBlock dataBlock;
    private final int bloomFilterLen;
    private final StringHasher hasher;

    public DataBlockLoader(DataBlock block, int bloomFilterLen, StringHasher hasher) {
        dataBlock = block;
        this.bloomFilterLen = bloomFilterLen;
        this.hasher = hasher;
    }

    public Modification get(String row, String col) {
        try {
            ComponentFile component = dataBlock.getComponentFile();
            component.seek(0);
            while (component.getFilePointer() < component.length()) {
                char currRowLen = component.readChar();
                byte[] currRowBytes = new byte[currRowLen];
                component.read(currRowBytes, 0, currRowLen);
                String currRow = new String(currRowBytes);
                if (currRow.compareTo(row) < 0) {
                    // shift the the position of <last column's absolute offset> of the current row
                    component.readInt();
                    int lastColOffset = component.readInt();
                    component.seek(lastColOffset);
                    component.readColumnModification();
                } else if (currRow.equals(row)) {
                    int firstColOffset = component.readInt();
                    int lastColOffset = component.readInt();
                    Filter filter = component.readFilter(bloomFilterLen / Long.SIZE, hasher);
                    if (!filter.isPresent(col)) {
                        // not present in the current
                        throw new NoSuchElementException();
                    } else {
                        char indexLen = component.readChar();
                        int[] index = new int[indexLen];
                        return component.binarySearch(col, index);
                    }
                } else break;
            }
            throw new NoSuchElementException("could not find the row: " + row);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new NoSuchElementException("could not find the element due to IOException: " + ioe.getMessage());
        }
    }
}
