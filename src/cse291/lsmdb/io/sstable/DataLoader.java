package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.utils.Pair;
import cse291.lsmdb.utils.Timed;

import java.io.*;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Created by musteryu on 2017/6/2.
 */
public class DataLoader {
    public static final String DEFAULT_SUFFIX = "Data.db";
    public static final int DEFAULT_BLOOMFILTER_LEN = 128;
    private final RandomAccessFile raf;

    public DataLoader(File f, String suffix) throws IOException {
        if (!f.isFile() || !f.canRead() || f.getName().endsWith(suffix)) {
            throw new IOException("could not load SSTable from file");
        }
        raf = new RandomAccessFile(f, "r");
    }

    public DataLoader(File f) throws IOException {
        this(f, DEFAULT_SUFFIX);
    }

    /**
     * Gets the value in the file for selected row and column. If the data is not presented in
     * the file, the method will throw a NoSuchElementException.
     * @param row the selected row
     * @param col the selected column
     * @return the value with timestamp
     * @throws NoSuchElementException if the value is not found
     * @throws IOException if an I/O error happens
     */
    public Timed<String> get(String row, String col) throws NoSuchElementException, IOException {
        raf.seek(0);
        while (raf.getFilePointer() < raf.length()) {
            char currRowLen = raf.readChar();
            byte[] currRowBytes = new byte[currRowLen];
            raf.read(currRowBytes, 0, currRowLen);
            String currRow = new String(currRowBytes);
            if (currRow.compareTo(row) < 0) {
                // shift the the position of <last column's absolute offset> of the current row
                raf.readInt();
                int lastColOffset = raf.readInt();
                raf.seek(lastColOffset);
                readColumn();
            } else if (currRow.equals(row)) {
                int firstColOffset = raf.readInt();
                int lastColOffset = raf.readInt();
                Filter filter = readFilter(DEFAULT_BLOOMFILTER_LEN / Long.SIZE, MurMurHasher::new);
                if (!filter.isPresent(col)) {
                    // not present in the current
                    throw new NoSuchElementException();
                } else {
                    char indexLen = raf.readChar();
                    int[] index = new int[indexLen];
                    return binarySearch(col, index);
                }
            } else break;
        }
        throw new NoSuchElementException("could not find the row: " + row);
    }

    private Timed<String> binarySearch(String col, int[] index)
            throws NoSuchElementException, EOFException, IOException {
        int lo = 0, hi = index.length;
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            int idx = index[mid];
            raf.seek(idx);
            Pair<String, Timed<String>> midColPair = readColumn();
            String midCol = midColPair.left;
            int cmp = midCol.compareTo(col);
            if (cmp == 0) return midColPair.right;
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        throw new NoSuchElementException("could not find the column: " + col);
    }

    /**
     * Reads a column-value pair from the current position. The method reads a column, its value
     * and timestamp from the current position and shift the file pointer to the starting position
     * of next column of next row. The format of a column in the data file is:
     * Row Length              | Row Data  | Data Length             | Data      | Timestamp
     * unsigned 16 bits (char) | varlength | unsigned 16 bits (char) | varlength | long
     * @return the next column-value pair of the file
     * @throws EOFException if the file reaches the end in the process of reading the whole column
     * @throws IOException if an I/O error happens
     */
    private Pair<String, Timed<String>> readColumn() throws EOFException, IOException {
        // build a column name
        char colLen = raf.readChar();
        byte[] colBytes = new byte[colLen];
        raf.read(colBytes, 0, colLen);
        String col = new String(colBytes);

        // build a column data
        char dataLen = raf.readChar();
        byte[] dataBytes = new byte[dataLen];
        raf.read(dataBytes, 0, dataLen);
        String data = new String(dataBytes);

        // build a timestamp
        long timestamp = raf.readLong();
        return new Pair<>(col, new Timed<>(data, timestamp));
    }

    /**
     * Reads a filter from the current position with specified number of longs and hasher factory
     * function.
     * @param numLongs num of longs
     * @param hasherFactory factory function to create a StringHasher
     * @return a bloom filter associated with a string hasher
     * @throws EOFException if the file reaches the end in the process of reading the filter
     * @throws IOException if an I/O error happens
     */
    private Filter readFilter(int numLongs, Supplier<StringHasher> hasherFactory)
            throws EOFException, IOException {
        long[] filterWords = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            filterWords[i] = raf.readLong();
        }
        return new BloomFilter(filterWords, hasherFactory.get());
    }
}
