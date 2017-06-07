package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;

import java.io.*;
import java.util.function.Function;

/**
 * Created by musteryu on 2017/6/3.
 */
public class ComponentFile extends RandomAccessFile {

    public ComponentFile(File f, String mode) throws FileNotFoundException {
        super(f, mode);
    }

    public ComponentFile(File f) throws FileNotFoundException {
        super(f, "r");
    }

    public boolean tryClose() {
        try {
            close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static boolean tryClose(ComponentFile c) {
        if (c != null) return c.tryClose();
        return true;
    }

    /**
     * Write the BloomFilter of the SSTableBlock as 128 bit longs
     * @param filter the BloomFilter of the data
     * @throws IOException if an I/O error happens
     */
    public void writeFilter(Filter filter) throws IOException{
        long[] filterLongs = filter.toLongs();
        for (Long filterLong: filterLongs){
            writeLong(filterLong);
        }
    }

    /**
     * Reads a filter from the current position with specified number of longs and hasher factory
     * function.
     * @param numLongs num of longs
     * @param filterFactory a factory to create a filter given a long array
     * @return a bloom filter associated with a string hasher
     * @throws EOFException if the file reaches the end in the process of reading the filter
     * @throws IOException if an I/O error happens
     */
    public Filter readFilter(int numLongs, Function<long[], Filter> filterFactory)
            throws EOFException, IOException {
        long[] filterWords = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            filterWords[i] = readLong();
        }
        return filterFactory.apply(filterWords);
    }

    /**
     * Reads offsets from the file. The file pointer will be pushed forward.
     * @param size number of offsets
     * @return offsets
     * @throws IOException if I/O error happens or EOF
     */
    public int[] readOffsets(int size) throws IOException {
        int[] offsets = new int[size];
        for (int i = 0; i < size; ++i) {
            offsets[i] = readInt();
        }
        return offsets;
    }

    /**
     * Write offsets to the file.
     * @param offsets
     * @throws IOException
     */
    public void writeOffsets(int[] offsets) throws IOException {
        for (int off: offsets) {
            writeInt(off);
        }
    }
}
