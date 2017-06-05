package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Pair;
import cse291.lsmdb.utils.RowCol;
import cse291.lsmdb.utils.Timed;

import java.io.*;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/3.
 */
public class ComponentFile extends RandomAccessFile {

    public ComponentFile(File f) throws FileNotFoundException {
        super(f, "r");
    }

    public enum VarLengthType {
        VAR_LENGTH_8,
        VAR_LENGTH_16,
        VAR_LENGTH_32
    }

    public byte[] readVarLength(VarLengthType varLengthType) throws IOException{
        switch (varLengthType) {
            case VAR_LENGTH_8: {
                byte len = this.readByte();
                byte[] bytes = new byte[len];
                this.read(bytes);
                return bytes;
            }
            case VAR_LENGTH_16: {
                char len = this.readChar();
                byte[] bytes = new byte[len];
                this.read(bytes);
                return bytes;
            }
            case VAR_LENGTH_32: default: {
                int len = this.readInt();
                byte[] bytes = new byte[len];
                this.read(bytes);
                return bytes;
            }
        }
    }

    public void writeVarLength(VarLengthType varLengthType, int value) throws IOException{
        switch (varLengthType) {
            case VAR_LENGTH_8: {
                writeByte(value);
            }
            case VAR_LENGTH_16: {
                writeChar(value);
            }
            case VAR_LENGTH_32: default: {
                writeInt(value);
            }
        }
    }
    
    public RowCol readRowNameColName() throws IOException {
        byte[] rowBytes = readVarLength(VarLengthType.VAR_LENGTH_16);
        String row = new String(rowBytes);

        byte[] colBytes = readVarLength(VarLengthType.VAR_LENGTH_16);
        String col = new String(colBytes);

        return new RowCol(row, col);
    }

    /**
     * Write the name of a RowCol pair
     * @param rowCol the RowCol pair to write
     * @throws IOException if an I/O error happens
     */
    public void writeRowNameColName(RowCol rowCol) throws IOException{
        String rowName = rowCol.left;
        int rowNameLength = rowName.getBytes().length;
        String colName = rowCol.right;
        int colNameLength = colName.getBytes().length;

        writeChar(rowNameLength);
        write(rowName.getBytes());
        writeChar(colNameLength);
        write(colName.getBytes());
    }

    /**
     * Write the BloomFilter of the SSTableBlock as 128 bit longs
     * @param filter the BloomFilter of the data
     * @throws IOException if an I/O error happens
     */
    public void writeFilter(Filter filter) throws IOException{
        long[] filterLongs = ((BloomFilter)filter).toLongs();
        for (Long filterLong: filterLongs){
            writeLong(filterLong);
        }
    }

    /**
     * Write a column-value pair from the current position. The method reads a column, its value
     * and timestamp from the current position and shift the file pointer to the starting position
     * of next column. The format of a column in the data file is:
     * Row Length              | Row Data  | Data Length             | Data      | Timestamp
     * unsigned 16 bits (char) | varlength | unsigned 16 bits (char) | varlength | long
     * @param pair a columnName:Modification pair with timeStamp to be written
     * @throws IOException if an I/O error happens
     */
    public void writeColumnModification(Pair<String, Modification> pair) throws IOException{
        // Write the columnName length and columnName
        String columnName = pair.left;
        int columnNameLength = columnName.getBytes().length;

        writeChar(columnNameLength);
        write(columnName.getBytes());

        // Write the columnValue length and column value
        // If it is a remove, the length is Short.MaxValue and no value will be written
        Modification mod = pair.right;
        if(mod.isRemove()){
            writeChar(Character.MAX_VALUE);
        } else {
            String columnValue = mod.getIfPresent().get();
            int columnValueLength = columnValue.getBytes().length;

            writeChar(columnValueLength);
            write(columnValue.getBytes());
        }

        // Write the timestamp
        long timeStamp = pair.right.getTimestamp();
        writeLong(timeStamp);
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
     * Reads a filter from the current position with specified number of longs and hasher factory
     * function.
     * @param numLongs num of longs
     * @param hasher a StringHasher
     * @return a bloom filter associated with a string hasher
     * @throws EOFException if the file reaches the end in the process of reading the filter
     * @throws IOException if an I/O error happens
     */
    public Filter readFilter(int numLongs, StringHasher hasher)
            throws EOFException, IOException {
        long[] filterWords = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            filterWords[i] = readLong();
        }
        return new BloomFilter(filterWords, hasher);
    }

    /**
     * Reads a column-value pair from the current position. The method reads a column, its value
     * and timestamp from the current position and shift the file pointer to the starting position
     * of next column of next row. The format of a column in the data file is:
     * Col Name Length         | Col Name  | Data Length             | Data      | Timestamp
     * unsigned 16 bits (char) | varlength | unsigned 16 bits (char) | varlength | long
     * @return the next column-value pair of the file
     * @throws EOFException if the file reaches the end in the process of reading the whole column
     * @throws IOException if an I/O error happens
     */
    public Pair<String, Modification> readColumnModification() throws EOFException, IOException {
        // build a column name
        char colLen = readChar();
        byte[] colBytes = new byte[colLen];
        read(colBytes, 0, colLen);
        String col = new String(colBytes);

        // build a column data
        Modification mod = null;

        char dataLen = readChar();
        if (dataLen == Character.MAX_VALUE) {
            long timestamp = readLong();
            mod = Modification.remove(timestamp);
        } else {
            byte[] dataBytes = new byte[dataLen];
            read(dataBytes, 0, dataLen);
            String data = new String(dataBytes);

            // build a timestamp
            long timestamp = readLong();
            mod = Modification.put(new Timed<>(data, timestamp));
        }

        return new Pair<>(col, mod);
    }

    public Modification binarySearch(String col, int[] index)
            throws NoSuchElementException, EOFException, IOException {
        int lo = 0, hi = index.length;
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            int idx = index[mid];
            seek(idx);
            Pair<String, Modification> midColPair = readColumnModification();
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
}
