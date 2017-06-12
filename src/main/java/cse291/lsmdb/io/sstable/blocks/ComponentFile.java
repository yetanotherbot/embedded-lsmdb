package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.io.interfaces.Filter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by musteryu on 2017/6/3.
 */
public class ComponentFile {
    private Optional<BufferedInputStream> reader;
    private Optional<BufferedOutputStream> writer;

    public ComponentFile(File f, String mode, int size) throws IOException {
        if (mode == "r") {
            reader = Optional.of(new BufferedInputStream(new FileInputStream(f), size));
            writer = Optional.empty();
        } else {
            writer = Optional.of(new BufferedOutputStream(new FileOutputStream(f), size));
            reader = Optional.empty();
        }
    }

    public ComponentFile(File f, int size) throws IOException {
        this(f, "r", size);
    }

    public ComponentFile(File f, String mode, int size, CompressorStreamProvider compressor, String compressType)
            throws IOException, CompressorException {
        if (compressor != null) {
            if (mode == "r") {
                reader = Optional.of(new BufferedInputStream(
                        compressor.createCompressorInputStream(
                                compressType,
                                new FileInputStream(f),
                                true
                        ),
                        size
                ));
                writer = Optional.empty();
            } else {
                writer = Optional.of(new BufferedOutputStream(
                        compressor.createCompressorOutputStream(
                                compressType, new FileOutputStream(f)
                        ),
                        size
                ));
                reader = Optional.empty();
            }
        } else {
            if (mode == "r") {
                reader = Optional.of(new BufferedInputStream(new FileInputStream(f), size));
                writer = Optional.empty();
            } else {
                writer = Optional.of(new BufferedOutputStream(new FileOutputStream(f), size));
                reader = Optional.empty();
            }
        }
    }

    public ComponentFile(
            File f, int size, CompressorStreamProvider compressor, String compressType
    ) throws IOException, CompressorException {
        this(f, "r", size, compressor, compressType);
    }

    public static boolean tryClose(ComponentFile c) {
        if (c != null) return c.tryClose();
        return true;
    }

    public boolean tryClose() {
        try {
            if (reader.isPresent()) reader.get().close();
            if (writer.isPresent()) writer.get().close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void checkWritable() {
        if (!writer.isPresent()) {
            System.err.println("it's not a writable component");
            throw new RuntimeException("it's not a writable component");
        }
    }

    /**
     * Write the BloomFilter of the SSTableBlock as 128 bit longs
     *
     * @param filter the BloomFilter of the data
     * @throws IOException if an I/O error happens
     */
    public void writeFilter(Filter filter) throws IOException {
        checkWritable();
        BufferedOutputStream w = writer.get();
        long[] filterLongs = filter.toLongs();
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * filterLongs.length);
        for (long filterLong : filterLongs) {
            buf.putLong(filterLong);
        }
        buf.flip();
        w.write(buf.array());
    }

    private void checkReadable() {
        if (!reader.isPresent()) {
            System.err.println("it's not a readable component");
            throw new RuntimeException("it's not a readable component");
        }
    }

    /**
     * Reads a filter from the current position with specified number of longs and hasher factory
     * function.
     *
     * @param numLongs      num of longs
     * @param filterFactory a factory to create a filter given a long array
     * @return a bloom filter associated with a string hasher
     * @throws EOFException if the file reaches the end in the process of reading the filter
     * @throws IOException  if an I/O error happens
     */
    public Filter readFilter(int numLongs, Function<long[], Filter> filterFactory)
            throws EOFException, IOException {
        checkReadable();
        BufferedInputStream r = reader.get();
        long[] filterLongs = new long[numLongs];
        byte[] array = new byte[Long.BYTES * numLongs];
        r.read(array);

        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * numLongs);
        buf.put(array);
        buf.flip();
        for (int i = 0; i < numLongs; i++) {
            filterLongs[i] = buf.getLong();
        }
        return filterFactory.apply(filterLongs);
    }

    public void writeString(String s) throws IOException {
        checkWritable();
        byte[] bytes = s.getBytes();
        BufferedOutputStream w = writer.get();
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + bytes.length);
        buf.putInt(bytes.length);
        buf.put(bytes);
        buf.flip();
        w.write(buf.array());
    }

    public String readString() throws IOException {
        checkReadable();
        BufferedInputStream r = reader.get();
        byte[] ibs = new byte[Integer.BYTES];

        // read the first 4 bytes to know the length of following bytes
        r.read(ibs);
        ByteBuffer ibuf = ByteBuffer.allocate(Integer.BYTES);
        ibuf.put(ibs).flip();
        int len = ibuf.getInt();
//        System.out.println("len is: " + len);

        // read the following bytes
        byte[] bytes = new byte[len];
        r.read(bytes);
//        System.out.println("string is: " + new String(bytes));

        return new String(bytes);
    }

    public void writeLong(long l) throws IOException {
        checkWritable();
        BufferedOutputStream w = writer.get();
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(l);
        buf.flip();
        w.write(buf.array());
    }

    public long readLong() throws IOException {
        checkReadable();
        BufferedInputStream r = reader.get();
        byte[] lbs = new byte[Long.BYTES];
        r.read(lbs);
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.put(lbs).flip();
        return buf.getLong();
    }

    public boolean eof() throws IOException {
        checkReadable();
        BufferedInputStream r = reader.get();
        return r.available() <= 0;
    }

//    /**
//     * Reads offsets from the file. The file pointer will be pushed forward.
//     * @param size number of offsets
//     * @return offsets
//     * @throws IOException if I/O error happens or EOF
//     */
//    public int[] readOffsets(int size) throws IOException {
//        int[] offsets = new int[size];
//        for (int i = 0; i < size; ++i) {
//            offsets[i] = readInt();
//        }
//        return offsets;
//    }

//    /**
//     * Write offsets to the file.
//     * @param offsets
//     * @throws IOException
//     */
//    public void writeOffsets(int[] offsets) throws IOException {
//        for (int off: offsets) {
//            writeInt(off);
//        }
//    }
}
