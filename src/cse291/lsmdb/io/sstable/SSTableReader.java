package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.io.sstable.blocks.FilterBlockLoader;
import cse291.lsmdb.io.sstable.blocks.IndexBlock;
import cse291.lsmdb.io.sstable.blocks.MemSSTableBlock;
import cse291.lsmdb.utils.RowCol;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/5/30.
 */
public class SSTableReader {
    public static final int MAX_NUM_LEVEL = 3;
    private File dir;
    private MemSSTableBlock level0DataLoader;
    private StringHasher hasher;

    public SSTableReader(File base, String namespace, String columnFamily, StringHasher hasher) throws IOException {
        dir = new File(base, namespace);
        if (!dir.isDirectory()) {
            throw new IOException("could not read SSTable from directory");
        }
        this.hasher = hasher;
    }

    /**
     * Gets the value level by level
     * @param row selected row
     * @param col selected col
     * @return value
     * @throws IOException I/O error on loaders
     * @throws NoSuchElementException if value is not found
     */
    public String get(String row, String col) throws IOException, NoSuchElementException {
        if (this.level0DataLoader.isRemoved(row, col))
            throw new NoSuchElementException("could not find such element");
        for (int i = 1; i < MAX_NUM_LEVEL; i++) {
            try {
                return getByLevel(row, col, i);
            } catch (NoSuchElementException nsee) { }
        }
        throw new NoSuchElementException("could not find such element");
    }

    /**
     * Gets the value of specified level.
     * @param row selected row
     * @param col selected col
     * @param level select level
     * @return value
     * @throws IOException I/O error on loaders
     * @throws NoSuchElementException if value is not found
     */
    public String getByLevel(String row, String col, int level) throws IOException, NoSuchElementException {
//        if (level < 0 || level >= MAX_NUM_LEVEL) {
//            throw new IllegalArgumentException("the level out of range");
//        }
//
//        if (level == 0) {
//            return this.level0DataLoader.get(row, col).get();
//        }
//        // String[] children = dir.list((d, name) -> name.startsWith(level + "_"));
//
//        File filterFile = new File(dir, level + "_Filter.db");
//        FilterBlockLoader filterBlock = new FilterBlockLoader(filterFile, hasher);
//        filterBlock.loadInMemory();
//        if (!filterBlock.isPresent(row)) {
//            throw new NoSuchElementException(String.format(
//                    "filter reject: row is not in the current level: row %s, level %d", row, level
//            ));
//        }
//
//        File indexFile = new File(dir, level + "_Index.db");
//        IndexBlock indexBlock = new IndexBlock(indexFile);
//        indexBlock.loadInMemory();
//        int blockIndex = indexBlock.lookup(new RowCol(row, col));
//        if (blockIndex < 0) {
//            throw new NoSuchElementException(String.format(
//                    "index reject: row column pair is not in the current level: row %s, col %s, level %d",
//                    row, col, level
//            ));
//        }
//
//        File dataFile = new File(dir, String.format("%d_%d_Data.db", level, blockIndex));
//        DataLoader dataLoader = new DataLoader(dataFile);
//        return dataLoader.get(row, col).get();
        return null;
    }
}
