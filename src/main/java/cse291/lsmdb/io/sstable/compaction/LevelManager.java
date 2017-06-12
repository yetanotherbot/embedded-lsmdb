package cse291.lsmdb.io.sstable.compaction;


import cse291.lsmdb.io.interfaces.Filter;
import cse291.lsmdb.io.interfaces.WritableFilter;
import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.blocks.*;
import cse291.lsmdb.io.sstable.filters.BloomFilter;
import cse291.lsmdb.utils.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by musteryu on 2017/6/4.
 */
public class LevelManager {
    private final Descriptor desc;
    private final int level;
    private final int levelBlocksLimit;
    private final SSTableConfig config;
    private final String column;
    private ReentrantReadWriteLock lock;
    private volatile boolean shouldWait;
    // TODO: 2017/6/9 parallelize compactions in the future

    public LevelManager(Descriptor desc, String column, int level, SSTableConfig config) {
        this.desc = desc;
        this.level = level;
        this.config = config;
        this.levelBlocksLimit = config.getBlocksNumLimitForLevel().apply(level);
        this.column = column;
        this.lock = new ReentrantReadWriteLock(true);
        this.shouldWait = false;
    }

    private IndexBlock getIndexBlock() {
        return new IndexBlock(desc, column, level, config);
    }

    private IndexBlockLoader getIndexBlockLoader() {
        return new IndexBlockLoader(getIndexBlock());
    }

    public Optional<String> get(String row) throws InterruptedException, NoSuchElementException {
        synchronized (this) {
            while (shouldWait) {
                this.wait();
            }
        }
        int index = getIndexBlockLoader().lookup(row);
        if (index != -1) {
            DataBlock dataBlock = new DataBlock(desc, column, level, index, config);
            DataBlockLoader dataBlockLoader = new DataBlockLoader(
                    dataBlock, config.getPerBlockBloomFilterBits(), config.getHasher());
            Modification mod = dataBlockLoader.get(row);
            if (mod.isPut()) {
                return Optional.of(mod.getIfPresent().get());
            } else {
                return Optional.empty();
            }
        } else throw new NoSuchElementException("could not find such element");
    }

    public Map<String, Timed<String>> getColumnWithQualifier(Qualifier q) throws IOException, InterruptedException {
        synchronized (this) {
            while (shouldWait) this.wait();
        }
        Map<String, Timed<String>> columns = new HashMap<>();
        DataBlock[] dataBlocks = this.getDataBlocks();
        for (DataBlock db : dataBlocks) {
            DataBlockLoader dbLoader = new DataBlockLoader(
                    db, config.getPerBlockBloomFilterBits(), config.getHasher()); //Useless basically
            columns.putAll(dbLoader.getColumnWithQualifier(q));
        }
        return columns;
    }

    private File getColumnDir() {
        File dir = getDesc().getDir();
        return new File(dir, column);
    }

    private DataBlock[] getDataBlocks() {
        String[] filenames = getColumnDir().list((dir, name) -> DataBlock.isDataBlockForLevel(name, config, level));
        DataBlock[] blocks = new DataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            blocks[i] = DataBlock.fromFileName(desc, column, filenames[i], config).get();
        }
        Arrays.sort(blocks);
        return blocks;
    }

    private TempDataBlock[] getTempDataBlocks() {
        String[] filenames = getColumnDir().list(
                (dir, name) -> TempDataBlock.isTempDataBlock(name, config)
        );
        if (filenames == null) {
            System.err.println("filenames array is null");
            System.exit(-1);
        }
        TempDataBlock[] tmpBlocks = new TempDataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            tmpBlocks[i] = TempDataBlock.fromFileName(desc, column, filenames[i], config).get();
        }
        Arrays.sort(tmpBlocks);
        return tmpBlocks;
    }

    public synchronized void freeze() {
        shouldWait = true;
        lock.writeLock().lock();
    }

    public synchronized void unfreeze() {
        shouldWait = false;
        notifyAll();
        lock.writeLock().unlock();
    }

    /**
     * Renames files. For example:
     * DataBlock:
     * 1_0_Data.db,
     * 1_1_Data.db,
     * 1_3_Data.db,
     * 1_5_Data.db
     * <p>
     * TempDataBlock:
     * 1_2_Data.db.tmp_0,
     * 1_2_Data.db.tmp_1,
     * 1_2_Data.db.tmp_2,
     * 1_4_Data.db.tmp0
     * <p>
     * The method will merge them and collect them like:
     * 1_0_Data.db,             -> 1_0_Data.db
     * 1_1_Data.db,             -> 1_1_Data.db
     * 1_2_Data.db.tmp_0,       -> 1_2_Data.db
     * 1_2_Data.db.tmp_1,       -> 1_3_Data.db
     * 1_2_Data.db.tmp_2,       -> 1_4_Data.db
     * 1_3_Data.db,             -> 1_5_Data.db
     * 1_4_Data.db.tmp_0,       -> 1_6_Data.db
     * 1_5_Data.db              -> 1_7_Data.db
     * <p>
     * This method requires the compact() method to remove original data file.
     *
     * @throws IOException
     */
    private Modifications collect() throws IOException {
        Modifications forNext = null;

        //requires the compact() method to remove original data files that are compacted
        ArrayList<File> files = mergeTempAndDataBlocks();
        for (int i = files.size() - 1; i >= 0; i--) { // must from higher index to lower
            relinkAsDataBlock(files.get(i), i);
        }
        if (files.size() > levelBlocksLimit) {
            truncateIndexUpTo(levelBlocksLimit);
            forNext = new Modifications(config.getBlockBytesLimit());
            for (int start = levelBlocksLimit; start < files.size(); start++) {
                DataBlock b = new DataBlock(desc, column, level, start, config);
                forNext.putAll(load(b));
                Files.delete(b.getFile().toPath());
            }
        }

        return forNext; //TODO: return blocks to next level
    }

    private void truncateIndexUpTo(int upTo) throws IOException {
        ArrayList<Pair<String, String>> ranges = getRanges();
        dumpIndex(ranges.subList(0, upTo));
    }

    private void truncateIndexRange(int start, int end) throws IOException {
        ArrayList<Pair<String, String>> ranges = getRanges();
        ArrayList<Pair<String, String>> newRanges = new ArrayList<>(ranges.subList(0, start));
        newRanges.addAll(ranges.subList(end, ranges.size()));
        dumpIndex(newRanges);
    }

    private ArrayList<File> mergeTempAndDataBlocks() throws IOException {
        DataBlock[] dbs = getDataBlocks();
        TempDataBlock[] tbs = getTempDataBlocks();
        int di = 0, ti = 0;
        ArrayList<File> files = new ArrayList<>();
        while (di < dbs.length && ti < tbs.length) {
            if (dbs[di].getIndex() < tbs[ti].getOriginIndex()) {
                files.add(dbs[di++].getFile());
            } else {
                files.add(tbs[ti++].getFile());
            }
        }
        while (di < dbs.length) {
            files.add(dbs[di++].getFile());
        }
        while (ti < tbs.length) {
            files.add(tbs[ti++].getFile());
        }
        System.out.println("to merge:");
        for (File f : files) {
            System.out.println(f.getName());
        }
        System.out.println("--------------");
        return files;
    }

    private void relinkAsDataBlock(File file, int index) throws IOException {
        File dst = new DataBlock(desc, column, level, index, config).getFile();
        if (file.getName().equals(dst.getName())) return;
        System.out.printf("link %s to %s\n", dst.getName(), file.getName());
        Files.createLink(dst.toPath(), file.toPath());
        Files.delete(file.toPath());
    }

    /**
     * Writes data to TempDataBlocks starting from original index.
     *
     * @return ranges for index block
     * @throws IOException
     */
    private List<Pair<String, String>> appendBlocks(Modifications block, int originIndex) throws IOException {
        WritableFilter f = new BloomFilter(config.getPerBlockBloomFilterBits(), config.getHasher());
        Modifications d = new Modifications(config.getBlockBytesLimit());

        List<Pair<String, String>> ranges;
        if (getIndexBlock().getFile().exists()) {
            ranges = new ArrayList<>(getRanges().subList(0, originIndex));
        } else {
            ranges = new ArrayList<>();
            Queue<Integer> q;
        }

        int i = 0;
        for (String row : block.rows()) {
            if (d.existLimit()) {
                ranges.add(new Pair<>(d.firstRow(), d.lastRow()));
                TempDataBlock t = new TempDataBlock(desc, column, level, i, originIndex, config);
                new DataBlockDumper(t, config.getPerBlockBloomFilterBits()).dump(d, f);
                d = new Modifications(config.getBlockBytesLimit());
                i++;
            }
            d.put(row, block.get(row));
            f.add(row);
        }
        // left over
        ranges.add(new Pair<>(d.firstRow(), d.lastRow()));
        TempDataBlock t = new TempDataBlock(desc, column, level, i, originIndex, config);
        new DataBlockDumper(t, config.getPerBlockBloomFilterBits()).dump(d, f);

        return ranges;
    }

    /**
     * Called when the level is empty.
     */
    private void firstBlocks(Modifications block) throws IOException {
        List<Pair<String, String>> ranges = appendBlocks(block, 0);
        dumpIndex(ranges);
    }

    private int locateBlock(String row) throws IOException {
        IndexBlockLoader idxLoader = getIndexBlockLoader();
        ArrayList<Pair<String, String>> ranges = idxLoader.getRanges();
        return locateBlock(row, ranges);
    }

    private int locateBlock(String row, ArrayList<Pair<String, String>> ranges) throws IOException {
        int i = ranges.size() - 1;
        while (i > 0 && ranges.get(i).left.compareTo(row) > 0)
            --i;
        return i;
    }

    private Modifications load(DataBlock b) throws IOException {
        DataBlockLoader loader = new DataBlockLoader(
                b, config.getPerBlockBloomFilterBits(), config.getHasher()
        );
        return loader.extractModifications(config.getBlockBytesLimit());
    }

    private void dumpTemp(Modifications m, int index, int origIndex) throws IOException {
        TempDataBlock t = new TempDataBlock(desc, column, level, index, origIndex, config);
        Filter f = m.calculateFilter(
                new BloomFilter(config.getPerBlockBloomFilterBits(), config.getHasher())
        );
        new DataBlockDumper(t, config.getPerBlockBloomFilterBits()).dump(m, f);
    }

    private void dumpIndex(List<Pair<String, String>> ranges) throws IOException {
        new IndexBlockDumper(getIndexBlock()).dump(ranges);
    }

    private ArrayList<Pair<String, String>> getRanges() throws IOException {
        return getIndexBlockLoader().getRanges();
    }

    private void compactWithExisting(Modifications block) throws IOException {
        ArrayList<Pair<String, String>> ranges = getRanges();
        int fst = locateBlock(block.firstRow(), ranges);
        int snd = locateBlock(block.lastRow(), ranges);
        DataBlock[] dbs = this.getDataBlocks();
        int j = 0;
        ArrayList<Pair<String, String>> newRanges = new ArrayList<>(ranges.subList(0, fst));

        for (int i = fst; i <= snd; ++i) {
            Modifications mod = load(dbs[i]);
            if (block.offer(mod)) {
                Modifications poll = block.poll();
                newRanges.add(new Pair<>(poll.firstRow(), poll.lastRow()));
                dumpTemp(poll, j++, fst);
            }
            Files.delete(dbs[i].getFile().toPath());
        }

        while (!block.isEmpty()) {
            Modifications poll = block.poll();
            newRanges.add(new Pair<>(poll.firstRow(), poll.lastRow()));
            dumpTemp(poll, j++, fst);
        }

        newRanges.addAll(ranges.subList(snd + 1, ranges.size()));
        dumpIndex(newRanges);
    }

    /**
     * Compacts a block into this level. In case that the total numLongs exceeds the limit,
     * return the block needs to push down.
     * Note that config should be used.
     *
     * @param block the block to compact in this level
     * @return the block to compact to next level. null if not exis
     * @throws IOException
     */
    public Modifications compact(Modifications block) throws IOException {
        IndexBlock idx = this.getIndexBlock();
        if (!idx.getFile().exists()) { // the first block get compacted in the level
            firstBlocks(block);
        } else {
            compactWithExisting(block);
        }
        return collect();
    }

    public boolean isEmpty() {
        try{
            return !getIndexBlock().getFile().exists();
        } catch (IOException e) {
            return true;
        }
    }

    public Descriptor getDesc() {
        return desc;
    }

    public int getLevel() {
        return level;
    }
}
