package cse291.lsmdb.io.sstable.compaction;


import cse291.lsmdb.io.sstable.SSTableConfig;
import cse291.lsmdb.io.sstable.blocks.*;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private AtomicBoolean shouldWait;
    private ExecutorService threadPool;

    public LevelManager(Descriptor desc, String column, int level, SSTableConfig config) {
        this.desc = desc;
        this.level = level;
        this.config = config;
        this.levelBlocksLimit = config.getBlocksNumLimitForLevel().apply(level);
        this.column = column;
        this.lock = new ReentrantReadWriteLock(true);
        this.shouldWait = new AtomicBoolean(false);
        this.threadPool = Executors.newCachedThreadPool();
    }

    private IndexBlock getIndexBlock() {
        return new IndexBlock(desc, column, level, config);
    }

    private IndexBlockLoader getIndexBlockLoader() {
        return new IndexBlockLoader(getIndexBlock());
    }

    public Optional<String> get(String row) throws InterruptedException {
        try {
            while (shouldWait.get()) {
                wait();
            }
            lock.readLock().lock();
            int index = getIndexBlockLoader().lookup(row);
            if (index != -1) {
                DataBlock dataBlock = new DataBlock(desc, column, level, index, config);
                DataBlockLoader dataBlockLoader = new DataBlockLoader(dataBlock);
                Modification mod = dataBlockLoader.get(row);
                if (mod.isPut()) {
                    return Optional.of(mod.getIfPresent().get());
                }
            }
            return Optional.empty();
        } catch (NoSuchElementException e) {
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    private DataBlock[] getDataBlocks() {
        String[] filenames = desc.getDir().list((dir, name) -> DataBlock.isDataBlock(name, config));
        DataBlock[] blocks = new DataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            blocks[i] = DataBlock.fromFileName(desc, column, filenames[i], config).get();
        }
        Arrays.sort(blocks);
        return blocks;
    }

    private TempDataBlock[] getTempDataBlocks() {
        String[] filenames = desc.getDir().list(
                (dir, name) -> TempDataBlock.isTempDataBlock(name, config)
        );
        TempDataBlock[] tmpBlocks = new TempDataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            tmpBlocks[i] = TempDataBlock.fromFileName(desc, column, filenames[i], config).get();
        }
        Arrays.sort(tmpBlocks);
        return tmpBlocks;
    }

    public void freeze() {
        shouldWait.set(true);
        lock.writeLock().lock();
    }

    public void unfreeze() {
        shouldWait.set(false);
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
     *
     * TempDataBlock:
     * 1_2_Data.db.tmp_0,
     * 1_2_Data.db.tmp_1,
     * 1_2_Data.db.tmp_2,
     * 1_4_Data.db.tmp0
     *
     * The method will merge them and rename them like:
     * 1_0_Data.db,             -> 1_0_Data.db
     * 1_1_Data.db,             -> 1_1_Data.db
     * 1_2_Data.db.tmp_0,       -> 1_2_Data.db
     * 1_2_Data.db.tmp_1,       -> 1_3_Data.db
     * 1_2_Data.db.tmp_2,       -> 1_4_Data.db
     * 1_3_Data.db,             -> 1_5_Data.db
     * 1_4_Data.db.tmp0,        -> 1_6_Data.db
     * 1_5_Data.db              -> 1_7_Data.db
     *
     * This method requires the compact() method to remove original data file as well as
     * to figure out the data to push downward without creating temporary files.
     * @throws IOException
     */
    public void rename() throws IOException {
        try {
            lock.writeLock().lock();
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
                files.add(dbs[di].getFile());
            }
            while (ti < tbs.length) {
                files.add(tbs[ti].getFile());
            }
            for (int i = 0; i < files.size(); i++) {
                File dst = new DataBlock(desc, column, level, i, config).getFile();
                if (!files.get(i).renameTo(dst)) {
                    throw new IOException("failed to rename file");
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compacts a block into this level. In case that the total size exceeds the limit,
     * return the block needs to push down.
     * Note that config should be used.
     * @param block the block to compact in this level
     * @return the block to compact to next level. null if not exis
     * @throws IOException
     */
    public Modifications compact(Modifications block) throws IOException {
        int blockBytesLimit = config.getBlockBytesLimit();
        return null;
    }


    public Descriptor getDesc() {
        return desc;
    }

    public int getLevel() {
        return level;
    }
}
