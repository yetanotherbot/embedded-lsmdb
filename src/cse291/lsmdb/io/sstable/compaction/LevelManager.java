package cse291.lsmdb.io.sstable.compaction;

import cse291.lsmdb.io.sstable.blocks.*;
import cse291.lsmdb.utils.Modification;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by musteryu on 2017/6/4.
 */
public class LevelManager {
    private final Descriptor desc;
    private final int level;
    private final String column;
    private ReentrantReadWriteLock lock;
    private AtomicBoolean shouldWait;

    public LevelManager(Descriptor desc, String column, int level) {
        this.desc = desc;
        this.level = level;
        this.column = column;
        this.lock = new ReentrantReadWriteLock(true);
        this.shouldWait = new AtomicBoolean(false);
    }

    private IndexBlock getIndexBlock() {
        return new IndexBlock(desc, column, level);
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
                DataBlock dataBlock = new DataBlock(desc, column, level, index);
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
        try {
            lock.readLock().lock();
            String[] filenames = desc.getDir().list((dir, name) -> DataBlock.isDataBlock(name));
            DataBlock[] blocks = new DataBlock[filenames.length];
            for (int i = 0; i < filenames.length; i++) {
                blocks[i] = DataBlock.fromFileName(desc, column, filenames[i]).get();
            }
            Arrays.sort(blocks);
            return blocks;
        } finally {
            lock.readLock().unlock();
        }
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

    public void renameAndGC() {
        //TODO rename and gc
    }

    public ArrayList<Map<String, Modification>> compact(Map<String, Modification> toCompact) {
        return null;
    }

    public Descriptor getDesc() {
        return desc;
    }

    public int getLevel() {
        return level;
    }
}
