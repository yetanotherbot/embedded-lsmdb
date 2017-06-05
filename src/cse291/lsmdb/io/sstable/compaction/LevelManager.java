package cse291.lsmdb.io.sstable.compaction;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.io.sstable.blocks.*;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by musteryu on 2017/6/4.
 */
public class LevelManager {
    private final Descriptor desc;
    private final int level;
    private ReentrantReadWriteLock lock;

    public LevelManager(Descriptor desc, int level) {
        this.desc = desc;
        this.level = level;
        this.lock = new ReentrantReadWriteLock(true);
    }

    public FilterBlock getFilterBlock() {
        try {
            lock.readLock().lock();
            return new FilterBlock(desc, level);
        } finally {
            lock.readLock().unlock();
        }
    }

    public FilterBlockLoader getFilterBlockLoader(StringHasher hasher) {
        try {
            lock.readLock().lock();
            return new FilterBlockLoader(getFilterBlock(), hasher);
        } finally {
            lock.readLock().unlock();
        }
    }

    public IndexBlock getIndexBlock() {
        try {
            lock.readLock().lock();
            return new IndexBlock(desc, level);
        } finally {
            lock.readLock().unlock();
        }
    }

    public IndexBlockLoader getIndexBlockLoader() {
        try {
            lock.readLock().lock();
            return new IndexBlockLoader(getIndexBlock());
        } finally {
            lock.readLock().unlock();
        }
    }

    public DataBlock[] getDataBlocks() {
        try {
            lock.readLock().lock();
            String[] filenames = desc.getDir().list((dir, name) -> DataBlock.isDataBlock(name));
            DataBlock[] blocks = new DataBlock[filenames.length];
            for (int i = 0; i < filenames.length; i++) {
                blocks[i] = DataBlock.fromFileName(desc, filenames[i]).get();
            }
            Arrays.sort(blocks);
            return blocks;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void freeze() {
        lock.readLock().lock();
    }

    public void unfreeze() {
        lock.readLock().unlock();
    }

    public ArrayList<Map<RowCol, Modification>> compact(Map<RowCol, Modification> toCompact) {
        return null;
    }

    public Descriptor getDesc() {
        return desc;
    }

    public int getLevel() {
        return level;
    }
}
