package cse291.lsmdb.io.sstable.compaction;

import cse291.lsmdb.io.sstable.blocks.*;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Pair;

import java.io.IOException;
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

    /**
     * Method to split a Map of <String, Modification> to maps which will be merged into individual data blocks
     * @param toCompact a Map of <String, Modification> to be split
     * @return ArrayList of maps to be merged into every data block in this level
     * @throws IOException in case of IO Exception
     */
    private ArrayList<Map<String, Modification>> split_map(Map<String, Modification> toCompact) throws IOException {

        // Create the list of Maps and fill with empty Maps
        ArrayList<Map<String, Modification>> split_maps = new ArrayList<>();
        IndexBlockLoader indexBlockLoader = this.getIndexBlockLoader();
        for(int i = 0; i < indexBlockLoader.getRanges().size(); i++){
            split_maps.add(new TreeMap<>());
        }

        ArrayList<Pair<String, String>> ranges = indexBlockLoader.getRanges(); // Get dataBlock ranges

        // We put all entries has rowKey less than the maximum value of current range to corresponding dataBlock
        // Unless it is already the last block
        int currentRangeIndex = 0;
        String end = ranges.get(currentRangeIndex).right;

        for (Map.Entry<String, Modification> pair : toCompact.entrySet()) {
            String rowName = pair.getKey();
            Modification colValue = pair.getValue();
            if(rowName.compareTo(end) > 0 && currentRangeIndex < ranges.size()-1){
                currentRangeIndex += 1;
                end = ranges.get(currentRangeIndex).right;
            }
            split_maps.get(currentRangeIndex).put(rowName,colValue);
        }

        return split_maps;
    }

    public Descriptor getDesc() {
        return desc;
    }

    public int getLevel() {
        return level;
    }
}
