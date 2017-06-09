package cse291.lsmdb.io.sstable;


import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.io.sstable.compaction.LevelManager;
import cse291.lsmdb.utils.Modifications;

import java.io.IOException;
import java.util.*;

/**
 * Created by musteryu on 2017/5/30.
 */
public class SSTable {
    private final Descriptor desc;
    private final String column;
    private SSTableConfig config;
    private final LevelManager[] levelManagers;
    private final LinkedList<MemTable> memTables;
    private final int memTablesLimit;

    public SSTable(Descriptor desc, String column, SSTableConfig config) {
        this.desc = desc;
        this.column = column;
        this.config = config;
        levelManagers = new LevelManager[config.getOnDiskLevelsLimit()];
        for (int i = 1; i < config.getOnDiskLevelsLimit(); i++) {
            levelManagers[i] = new LevelManager(desc, column, i, config);
        }
        memTables = new LinkedList<>();
        memTablesLimit = config.getMemTablesLimit();
        memTables.add(new MemTable(desc, column, config));
    }

    public Optional<String> get(String row) throws InterruptedException {

        Iterator<MemTable> descItr = memTables.descendingIterator();
        while (descItr.hasNext()) {
            try {
                String v = descItr.next().get(row);
                return Optional.of(v);
            } catch (NoSuchElementException e) {
                continue;
            }
        }

        for (int i = 1; i < levelManagers.length; i++) {
            Optional<String> res = levelManagers[i].get(row);
            if (res.isPresent()) return res;
        }

        return Optional.empty();
    }

    public synchronized boolean put(String row, String val) throws IOException {
        if (row.length() == 0) return false;
        try {
            if (val != null) {
                memTables.getLast().put(row, val);
            } else {
                memTables.getLast().remove(row);
            }
        } catch (MemTable.MemTableFull full) {
            if (memTables.size() < memTablesLimit) {
                return memTables.add(new MemTable(desc, column, config));
            }
            Modifications mods = memTables.removeFirst().stealModifications();
            for (LevelManager levelManager: levelManagers) {
                levelManager.freeze();
                mods = levelManager.compact(mods);
                levelManager.unfreeze();
            }
            if (mods != null) {
                throw new RuntimeException("out of storage");
            }
        }
        return true;
    }
}
