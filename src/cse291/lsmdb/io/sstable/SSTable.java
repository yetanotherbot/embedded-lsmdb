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
    private final LevelManager[] levelManagers;
    private final MemTable mt;

    public SSTable(Descriptor desc, String column, SSTableConfig config) {
        this.desc = desc;
        this.column = column;
        levelManagers = new LevelManager[config.getOnDiskLevelsLimit()];
        for (int i = 1; i < config.getOnDiskLevelsLimit(); i++) {
            levelManagers[i] = new LevelManager(desc, column, i, config);
        }
        mt = new MemTable(desc, column, config);
    }

    public Optional<String> get(String row) throws InterruptedException {
        try {
            String v = mt.get(row);
            return Optional.of(v);
        } catch (NoSuchElementException nsee) {
            for (int i = 1; i < levelManagers.length; i++) {
                Optional<String> res = levelManagers[i].get(row);
                if (res.isPresent()) return res;
            }
        }
        return Optional.empty();
    }

    public boolean put(String row, String val) throws IOException {
        if (row.length() == 0) return false;
        try {
            if(val != null) {
                mt.put(row, val);
            } else {
                mt.remove(row);
            }
        } catch (MemTable.MemTableFull full) {
            Modifications mods = mt.stealModifications();
            for (LevelManager levelManager: levelManagers) {
                levelManager.freeze();
                mods = levelManager.compact(mods);
                levelManager.rename();
                levelManager.unfreeze();
            }
            if (mods != null) throw new RuntimeException("out of storage");
        }
        return true;
    }
}
