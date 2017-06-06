package cse291.lsmdb.io.sstable;


import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.io.sstable.blocks.IndexBlockLoader;
import cse291.lsmdb.io.sstable.compaction.LevelManager;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Created by musteryu on 2017/5/30.
 */
public class SSTable {
    private final Descriptor desc;
    private final String column;
    private final LevelManager[] levelManagers;
    private final MemTable mt;

    public SSTable(Descriptor desc, String column, int level) {
        this.desc = desc;
        this.column = column;
        levelManagers = new LevelManager[level];
        for (int i = 1; i < level; i++) {
            levelManagers[i] = new LevelManager(desc, column, i);
        }
        mt = new MemTable(desc, column, MemTable.DEFAULT_BYTES_LIMIT);
    }

    public Optional<String> get(String row) {
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

    public boolean put(String row, String val) {
        if (val.length() == 0) return false;
        if (row.length() == 0) return false;
        try {
            mt.put(row, val);
        } catch (MemTable.MemTableFull full) {

        }
        return true;
    }
}
