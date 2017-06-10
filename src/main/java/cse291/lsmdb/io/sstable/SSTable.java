package cse291.lsmdb.io.sstable;


import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.io.sstable.compaction.LevelManager;
import cse291.lsmdb.utils.Modifications;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
            Modifications mods = new Modifications(config.getBlockBytesLimit());
            while (memTables.size() > config.getMemTablesFlushStrategy().apply(memTablesLimit)) {
                mods.putAll(memTables.removeFirst().stealModifications());
            }
            for (int i = 1; i < levelManagers.length; i++) {
                if (mods == null) break;
                LevelManager levelManager = levelManagers[i];
                System.out.printf("compact begin for level %d\n", i);
                levelManager.freeze();
                mods = levelManager.compact(mods);
                levelManager.unfreeze();
                System.out.printf("compact success for level %d\n\n", i);
            }
            if (mods != null) {
                throw new RuntimeException("out of storage");
            }
        }
        return true;
    }

    public static void main(String[] args) {
        SSTable sst = new SSTable(
                new Descriptor("base", "ns", "cf", new String[]{"col"}),
                "col",
                SSTableConfig.defaultConfig()
        );
        try {
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                String row = "test" + RandomStringUtils.randomAlphabetic(50) + RandomUtils.nextInt(100) + RandomStringUtils.randomAlphabetic(2);
                String val = "test" + RandomStringUtils.randomAlphabetic(50) + RandomUtils.nextInt(100) + RandomStringUtils.randomAlphabetic(2);
                if (i % 2 == 0) {
                    sst.put(row, val);
                } else {
                    sst.put(row, null);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
