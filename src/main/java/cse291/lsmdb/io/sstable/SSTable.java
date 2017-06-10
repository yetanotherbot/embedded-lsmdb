package cse291.lsmdb.io.sstable;


import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.io.sstable.compaction.LevelManager;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Qualifier;
import cse291.lsmdb.utils.Timed;

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

    /**
     * Go through all the MemTables and Levels of datablocks to find entries meet the qualifier
     * @param q
     * @return Map of rowKey and columnValue
     */
    public Map<String,String> getColumnWithQualifier(Qualifier q) throws IOException{
        Map<String,Timed<String>> result = new HashMap<>();
        for(int i =  0; i < config.getOnDiskLevelsLimit(); i++){
            result = this.mergeEntryMaps(result, this.levelManagers[i].getColumnWithQualifier(q));
        }
        for(int i = 0; i < config.getMemTablesLimit(); i++){
            result = this.mergeEntryMaps(result, this.memTables.get(i).getColumnWithQualifier(q));
        }

        Map<String,String> toReturn = new HashMap<>();
        for (Map.Entry<String, Timed<String>> entry : result.entrySet())
        {
            String value = entry.getValue().get();
            if(value.length() > 0){
                toReturn.put(entry.getKey(),value);
            }
        }
        return toReturn;
    }

    private Map<String,Timed<String>> mergeEntryMaps(Map<String,Timed<String>> m1, Map<String,Timed<String>> m2){
        Map<String,Timed<String>> mergedMap = new HashMap<>();
        for (Map.Entry<String, Timed<String>> entry : m1.entrySet())
        {
            String rowKey = entry.getKey();
            Timed<String> timedValue = entry.getValue();
            if(!mergedMap.containsKey(rowKey) || mergedMap.get(rowKey).getTimestamp() < timedValue.getTimestamp()) {
                mergedMap.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Timed<String>> entry : m2.entrySet())
        {
            String rowKey = entry.getKey();
            Timed<String> timedValue = entry.getValue();
            if(!mergedMap.containsKey(rowKey) || mergedMap.get(rowKey).getTimestamp() < timedValue.getTimestamp()) {
                mergedMap.put(entry.getKey(), entry.getValue());
            }
        }
        return mergedMap;
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
