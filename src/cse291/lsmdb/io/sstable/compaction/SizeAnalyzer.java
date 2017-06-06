package cse291.lsmdb.io.sstable.compaction;

import cse291.lsmdb.utils.Modification;

import java.util.Map;

/**
 * Created by musteryu on 2017/6/6.
 */
public class SizeAnalyzer {
    private int size = 0;

    public static int analyze(Map<String, Modification> mods) {
        int size = 0;
        for (String key: mods.keySet()) {
            size += key.getBytes().length;
            Modification mod = mods.get(key);
            if (mod.isPut()) {
                size += mod.getIfPresent().get().getBytes().length;
            }
            size += Long.BYTES;
        }
        return size;
    }

    public void reset() { size = 0; }

    public int add(Map.Entry<String, Modification> e) {
        size += e.getKey().getBytes().length;
        Modification mod = e.getValue();
        if (mod.isPut()) {
            size += mod.getIfPresent().get().getBytes().length;
        }
        size += Long.BYTES;
        return size;
    }

    public int addMap(Map<String, Modification> mods) {
        size += analyze(mods);
        return size;
    }
}
