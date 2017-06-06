package cse291.lsmdb.io.sstable.compaction;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by musteryu on 2017/6/5.
 */
public class Compactor {
    private LevelManager[] levelManagers;
    private Descriptor desc;
    private final String column;
    private ExecutorService threadPool;

    public Compactor(Descriptor desc, String column, int levels) {
        this.column = column;
        levelManagers = new LevelManager[levels];
        for (int i = 0; i < levels; ++i) {
            levelManagers[i] = new LevelManager(desc, column, i);
        }
        this.threadPool = Executors.newCachedThreadPool();
    }

    /**
     * Compacts an modification to destination level
     */
    public ArrayList<Map<RowCol, Modification>> compact(int dst, Map<RowCol, Modification> modifications) {
        return levelManagers[dst].compact(modifications);
    }

    public void compact(Map<RowCol, Modification> level0) {
        boolean done = false;
        int dst = 1;
        ArrayList<Map<RowCol, Modification>> curr = new ArrayList<>();
        curr.add(level0);
        while (!done) {
            for (Map<RowCol, Modification> c: curr) {
                ArrayList<Map<RowCol, Modification>> next = compact(dst, c);
            }
        }
    }

    public void rename(int level) {
        levelManagers[level].freeze();
        //TODO: do rename stuffs
        levelManagers[level].unfreeze();
    }
}
