package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;

import java.util.Map;

/**
 * Created by musteryu on 2017/6/4.
 */
public class DataBlockDumper {
    private final Descriptor desc;
    public DataBlockDumper(Descriptor desc) {
        this.desc = desc;
    }
    public void dumpTo(int level, int index, Map<RowCol, Modification> modification) {
        //TODO
    }

    public void dumpTo(int level, int index, Map<RowCol, Modification> m1, Map<RowCol, Modification> m2) {
        //TODO
    }
}
