package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;
import cse291.lsmdb.utils.Timed;

import java.util.*;

/**
 * Created by musteryu on 2017/6/3.
 */
public class MemSSTableBlock extends AbstractSSTableBlock {
    private Descriptor desc;
    private Map<RowCol, Modification> modifications;

    public MemSSTableBlock(
            Descriptor desc,
            Map<RowCol, Timed<String>> puts,
            Map<RowCol, Long> removes
    ) {
        this.desc = desc;
        Set<RowCol> rcs = new TreeSet<>();
        rcs.addAll(puts.keySet());
        rcs.addAll(removes.keySet());
        for (RowCol rc: rcs) {
            Modification putM = Modification.nothing(), removeM = Modification.nothing();
            if (puts.containsKey(rc)) {
                putM = Modification.put(puts.get(rc));
            }
            if (removes.containsKey(rc)) {
                removeM = Modification.remove(removes.get(rc));
            }
            modifications.put(rc, Modification.select(putM, removeM));
        }
    }

    public MemSSTableBlock(Descriptor desc, Map<RowCol, Modification> modifications) {
        this.desc = desc;
        this.modifications = modifications;
    }

    public boolean isRemoved(String row, String col) {
        RowCol rc = new RowCol(row, col);
        return modifications.containsKey(rc) && modifications.get(rc).isRemove();
    }

    @Override
    public Modification get(String row, String col) throws NoSuchElementException {
        RowCol rc = new RowCol(row, col);
        if (modifications.containsKey(rc)) {
            return modifications.get(rc);
        }

        throw new NoSuchElementException("no such element in this MemSSTableBlock");
    }

    public String getName() {
        return "0_0_Data.db";
    }
}
