package cse291.lsmdb.io.sstable;

import cse291.lsmdb.utils.Timed;

import java.io.IOException;
import java.util.*;

/**
 * Created by musteryu on 2017/6/3.
 */
public class MemDataLoader extends AbstractDataLoader {
    private String namespace, columnFamily;
    private Map<RowCol, Modification> modifications;

    public MemDataLoader(
            String namespace,
            String columnFamily,
            Map<RowCol, Timed<String>> puts,
            Map<RowCol, Long> removes
    ) {
        this.namespace = namespace;
        this.columnFamily = columnFamily;
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

    public boolean isRemoved(String row, String col) {
        RowCol rc = new RowCol(row, col);
        return modifications.containsKey(rc) && modifications.get(rc).isRemove();
    }

    @Override
    public Timed<String> get(String row, String col) throws NoSuchElementException, IOException {
        RowCol rc = new RowCol(row, col);
        if (modifications.containsKey(rc)) {
            Modification m = modifications.get(rc);
            if (m.isPut()) return m.getIfPresent();
        }

        throw new NoSuchElementException("no such element in this MemDataLoader");
    }

    public String getName() {
        return "0_0_Data.db";
    }
}
