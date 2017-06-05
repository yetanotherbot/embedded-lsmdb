package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Counter;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.RowCol;
import cse291.lsmdb.utils.Timed;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Created by musteryu on 2017/6/1.
 */
public class MemTable {
    private Descriptor desc;
    private Map<RowCol, Modification> modifications;
    private Counter<String> rowRefcnt; // counter of cols per row
    private int bytesLimit;
    private int bytesNum;
    public static final int DEFAULT_BYTES_LIMIT = 1 << 16; // 64KB

    public MemTable(Descriptor desc, int bytesLimit) {
        this.desc = desc;
        this.modifications = new TreeMap<>();
        this.rowRefcnt = new Counter<>();
        this.bytesLimit = bytesLimit;
        this.bytesNum = 0;
    }

    public MemTable(Descriptor desc) {
        this(desc, DEFAULT_BYTES_LIMIT);
    }

    private void addModification(String row, String col, Modification curr) {
        if (rowRefcnt.getCnt(row) == 0) {
            bytesNum += bytelen(row);
        }
        rowRefcnt.inc(row);
        RowCol rc = new RowCol(row, col);
        if (!modifications.containsKey(rc)) {
            bytesNum += bytelen(col) + Long.BYTES;
        } else {
            Modification last = modifications.get(rc);
            if (last.getTimestamp() > curr.getTimestamp()) return;
            int lastLen = last.isRemove() ? 0 : bytelen(last.getIfPresent().get());
            bytesNum -= lastLen;
        }
        if (curr.isPut()) {
            bytesNum += bytelen(curr.getIfPresent().get());
        }
        modifications.put(rc, curr);
    }

    public void put(String row, String col, String val, long timestamp) {
        addModification(row, col, Modification.put(new Timed<String>(val)));
    }

    public void put(String row, String col, String val) {
        addModification(row, col, Modification.put(Timed.now(val)));
    }

    public void remove(String row, String col, long timestamp) {
        addModification(row, col, Modification.remove(timestamp));
    }

    public void remove(String row, String col) {
        remove(row, col, System.currentTimeMillis());
    }

    private int bytelen(String s) {
        return s.getBytes().length;
    }

    public String get(String row, String col) {
        RowCol rc = new RowCol(row, col);
        if (!modifications.containsKey(rc)) {
            return getFromSSTable(row, col);
        } else if (modifications.get(rc).isRemove()){
            // if no put happened or the put is stale
            throw new NoSuchElementException(String.format(
                    "the element is already deleted: %s/%s/(row %s, col %s)",
                    desc.ns, desc.cf, row, col
            ));
        }
        Modification m = modifications.get(rc);
        if (m.isPut()) return m.getIfPresent().get();
        throw new NoSuchElementException("no such element, reason unknown");
    }

    private String getFromSSTable(String row, String col) {
        return null;
    }

    private void checkLimit() {
        if (bytesNum > bytesLimit) {
            Map<RowCol, Modification> modifications = this.modifications;
            cleanup();
            persist(modifications);
        }
    }

    private void cleanup() {
        modifications = new TreeMap<>();
        bytesNum = 0;
        rowRefcnt = new Counter<>();
    }

    private void persist(Map<RowCol, Modification> modifications) {
        // TODO persist to SSTable L0 and L0 persists to L1 and so on...
    }
}
