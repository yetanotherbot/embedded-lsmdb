package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Timed;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Created by musteryu on 2017/6/1.
 */
public class MemTable {
    private final Descriptor desc;
    private Map<String, Modification> modifications;
    private final String column;
    private int bytesLimit;
    private int bytesNum;
    public static final int DEFAULT_BYTES_LIMIT = 16 * 1024 * 1024; // 16 MB

    public MemTable(Descriptor desc, String column, int bytesLimit) {
        this.desc = desc;
        this.modifications = new TreeMap<>();
        this.bytesLimit = bytesLimit;
        this.bytesNum = 0;
        this.column = column;
    }

    public MemTable(Descriptor desc, String column) {
        this(desc, column, DEFAULT_BYTES_LIMIT);
    }

    private boolean addModification(String row, Modification curr) throws MemTableFull {
        try {
            if (modifications.containsKey(row)) {
                Modification last = modifications.get(row);
                if (last.getTimestamp() > curr.getTimestamp()) return false;
                if (last.isPut()) {
                    bytesNum -= bytelen(last.getIfPresent().get());
                }
                if (curr.isPut()) {
                    bytesNum += bytelen(curr.getIfPresent().get());
                }
            } else {
                bytesNum += bytelen(curr.getIfPresent().get());
                bytesNum += Long.BYTES;
            }
            return true;
        } finally {
            checkLimit();
        }
    }

    public boolean put(String row, String val, long timestamp) throws MemTableFull {
        Modification mod = Modification.put(new Timed<>(val, timestamp));
        return addModification(row, mod);
    }

    public boolean put(String row, String val) throws MemTableFull {
        return addModification(row, Modification.put(Timed.now(val)));
    }

    public boolean remove(String row, long timestamp) throws MemTableFull {
        Modification mod = Modification.remove(timestamp);
        return addModification(row, mod);
    }

    public boolean remove(String row) throws MemTableFull {
        return remove(row, System.currentTimeMillis());
    }


    private int bytelen(String s) {
        return s.getBytes().length;
    }

    /**
     * Gets an element from the MemTable if it exists
     * @throws NoSuchElementException if the element is not present in the MemTable
     */
    public String get(String row) throws NoSuchElementException {
        if (!modifications.containsKey(row)) {
            throw new NoSuchElementException(String.format(
                    "the element is not in memtable: (col: %s, row %s)",
                    column, row
            ));
        } else if (modifications.get(row).isRemove()){
            // if no put happened or the put is stale
            throw new NoSuchElementException(String.format(
                    "the element is already deleted: %s/%s/(col %s, row %s)",
                    desc.ns, desc.cf, row, column
            ));
        }
        Modification m = modifications.get(row);
        if (m.isPut()) return m.getIfPresent().get();
        throw new NoSuchElementException("no such element, reason unknown");
    }

    private void checkLimit() throws MemTableFull {
        if (bytesNum > bytesLimit) {
            throw new MemTableFull();
        }
    }

    private void cleanup() {
        modifications = new TreeMap<>();
        bytesNum = 0;
    }

    public Map<String, Modification> steal() {
        Map<String, Modification> mods = modifications;
        cleanup();
        return mods;
    }

    public static class MemTableFull extends Exception {}
}
