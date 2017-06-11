package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.sstable.blocks.Descriptor;
import cse291.lsmdb.utils.Modification;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Qualifier;
import cse291.lsmdb.utils.Timed;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by musteryu on 2017/6/1.
 */
public class MemTable {
    private final Descriptor desc;
    private final String column;
    private Modifications modifications;
    private SSTableConfig config;

    public MemTable(Descriptor desc, String column, SSTableConfig config) {
        this.desc = desc;
        this.config = config;
        this.modifications = new Modifications(config.getMemTableBytesLimit());
        this.column = column;
    }

    public synchronized boolean put(String row, String val, long timestamp) throws MemTableFull {
        Modification mod = Modification.put(new Timed<>(val, timestamp));
        Modification inserted = modifications.put(row, mod);
        if (modifications.existLimit()) {
            throw new MemTableFull();
        }
        return inserted != null;
    }

    public synchronized boolean put(String row, String val) throws MemTableFull {
        return put(row, val, System.currentTimeMillis());
    }

    public synchronized boolean remove(String row, long timestamp) throws MemTableFull {
        Modification mod = Modification.remove(timestamp);
        Modification inserted = modifications.put(row, mod);
        if (modifications.existLimit()) {
            throw new MemTableFull();
        }
        return inserted != null;
    }

    public synchronized Map<String, Timed<String>> getColumnWithQualifier(Qualifier q) {
        Map<String, Timed<String>> column = new HashMap<>();
        for (Map.Entry<String, Modification> entry : modifications.entrySet()) {
            String rowKey = entry.getKey();
            Modification mod = entry.getValue();
            Timed<String> timedValue = mod.getIfPresent();
            if (mod.isPut() && q.qualify(rowKey,timedValue.get())) {
                column.put(rowKey, timedValue);
            }
        }
        return column;
    }

    public synchronized boolean remove(String row) throws MemTableFull {
        return remove(row, System.currentTimeMillis());
    }


    /**
     * Gets an element from the MemTable if it exists
     *
     * @throws NoSuchElementException if the element is not present in the MemTable
     */
    public String get(String row) throws NoSuchElementException {
        if (!modifications.containsKey(row)) {
            throw new NoSuchElementException(String.format(
                    "the element is not in memtable: (col: %s, row %s)",
                    column, row
            ));
        } else if (modifications.get(row).isRemove()) {
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


    private synchronized void cleanup() {
        modifications = new Modifications(config.getMemTableBytesLimit());
    }

    /**
     * Steal the modifications from the current MemTable
     */
    public Modifications stealModifications() {
        Modifications mods = modifications;
        cleanup();
        return mods;
    }

    /**
     * Steal the immutable modifications from the current MemTable
     */
    public Modifications stealImmutableModifications() {
        return Modifications.immutableRef(stealModifications());
    }

    public Modifications getImmutableModifications() {
        return Modifications.immutableRef(modifications);
    }

    public static class MemTableFull extends Exception {
    }
}
