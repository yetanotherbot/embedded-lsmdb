package cse291.lsmdb.io.sstable;

import cse291.lsmdb.utils.Counter;
import cse291.lsmdb.utils.Pair;
import cse291.lsmdb.utils.Timed;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Created by musteryu on 2017/6/1.
 */
public class MemTable {
    private String namespace, columnFamily;
    private Map<RowCol, Timed<String>> puts;
    private Map<RowCol, Long> removes;
    private Counter<String> rowRefcnt; // counter of cols per row
    private int bytesLimit;
    private int bytesNum;
    public static final int DEFAULT_BYTES_LIMIT = 1 << 16; // 64KB

    public MemTable(String namespace, String columnFamily, int bytesLimit) {
        this.puts = new TreeMap<>();
        this.removes = new TreeMap<>();
        this.rowRefcnt = new Counter<>();
        this.bytesLimit = bytesLimit;
        this.bytesNum = 0;
    }

    public MemTable(String namespace, String columnFamily) {
        this(namespace, columnFamily, DEFAULT_BYTES_LIMIT);
    }

    public void put(String row, String col, String val, long timestamp) {
        if (rowRefcnt.getCnt(row) == 0) {
            bytesNum += bytelen(row);
        }
        rowRefcnt.inc(row);
        RowCol rc = new RowCol(row, col);
        if (!puts.containsKey(rc)) {
            bytesNum += bytelen(col) + bytelen(val) + Long.BYTES;
        } else if (timestamp > puts.get(rc).getTimestamp()) {
            bytesNum -= bytelen(puts.get(rc).get());
            bytesNum += bytelen(val);
        } else return;
        puts.put(rc, new Timed<>(val, timestamp));
    }

    public void put(String row, String col, String val) {
        put(row, col, val, System.currentTimeMillis());
    }

    public void remove(String row, String col, long timestamp) {
        if (rowRefcnt.getCnt(row) == 0) {
            bytesNum += bytelen(row);
        }
        rowRefcnt.inc(row);
        RowCol rc = new RowCol(row, col);
        if (!removes.containsKey(rc)) {
            bytesNum += bytelen(col) + Long.BYTES;
        } else if (timestamp < removes.get(rc)) return;
        removes.put(rc, timestamp);
    }

    public void remove(String row, String col) {
        remove(row, col, System.currentTimeMillis());
    }

    private int bytelen(String s) {
        return s.getBytes().length;
    }

    public String get(String row, String col) {
        RowCol rc = new RowCol(row, col);
        if (!removes.containsKey(rc) && !puts.containsKey(rc)) {
            return getFromSSTable(row, col);
        } else if (!puts.containsKey(rc) || puts.get(rc).getTimestamp() < removes.get(rc)){
            // if no put happened or the put is stale
            throw new NoSuchElementException(String.format(
                    "the element is already deleted: %s/%s/(row %s, col %s)",
                    namespace, columnFamily, row, col
            ));
        }
        return puts.get(rc).get();
    }

    private String getFromSSTable(String row, String col) {
        return null;
    }

    private void checkLimit() {
        if (bytesNum > bytesLimit) {
            Map<RowCol, Timed<String>> puts = this.puts;
            Map<RowCol, Long> removes = this.removes;
            cleanup();
            persist(puts, removes);
        }
    }

    private void cleanup() {
        puts = new TreeMap<>();
        removes = new TreeMap<>();
        bytesNum = 0;
        rowRefcnt = new Counter<>();
    }

    private void persist(Map<RowCol, Timed<String>> puts, Map<RowCol, Long> removes) {
        // TODO persist to SSTable L0 and L0 persists to L1 and so on...
    }
}
