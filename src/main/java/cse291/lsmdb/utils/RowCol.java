package cse291.lsmdb.utils;

/**
 * Created by musteryu on 2017/6/3.
 */
public class RowCol extends Pair<String, String> implements Comparable<RowCol> {
    public RowCol(String row, String col) { super(row, col); }

    @Override
    public int compareTo(RowCol that) {
        int rcmp = this.left.compareTo(that.left);
        if (rcmp < 0) return -1;
        if (rcmp > 0) return 1;
        int ccmp = this.right.compareTo(that.right);
        if (ccmp < 0) return -1;
        if (ccmp > 0) return 1;
        return 0;
    }
}
