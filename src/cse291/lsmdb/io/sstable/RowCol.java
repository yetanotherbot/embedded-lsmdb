package cse291.lsmdb.io.sstable;

import cse291.lsmdb.utils.Pair;

/**
 * Created by musteryu on 2017/6/3.
 */
class RowCol extends Pair<String, String> {
    RowCol(String row, String col) { super(row, col); }
}
