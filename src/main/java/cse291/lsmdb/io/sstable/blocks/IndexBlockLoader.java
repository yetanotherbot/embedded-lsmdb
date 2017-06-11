package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by musteryu on 2017/6/4.
 */
public class IndexBlockLoader {
    private IndexBlock indexBlock;

    public IndexBlockLoader(IndexBlock indexBlock) {
        this.indexBlock = indexBlock;
    }

    /**
     * Get the range of Data block i.
     * @param i the index of Data block to get the range
     * @return the range of the selected Data block
     */
    public Pair<String, String> rangeOf(int i) throws IOException {
        ArrayList<Pair<String, String>> ranges = getRanges();
        return ranges.get(i);
    }

    public int lookup(String row) {
        try {
            ArrayList<Pair<String, String>> ranges = getRanges();
            int lo = 0, hi = ranges.size();
            while (lo < hi) {
                int mid = lo + (hi - lo) / 2;
                Pair<String, String> range = ranges.get(mid);
                if (row.compareTo(range.right) > 0) lo = mid + 1;
                else if (row.compareTo(range.left) < 0) hi = mid;
                else return mid;
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return -1;
    }

    public ArrayList<Pair<String, String>> getRanges() throws IOException {
        ComponentFile c = indexBlock.getReadableComponentFile();
        ArrayList<Pair<String, String>> ranges = new ArrayList<>();
        try {
            while (!c.eof()) {
                String r1 = c.readString();
                String r2 = c.readString();
                if (r1 != null && r2 != null)
                    ranges.add(new Pair<>(r1, r2));
//                System.out.println("get: " + new Pair<>(r1, r2));
            }
            Comparator<Pair<String, String>> comp = Pair.<String, String>comparator();
            for (int i = 0; i < ranges.size() - 1; i++) {
                assert comp.compare(ranges.get(i), ranges.get(i+1)) <= 0;
            }
            return ranges;
        } finally {
            c.tryClose();
        }
    }
}
