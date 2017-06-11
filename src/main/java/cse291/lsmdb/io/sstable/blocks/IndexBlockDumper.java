package cse291.lsmdb.io.sstable.blocks;

import cse291.lsmdb.utils.Pair;

import java.io.IOException;
import java.util.List;

/**
 * Created by musteryu on 2017/6/8.
 */
public class IndexBlockDumper {
    private IndexBlock idx;

    public IndexBlockDumper(IndexBlock idx) {
        this.idx = idx;
    }

    public void dump(List<Pair<String, String>> ranges) throws IOException {
        idx.requireFileExists();
        ComponentFile c = null;
        try {
            c = idx.getWritableComponentFile();
            for (Pair<String, String> range : ranges) {
//                System.out.println("put: " + range);
                c.writeString(range.left);
                c.writeString(range.right);
            }
        } finally {
            ComponentFile.tryClose(c);
        }
    }
}
