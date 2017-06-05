package cse291.lsmdb.io.sstable.blocks;

import java.io.File;

/**
 * Created by musteryu on 2017/5/30.
 */
public class Descriptor {
    public final String base, ns, cf;

    public Descriptor(String base, String ns, String cf) {
        this.base = base;
        this.ns = ns;
        this.cf = cf;
    }

    public File getDir() {
        File dir = new File(new File(base, ns), cf);
        if (!dir.exists()) {
            dir.mkdir();
        }
        return dir;
    }
}
