package cse291.lsmdb.io.interfaces;

import java.io.File;
import java.io.IOException;

/**
 * Created by musteryu on 2017/6/4.
 */
public interface Block {
    File getFile() throws IOException;
}
