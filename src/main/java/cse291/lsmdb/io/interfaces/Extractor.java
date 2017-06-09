package cse291.lsmdb.io.interfaces;

import java.io.IOException;

/**
 * Created by musteryu on 2017/6/5.
 */
public interface Extractor<T> {
    T extract() throws IOException;
}
