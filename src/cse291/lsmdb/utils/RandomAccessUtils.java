package cse291.lsmdb.utils;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by musteryu on 2017/6/3.
 */
public class RandomAccessUtils {

    public static enum VarLengthType {
        VAR_LENGTH_8,
        VAR_LENGTH_16,
        VAR_LENGTH_32
    }

    public static byte[] readVarLength(RandomAccessFile raf, VarLengthType varLengthType)
            throws IOException{
        switch (varLengthType) {
            case VAR_LENGTH_8: {
                byte len = raf.readByte();
                byte[] bytes = new byte[len];
                raf.read(bytes);
                return bytes;
            }
            case VAR_LENGTH_16: {
                char len = raf.readChar();
                byte[] bytes = new byte[len];
                raf.read(bytes);
                return bytes;
            }
            case VAR_LENGTH_32: default: {
                int len = raf.readInt();
                byte[] bytes = new byte[len];
                raf.read(bytes);
                return bytes;
            }
        }
    }
}
