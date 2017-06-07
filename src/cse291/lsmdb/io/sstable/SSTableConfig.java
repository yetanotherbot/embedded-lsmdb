package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;

import java.util.function.Function;

/**
 * Created by musteryu on 2017/6/7.
 */
public final class SSTableConfig {
    private int blockBytesLimit = 1024 * 1024 * 16; // 16 MB
    private int memTableBytesLimit = 1024 * 1024 * 16; // 16 MB;
    private int perBlockBloomFilterBits = 1024;
    private int onDiskLevelsLimit = 3;
    private Function<Integer, Integer> blocksNumLimitForLevel = l -> ((int) Math.pow(10, l));
    private StringHasher hasher = new MurMurHasher();
    private String blockFilenameSuffix = ".db";
    private String tempBlockFilenameSuffix = ".db.tmp";

    public String getTempBlockFilenameSuffix() {
        return tempBlockFilenameSuffix;
    }

    public void setTempBlockFilenameSuffix(String tempBlockFilenameSuffix) {
        this.tempBlockFilenameSuffix = tempBlockFilenameSuffix;
    }


    public int getBlockBytesLimit() {
        return blockBytesLimit;
    }

    public void setBlockBytesLimit(int blockBytesLimit) {
        this.blockBytesLimit = blockBytesLimit;
    }

    public int getMemTableBytesLimit() {
        return memTableBytesLimit;
    }

    public void setMemTableBytesLimit(int memTableBytesLimit) {
        this.memTableBytesLimit = memTableBytesLimit;
    }

    public int getPerBlockBloomFilterBits() {
        return perBlockBloomFilterBits;
    }

    public void setPerBlockBloomFilterBits(int perBlockBloomFilterBits) {
        this.perBlockBloomFilterBits = perBlockBloomFilterBits;
    }

    public int getOnDiskLevelsLimit() {
        return onDiskLevelsLimit;
    }

    public void setOnDiskLevelsLimit(int onDiskLevelsLimit) {
        this.onDiskLevelsLimit = onDiskLevelsLimit;
    }

    public Function<Integer, Integer> getBlocksNumLimitForLevel() {
        return blocksNumLimitForLevel;
    }

    public void setBlocksNumLimitForLevel(Function<Integer, Integer> blocksNumLimitForLevel) {
        this.blocksNumLimitForLevel = blocksNumLimitForLevel;
    }

    public StringHasher getHasher() {
        return hasher;
    }

    public void setHasher(StringHasher hasher) {
        this.hasher = hasher;
    }

    public String getBlockFilenameSuffix() {
        return blockFilenameSuffix;
    }

    public void setBlockFilenameSuffix(String blockFilenameSuffix) {
        this.blockFilenameSuffix = blockFilenameSuffix;
    }
}
