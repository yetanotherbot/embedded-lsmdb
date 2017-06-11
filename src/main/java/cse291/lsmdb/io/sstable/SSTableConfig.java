package cse291.lsmdb.io.sstable;

import cse291.lsmdb.io.interfaces.StringHasher;
import cse291.lsmdb.utils.Modifications;
import cse291.lsmdb.utils.Row;
import cse291.lsmdb.utils.Timed;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * Created by musteryu on 2017/6/7.
 */
public final class SSTableConfig {
    private int blockBytesLimit = 1024 * 1024 * 16; // 16 MB

    private int memTableBytesLimit = 1024 * 1024 * 16; // 16 MB;

    private int perBlockBloomFilterBits = 1024;

    private int onDiskLevelsLimit = 3;

    private Function<LinkedList<MemTable>, Modifications> memTablesFlushStrategy = l -> {
        Modifications mod = new Modifications(blockBytesLimit);
        while (l.size() > 1) {
            mod.offer(l.removeFirst().stealModifications());
        }
        return mod;
    };

    private int memTablesLimit = 4;

    private Function<Integer, Integer> blocksNumLimitForLevel = l -> ((int) Math.pow(10, l));

    private StringHasher hasher = new MurMurHasher();

    private String blockFilenameSuffix = ".db";

    private String tempBlockFilenameSuffix = ".db.tmp";

    private int fileBufferSize = 256 * 256;

    private int rowCacheCapacity = 1024;

    private SSTableConfig() { }

    public int getBlockBytesLimit() {
        return blockBytesLimit;
    }

    public int getMemTableBytesLimit() {
        return memTableBytesLimit;
    }

    public int getPerBlockBloomFilterBits() {
        return perBlockBloomFilterBits;
    }

    public int getOnDiskLevelsLimit() {
        return onDiskLevelsLimit;
    }

    public int getMemTablesLimit() {
        return memTablesLimit;
    }

    public int getRowCacheCapacity() { return rowCacheCapacity; }

    public Function<LinkedList<MemTable>, Modifications> getMemTablesFlushStrategy() {
        return memTablesFlushStrategy;
    }

    public Function<Integer, Integer> getBlocksNumLimitForLevel() {
        return blocksNumLimitForLevel;
    }

    public StringHasher getHasher() {
        return hasher;
    }

    public String getBlockFilenameSuffix() {
        return blockFilenameSuffix;
    }

    public String getTempBlockFilenameSuffix() {
        return tempBlockFilenameSuffix;
    }

    public static SSTableConfigBuilder builder() {
        return new SSTableConfigBuilder();
    }

    public static SSTableConfig defaultConfig() {
        return new SSTableConfig();
    }

    public int getFileBufferSize() {
        return fileBufferSize;
    }

    public static class SSTableConfigBuilder {
        private SSTableConfig config;
        private SSTableConfigBuilder() {
            config = new SSTableConfig();
        }

        public SSTableConfigBuilder setBlockBytesLimit(int blockBytesLimit) {
            config.blockBytesLimit = blockBytesLimit;
            return this;
        }

        public SSTableConfigBuilder setMemTableBytesLimit(int memTableBytesLimit) {
            config.memTableBytesLimit = memTableBytesLimit;
            return this;
        }

        public SSTableConfigBuilder setPerBlockBloomFilterBits(int perBlockBloomFilterBits) {
            config.perBlockBloomFilterBits = perBlockBloomFilterBits;
            return this;
        }

        public SSTableConfigBuilder setOnDiskLevelsLimit(int onDiskLevelsLimit) {
            config.onDiskLevelsLimit = onDiskLevelsLimit;
            return this;
        }

        public SSTableConfigBuilder setBlocksNumLimitForLevel(
                Function<Integer, Integer> blocksNumLimitForLevel) {
            config.blocksNumLimitForLevel = blocksNumLimitForLevel;
            return this;
        }

        public SSTableConfigBuilder setHasher(StringHasher hasher) {
            config.hasher = hasher;
            return this;
        }

        public SSTableConfigBuilder setBlockFilenameSuffix(String blockFilenameSuffix) {
            config.blockFilenameSuffix = blockFilenameSuffix;
            return this;
        }

        public SSTableConfigBuilder setTempBlockFilenameSuffix(String tempBlockFilenameSuffix) {
            config.tempBlockFilenameSuffix = tempBlockFilenameSuffix;
            return this;
        }

        public SSTableConfigBuilder setMemTablesLimit(int limit) {
            config.memTablesLimit = limit;
            return this;
        }

        public SSTableConfigBuilder setMemTablesFlushStrategy(
                Function<LinkedList<MemTable>, Modifications> f) {
            config.memTablesFlushStrategy = f;
            return this;
        }

        public SSTableConfigBuilder setFileBufferSize(int size) {
            config.fileBufferSize = size;
            return this;
        }

        public SSTableConfig build() {
            return config;
        }

    }
}
