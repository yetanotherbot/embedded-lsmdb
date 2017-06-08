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

    private static class SSTableConfigBuilder {
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

        public SSTableConfigBuilder setBlocksNumLimitForLevel(Function<Integer, Integer> blocksNumLimitForLevel) {
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

        public SSTableConfig build() {
            return config;
        }

    }
}