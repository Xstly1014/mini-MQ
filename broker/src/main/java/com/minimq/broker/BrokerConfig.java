package com.minimq.broker;

import java.io.File;

import com.minimq.storage.CommitLog;
import com.minimq.storage.FlushMode;

/**
 * Broker tuning knobs with M1 defaults (see docs/mini-MQ-M1深入手册.html task 2):
 * boss 1 thread, workers 2x CPU, bounded business pool. Port 0 asks the OS
 * for an ephemeral port (used by tests).
 *
 * M2 group-commit knobs mirror MySQL's binlog_group_commit_sync_delay /
 * no_delay_count (see docs/mini-MQ-M2深入手册.html task 1): the flush thread
 * forces when either linger or batch count trips, whichever first.
 */
public final class BrokerConfig {

    private int port = 8899;
    private File storeDir = new File("data", "commitlog");
    private int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
    private int businessThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    /** Bounded queue: saturation is translated into RETRY_LATER, never blocking. */
    private int businessQueueCapacity = 1024;
    private int segmentSize = CommitLog.DEFAULT_SEGMENT_SIZE;

    // ---- M2 group commit ----
    /** Durability tier; OS_FLUSH preserves the M1 behavior exactly. */
    private FlushMode flushMode = FlushMode.OS_FLUSH;
    /** Max microseconds between two forces (the aggregation window). */
    private long flushLingerUs = 500;
    /** Appends aggregated before the flusher wakes early. */
    private int flushBatchCount = 64;
    /** SYNC_FLUSH park timeout: on expiry the ack is ERROR, never a lying OK. */
    private long flushAwaitTimeoutMs = 5_000;

    // ---- M2 zero-copy fetch ----
    /**
     * FETCH responses travel as head + FileRegions (sendfile) when true;
     * false preserves the M1 inline path. Both share the v1.2 wire layout
     * [4B metaLen][meta][raw], so consumers parse them identically.
     */
    private boolean zeroCopyFetch = false;

    public static BrokerConfig defaults() {
        return new BrokerConfig();
    }

    public BrokerConfig port(int port) {
        this.port = port;
        return this;
    }

    public BrokerConfig storeDir(File storeDir) {
        this.storeDir = storeDir;
        return this;
    }

    public BrokerConfig workerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
        return this;
    }

    public BrokerConfig businessThreads(int businessThreads) {
        this.businessThreads = businessThreads;
        return this;
    }

    public BrokerConfig businessQueueCapacity(int businessQueueCapacity) {
        this.businessQueueCapacity = businessQueueCapacity;
        return this;
    }

    public BrokerConfig segmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public BrokerConfig flushMode(FlushMode flushMode) {
        this.flushMode = flushMode;
        return this;
    }

    public BrokerConfig flushLingerUs(long flushLingerUs) {
        this.flushLingerUs = flushLingerUs;
        return this;
    }

    public BrokerConfig flushBatchCount(int flushBatchCount) {
        this.flushBatchCount = flushBatchCount;
        return this;
    }

    public BrokerConfig flushAwaitTimeoutMs(long flushAwaitTimeoutMs) {
        this.flushAwaitTimeoutMs = flushAwaitTimeoutMs;
        return this;
    }

    public BrokerConfig zeroCopyFetch(boolean zeroCopyFetch) {
        this.zeroCopyFetch = zeroCopyFetch;
        return this;
    }

    public int getPort() {
        return port;
    }

    public File getStoreDir() {
        return storeDir;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public int getBusinessThreads() {
        return businessThreads;
    }

    public int getBusinessQueueCapacity() {
        return businessQueueCapacity;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    public long getFlushLingerUs() {
        return flushLingerUs;
    }

    public int getFlushBatchCount() {
        return flushBatchCount;
    }

    public long getFlushAwaitTimeoutMs() {
        return flushAwaitTimeoutMs;
    }

    public boolean isZeroCopyFetch() {
        return zeroCopyFetch;
    }
}
