package com.minimq.broker;

import java.io.File;

import com.minimq.storage.CommitLog;

/**
 * Broker tuning knobs with M1 defaults (see docs/mini-MQ-M1深入手册.html task 2):
 * boss 1 thread, workers 2x CPU, bounded business pool. Port 0 asks the OS
 * for an ephemeral port (used by tests).
 */
public final class BrokerConfig {

    private int port = 8899;
    private File storeDir = new File("data", "commitlog");
    private int workerThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
    private int businessThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    /** Bounded queue: saturation is translated into RETRY_LATER, never blocking. */
    private int businessQueueCapacity = 1024;
    private int segmentSize = CommitLog.DEFAULT_SEGMENT_SIZE;
    /**
     * Max CommitLog records examined by one FETCH before answering, even when
     * few match the requested topic. Keeps per-request work bounded until
     * ConsumeQueue indexes arrive in M2.
     */
    private int fetchScanBudget = 256;

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

    public BrokerConfig fetchScanBudget(int fetchScanBudget) {
        this.fetchScanBudget = fetchScanBudget;
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

    public int getFetchScanBudget() {
        return fetchScanBudget;
    }
}
