package com.minimq.broker;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;

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

    // ---- M3 cluster (replication & election) ----
    /**
     * This node's id in the cluster, or -1 for standalone (the M1/M2
     * single-broker behavior: every request is served locally, the leader
     * gate is always open, the HW equals the local log end).
     */
    private int nodeId = -1;
    /**
     * Full static cluster map, index == nodeId ("node-0": first entry).
     * Standalone brokers leave this null.
     */
    private List<InetSocketAddress> clusterNodes = null;
    /** acks=all floor: ISR below this refuses writes (ISR_TOO_SMALL). */
    private int minIsr = 2;
    /** ISR membership window: no catch-up within this = evicted (Kafka 0.10+). */
    private long replicaLagMaxMs = 10_000;
    /** Raft leader heartbeat period (etcd 10:1 shape against the timeout). */
    private long heartbeatIntervalMs = 50;
    /**
     * Election timeout base; the actual timeout is randomized in [base, 2*base).
     */
    private long electionTimeoutBaseMs = 300;
    /** acks=all HW-wait park timeout: expiry answers ERROR, never a lying OK. */
    private long replicationAwaitTimeoutMs = 5_000;
    /** FOLLOWER_FETCH batch budget (bytes per replication round). */
    private int replicaMaxFetchBytes = 1024 * 1024;

    // ---- M4 reliability (chaos matrix / dedup / dead-letter support) ----
    /**
     * Loopback chaos control port (partition injection + state readback for
     * the reliability matrix), or 0 = disabled. Test/ops tooling only — a
     * production broker never sets it.
     */
    private int chaosControlPort = 0;
    /**
     * Leader session-scoped seq-key dedup window capacity (entries), or 0 =
     * disabled (the M1-M3 semantics: every produce appends). Opt-in: legacy
     * clients reuse seq-keys across producer instances, so the window must
     * never be sprung on an existing deployment by an upgrade.
     */
    private int dedupWindowEntries = 0;

    public static BrokerConfig defaults() {
        return new BrokerConfig();
    }

    /** True when replication/election wiring should be assembled at all. */
    public boolean isClustered() {
        return nodeId >= 0 && clusterNodes != null && clusterNodes.size() > 1;
    }

    public BrokerConfig cluster(int nodeId, List<InetSocketAddress> nodes) {
        if (nodes == null || nodeId < 0 || nodeId >= nodes.size()) {
            throw new IllegalArgumentException(
                    "cluster needs a node id within the node list");
        }
        this.nodeId = nodeId;
        this.clusterNodes = nodes;
        return this;
    }

    public BrokerConfig minIsr(int minIsr) {
        this.minIsr = minIsr;
        return this;
    }

    public BrokerConfig replicaLagMaxMs(long replicaLagMaxMs) {
        this.replicaLagMaxMs = replicaLagMaxMs;
        return this;
    }

    public BrokerConfig heartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        return this;
    }

    public BrokerConfig electionTimeoutBaseMs(long electionTimeoutBaseMs) {
        this.electionTimeoutBaseMs = electionTimeoutBaseMs;
        return this;
    }

    public BrokerConfig replicationAwaitTimeoutMs(long replicationAwaitTimeoutMs) {
        this.replicationAwaitTimeoutMs = replicationAwaitTimeoutMs;
        return this;
    }

    public BrokerConfig replicaMaxFetchBytes(int replicaMaxFetchBytes) {
        this.replicaMaxFetchBytes = replicaMaxFetchBytes;
        return this;
    }

    public BrokerConfig chaosControlPort(int chaosControlPort) {
        this.chaosControlPort = chaosControlPort;
        return this;
    }

    public BrokerConfig dedupWindowEntries(int dedupWindowEntries) {
        this.dedupWindowEntries = dedupWindowEntries;
        return this;
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

    public int getNodeId() {
        return nodeId;
    }

    public List<InetSocketAddress> getClusterNodes() {
        return clusterNodes;
    }

    public int getMinIsr() {
        return minIsr;
    }

    public long getReplicaLagMaxMs() {
        return replicaLagMaxMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long getElectionTimeoutBaseMs() {
        return electionTimeoutBaseMs;
    }

    public long getReplicationAwaitTimeoutMs() {
        return replicationAwaitTimeoutMs;
    }

    public int getReplicaMaxFetchBytes() {
        return replicaMaxFetchBytes;
    }

    public int getChaosControlPort() {
        return chaosControlPort;
    }

    public int getDedupWindowEntries() {
        return dedupWindowEntries;
    }
}
