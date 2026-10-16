package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.broker.ReplicationService;
import com.minimq.protocol.gen.MqProto;
import com.minimq.replica.RaftNode;

/**
 * M3-8 system integration: a REAL three-node cluster inside one JVM —
 * loopback TCP, real elections, real FOLLOWER_FETCH sync, real HW — driven
 * by the real cluster SDKs (which folds the M3-7 failover client into the
 * loop: every scenario below only works if NOT_LEADER redirects work).
 *
 * <p>The four acceptance scenes from the M3 playbook, plus the deterministic
 * dirty-tail construction:
 * <ol>
 *   <li>convergence + acks=all replication + zero-loss reconcile,</li>
 *   <li>ISR shrink on follower death, writes continuing, ISR re-expansion
 *       on revival,</li>
 *   <li>ISR_TOO_SMALL refusal below min.insync.replicas (acks=1 still
 *       served),</li>
 *   <li>kill-the-leader under write load: new leader serving within the 8s
 *       budget, reconcile zero-loss,</li>
 *   <li>stale-leader revival: epoch arbitration truncates the dirty tail
 *       (uncommitted acks=1 records from the dead reign disappear; the
 *       committed acks=all prefix survives untouched).</li>
 * </ol>
 *
 * <p><b>About "kill -9"</b>: inside a JVM the closest simulation is
 * {@code shutdown()} — to the rest of the cluster it is indistinguishable
 * from process death (connections break, heartbeats stop, the election
 * timer fires). The one difference is local durability (shutdown flushes,
 * kill -9 leaves the page cache), which is exactly the M2 flush-tier
 * comparison — the multi-process ClusterMain run in M3-9 covers the real
 * thing.
 */
class ClusterIntegrationTest {

    private static final String TOPIC = "orders";
    private static final long REQUEST_TIMEOUT_MS = 5_000;
    /** The playbook's failover budget: new leader serving within 8s. */
    private static final long FAILOVER_BUDGET_MS = 8_000;

    @TempDir
    File root;

    private MiniCluster cluster;

    @BeforeEach
    void startCluster() throws Exception {
        // FailoverTransport's retry reasons log at FINE; without this the
        // producer's redirect trail is invisible when a scene fails.
        java.util.logging.Logger rootLog = java.util.logging.Logger.getLogger("");
        rootLog.setLevel(java.util.logging.Level.FINE);
        for (java.util.logging.Handler h : rootLog.getHandlers()) {
            h.setLevel(java.util.logging.Level.FINE);
        }
        cluster = new MiniCluster(root);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.close();
    }

    // ------------------------------------------------------------------
    // scene 1: convergence, acks=all replication, zero-loss reconcile
    // ------------------------------------------------------------------

    @Test
    void acksAllWritesReplicateAndReconcile() throws Exception {
        int leader = cluster.awaitLeader(5_000);
        cluster.awaitIsrSize(3, 5_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                futures.add(producer.send(TOPIC, payload(i)));
            }
            Set<Long> acked = collect(futures, 15_000);

            // offsets dense and ascending: every acks=all ack is a durability
            // claim backed by the whole ISR
            long prev = -1;
            for (long off : acked) {
                assertTrue(off > prev, "offsets ascend without gaps (prev=" + prev + ")");
                prev = off;
            }
            assertEquals(30, acked.size());

            // the ISR saw every write: leader HW passed the last ack
            ReplicationService repl = cluster.node(leader).replication();
            assertTrue(repl.highWatermark() >= prev,
                    "HW " + repl.highWatermark() + " covers the last acked offset " + prev);

            // consumers see exactly the acked set — 0 lost, 0 phantom
            List<ClientRecord> records = cluster.drainAll(TOPIC, 40, 10_000);
            assertEquals(acked, offsets(records), "reconcile: consumed == acked");
            assertEquals(30, records.size());
            for (int i = 0; i < records.size(); i++) {
                assertEquals(i, seqOf(records.get(i)), "content matches send order");
            }
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 2: ISR shrink on follower death, writes continue, re-expansion
    // ------------------------------------------------------------------

    @Test
    void followerOutageShrinksIsrWritesContinueAndIsrReexpands() throws Exception {
        int leader = cluster.awaitLeader(5_000);
        cluster.awaitIsrSize(3, 5_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            Set<Long> acked = new LinkedHashSet<>(collect(sends(producer, 10), 15_000));

            // one follower dies: within the lag window the sweep evicts it
            int deadFollower = cluster.someFollowerOf(leader);
            cluster.killNode(deadFollower);
            cluster.awaitIsrSize(2, 5_000);

            // acks=all keeps flowing: a dead replica stops gating writes the
            // moment it leaves the ISR (the anti-HW-deadlock contract)
            acked.addAll(collect(sends(producer, 10), 15_000));
            assertEquals(20, acked.size());

            // revival: catch up, prove currency, rejoin
            cluster.restartNode(deadFollower);
            cluster.awaitIsrSize(3, 10_000);

            acked.addAll(collect(sends(producer, 10), 15_000));
            assertEquals(30, acked.size());

            List<ClientRecord> records = cluster.drainAll(TOPIC, 60, 10_000);
            assertEquals(acked, offsets(records), "reconcile after shrink+rejoin: 0 lost");
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 3: ISR_TOO_SMALL below min.insync.replicas
    // ------------------------------------------------------------------

    @Test
    void acksAllRefusedBelowMinIsrWhileAcksOneServed() throws Exception {
        int leader = cluster.awaitLeader(5_000);
        cluster.awaitIsrSize(3, 5_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            Set<Long> acked = new LinkedHashSet<>(collect(sends(producer, 10), 15_000));

            // both followers die: ISR bottoms out at 1 (the leader alone)
            for (int f : cluster.followersOf(leader)) {
                cluster.killNode(f);
            }
            cluster.awaitIsrSize(1, 5_000);

            // acks=all is REFUSED — prefer unavailability over silently
            // promising durability the cluster can no longer provide
            producer.acks(MqProto.AckMode.ACKS_ALL);
            CompletableFuture<Long> refused = producer.send(TOPIC, payload(999));
            try {
                refused.get(10, TimeUnit.SECONDS);
                fail("acks=all below min.isr must not succeed");
            } catch (java.util.concurrent.ExecutionException e) {
                assertTrue(String.valueOf(e.getCause()).contains("ISR below min"),
                        "ISR_TOO_SMALL refusal, got: " + e.getCause());
            }

            // acks=1 still served: local durability is an honest, weaker promise
            producer.acks(MqProto.AckMode.ACKS_1);
            collect(java.util.Collections.singletonList(producer.send(TOPIC, payload(998))),
                    10_000);

            // one follower back: ISR=2 meets the floor, acks=all returns
            cluster.restartNode(cluster.followersOf(leader)[0]);
            cluster.awaitIsrSize(2, 10_000);
            producer.acks(MqProto.AckMode.ACKS_ALL);
            acked.addAll(collect(sends(producer, 5), 15_000));
            assertEquals(15, acked.size());

            // the acks=all durability claims all reconcile (the lone acks=1
            // write may or may not have been replicated before the assert —
            // it makes no claim, so we make none about it)
            List<ClientRecord> records = cluster.drainAll(TOPIC, 40, 10_000);
            assertTrue(offsets(records).containsAll(acked),
                    "every acks=all ack survived the ISR_TOO_SMALL episode");
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 4: kill the leader under load, new leader within 8s, reconcile
    // ------------------------------------------------------------------

    @Test
    void killedLeaderFailsOverWithinBudgetAndReconciles() throws Exception {
        int leader = cluster.awaitLeader(5_000);
        cluster.awaitIsrSize(3, 5_000);

        final MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            final Set<Long> acked = new ConcurrentSkipListSet<>();
            acked.addAll(collect(sends(producer, 20), 15_000)); // steady state

            // background writer: blocks per send, resends on failure —
            // at-least-once, reconciled by offset set
            final AtomicBoolean running = new AtomicBoolean(true);
            final AtomicLong killedAt = new AtomicLong(0);
            final AtomicLong recoveredAt = new AtomicLong(0);
            Thread writer = new Thread(new Runnable() {
                @Override
                public void run() {
                    int i = 0;
                    while (running.get()) {
                        try {
                            Long off = producer.send(TOPIC, payload(i)).get(3, TimeUnit.SECONDS);
                            acked.add(off);
                            if (killedAt.get() > 0 && recoveredAt.get() == 0) {
                                recoveredAt.set(System.nanoTime()); // first post-kill ack
                            }
                        } catch (Exception retry) {
                            // failover window: transport chases the next leader
                        }
                        i++;
                    }
                }
            }, "m3-load-writer");
            writer.setDaemon(true);
            writer.start();
            Thread.sleep(500); // let it reach cruising speed

            cluster.killNode(leader);
            killedAt.set(System.nanoTime());

            // the budget assertion: a NEW leader (never the dead one) serving
            // the same producer within the playbook's 8s
            long deadline = System.currentTimeMillis() + FAILOVER_BUDGET_MS + 2_000;
            while (recoveredAt.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            long elapsedMs = (recoveredAt.get() - killedAt.get()) / 1_000_000L;
            assertNotEquals(0L, recoveredAt.get(),
                    "producer never recovered after the leader died");
            assertTrue(elapsedMs <= FAILOVER_BUDGET_MS,
                    "failover took " + elapsedMs + "ms, budget " + FAILOVER_BUDGET_MS + "ms");

            int newLeader = cluster.awaitLeaderOtherThan(leader, 5_000);
            assertNotEquals(leader, newLeader);
            cluster.awaitIsrSize(2, 5_000);

            running.set(false);
            writer.join(5_000);
            Thread.sleep(300); // let the last in-flight ack land

            // zero-loss reconcile against the NEW leader (the consumer's own
            // failover transport finds it on its own)
            List<ClientRecord> records = cluster.drainAll(TOPIC, 500, 15_000);
            assertTrue(records.size() >= acked.size(),
                    "consumed " + records.size() + " < acked " + acked.size());
            assertTrue(offsets(records).containsAll(acked),
                    "lost acks=all data across the failover: "
                            + missing(acked, offsets(records)));
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 5: stale leader revival — epoch arbitration truncates the tail
    // ------------------------------------------------------------------

    @Test
    void staleLeaderDirtyTailTruncatedOnRevival() throws Exception {
        int leader = cluster.awaitLeader(5_000);
        cluster.awaitIsrSize(3, 5_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            Set<Long> committed = new LinkedHashSet<>(collect(sends(producer, 10), 15_000));
            cluster.awaitIsrSize(3, 5_000); // fully replicated prefix

            // deterministically build the dirty tail: kill BOTH followers, so
            // whatever the leader writes next exists nowhere else
            for (int f : cluster.followersOf(leader)) {
                cluster.killNode(f);
            }
            cluster.awaitIsrSize(1, 5_000);

            // uncommitted tail: acks=1 lands locally (ISR=1 lets the HW cover
            // it, but no majority ever vouched for it)
            producer.acks(MqProto.AckMode.ACKS_1);
            collect(sends(producer, 5), 10_000);

            // the leader dies with its dirty tail persisted (shutdown flushes;
            // the multi-process kill -9 comparison is M3-9)
            cluster.killNode(leader);

            // the followers come back exactly as they died — logs ending at
            // the committed prefix, blind to the dirty tail — and elect the
            // next leader from that lineage
            for (int f : cluster.followersOf(leader)) {
                cluster.restartNode(f);
            }
            int newLeader = cluster.awaitLeaderOtherThan(leader, 5_000);
            cluster.awaitIsrSize(2, 5_000);

            List<ClientRecord> afterElection = cluster.drainAll(TOPIC, 40, 10_000);
            assertEquals(committed, offsets(afterElection),
                    "new leader serves exactly the committed prefix");

            // the stale leader returns: epoch arbitration must truncate its
            // tail to the new epoch boundary, then it catches up and rejoins
            cluster.restartNode(leader);
            cluster.awaitIsrSize(3, 15_000);

            // the arbitration was real: rejoin requires fetching at/behind the
            // leader's LEO, which is only possible after truncation (its
            // post-crash LEO sat past the epoch boundary)
            assertEquals(RaftNode.Role.FOLLOWER, cluster.node(leader).replication().role(),
                    "the revived node came back as a follower, never a leader");

            // and the committed prefix is still exactly right
            List<ClientRecord> afterRevival = cluster.drainAll(TOPIC, 40, 10_000);
            assertEquals(committed, offsets(afterRevival),
                    "committed data untouched by the truncation");
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static List<CompletableFuture<Long>> sends(MQProducer producer, int count) {
        List<CompletableFuture<Long>> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            out.add(producer.send(TOPIC, payload(i)));
        }
        return out;
    }

    private static Set<Long> collect(List<CompletableFuture<Long>> futures, long timeoutMs)
            throws Exception {
        Set<Long> out = new LinkedHashSet<>();
        for (CompletableFuture<Long> f : futures) {
            out.add(f.get(timeoutMs, TimeUnit.MILLISECONDS));
        }
        return out;
    }

    private static Set<Long> offsets(List<ClientRecord> records) {
        Set<Long> out = new LinkedHashSet<>();
        for (ClientRecord r : records) {
            out.add(r.getOffset());
        }
        return out;
    }

    private static Set<Long> missing(Set<Long> acked, Set<Long> consumed) {
        Set<Long> miss = new HashSet<>(acked);
        miss.removeAll(consumed);
        return miss;
    }

    /** payload = 4-byte little sequence id (self-describing, cheap). */
    private static byte[] payload(int seq) {
        return ByteBuffer.allocate(4).putInt(seq).array();
    }

    private static int seqOf(ClientRecord r) {
        return ByteBuffer.wrap(r.getPayload()).getInt(0);
    }

    // ------------------------------------------------------------------
    // the cluster itself: 3 real brokers, fast election timers
    // ------------------------------------------------------------------

    private static final class MiniCluster implements java.io.Closeable {
        private static final int NODES = 3;

        private final File root;
        private final int[] ports = new int[NODES];
        private final List<InetSocketAddress> addresses = new ArrayList<>();
        private final BrokerConfig[] configs = new BrokerConfig[NODES];
        private final BrokerServer[] nodes = new BrokerServer[NODES];
        private final File offsetsDir;

        MiniCluster(File root) throws IOException {
            this.root = root;
            for (int i = 0; i < NODES; i++) {
                ports[i] = freePort();
            }
            for (int i = 0; i < NODES; i++) {
                addresses.add(new InetSocketAddress("127.0.0.1", ports[i]));
            }
            this.offsetsDir = new File(root, "consumer-offsets");
            if (!offsetsDir.mkdirs()) {
                throw new IOException("cannot create " + offsetsDir);
            }
        }

        void startAll() throws Exception {
            for (int i = 0; i < NODES; i++) {
                startNode(i);
            }
        }

        void startNode(int i) throws Exception {
            if (nodes[i] != null) {
                return; // already up
            }
            if (configs[i] == null) {
                // Test profile: election converges in a few hundred ms, the
                // ISR sweep reacts in ~1s. Everything else is production
                // wiring — real dispatch, real replication thread, real gate.
                configs[i] = BrokerConfig.defaults()
                        .port(ports[i])
                        .storeDir(new File(new File(root, "node" + i), "commitlog"))
                        .cluster(i, addresses)
                        .minIsr(2)
                        .replicaLagMaxMs(1_000)
                        .heartbeatIntervalMs(25)
                        .electionTimeoutBaseMs(120)
                        .replicationAwaitTimeoutMs(2_000)
                        .zeroCopyFetch(false);
            }
            nodes[i] = new BrokerServer(configs[i]);
            nodes[i].start();
        }

        BrokerServer node(int i) {
            return nodes[i];
        }

        /** Simulated process death (see the class javadoc for the caveat). */
        void killNode(int i) {
            if (nodes[i] != null) {
                nodes[i].shutdown();
                nodes[i] = null;
            }
        }

        /** Same data dir, same port: the revival IS the point. */
        void restartNode(int i) throws Exception {
            killNode(i);
            startNode(i);
        }

        int awaitLeader(long timeoutMs) throws InterruptedException {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int leader = leaderId();
                if (leader >= 0) {
                    return leader;
                }
                Thread.sleep(20);
            }
            throw new AssertionError(stateMessage("a leader to be elected"));
        }

        int awaitLeaderOtherThan(int deadId, long timeoutMs) throws InterruptedException {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int leader = leaderId();
                if (leader >= 0 && leader != deadId) {
                    return leader;
                }
                Thread.sleep(20);
            }
            throw new AssertionError(stateMessage("a leader other than node-" + deadId));
        }

        /** ISR including the leader (standalone semantics: 1). */
        void awaitIsrSize(int size, long timeoutMs) throws InterruptedException {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int leader = leaderId();
                if (leader >= 0) {
                    Set<Integer> isr = nodes[leader].replication().isrSnapshot();
                    if (isr.size() + 1 == size) {
                        return;
                    }
                }
                Thread.sleep(25);
            }
            throw new AssertionError(stateMessage("ISR size " + size));
        }

        private int leaderId() {
            for (int i = 0; i < NODES; i++) {
                BrokerServer n = nodes[i];
                if (n != null && n.replication() != null
                        && n.replication().role() == RaftNode.Role.LEADER) {
                    return i;
                }
            }
            return -1;
        }

        private String stateMessage(String what) {
            StringBuilder sb = new StringBuilder("timed out waiting for " + what + "; roles: [");
            for (int i = 0; i < NODES; i++) {
                BrokerServer n = nodes[i];
                sb.append(i).append('=');
                sb.append(n == null ? "DOWN"
                        : (n.replication() == null ? "standalone" : n.replication().role()));
                sb.append(i < NODES - 1 ? ", " : "");
            }
            sb.append("]");
            return sb.toString();
        }

        int[] followersOf(int leader) {
            int[] out = new int[NODES - 1];
            for (int i = 0, j = 0; i < NODES; i++) {
                if (i != leader) {
                    out[j++] = i;
                }
            }
            return out;
        }

        int someFollowerOf(int leader) {
            return followersOf(leader)[0];
        }

        /** Polls until the failover consumer has drained everything below the HW. */
        List<ClientRecord> drainAll(String topic, int maxCount, long timeoutMs)
                throws Exception {
            MQConsumer consumer = MQConsumer.subscribeCluster(addresses, topic,
                    "reconcile-" + System.nanoTime(), offsetsDir, 100, REQUEST_TIMEOUT_MS);
            try {
                List<ClientRecord> all = new ArrayList<>();
                long deadline = System.currentTimeMillis() + timeoutMs;
                int emptyPolls = 0;
                while (emptyPolls < 20 && System.currentTimeMillis() < deadline
                        && all.size() < maxCount) {
                    List<ClientRecord> batch = consumer.poll();
                    if (batch.isEmpty()) {
                        emptyPolls++;
                        Thread.sleep(25);
                    } else {
                        emptyPolls = 0;
                        all.addAll(batch);
                    }
                }
                return all;
            } finally {
                consumer.close();
            }
        }

        private static int freePort() throws IOException {
            try (ServerSocket s = new ServerSocket(0)) {
                return s.getLocalPort();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < NODES; i++) {
                if (nodes[i] != null) {
                    nodes[i].shutdown();
                    nodes[i] = null;
                }
            }
        }
    }
}
