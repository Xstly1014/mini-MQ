package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.gen.MqProto;

/**
 * M3-9 acceptance: the SAME four playbook scenes as ClusterIntegrationTest,
 * but across REAL process boundaries — three java subprocesses, each a full
 * broker with its own storage and election timers, wired over loopback TCP.
 *
 * <p>What only this harness can prove:
 * <ul>
 *   <li><b>Real kill -9</b>: {@code Process.destroyForcibly()} is
 *       TerminateProcess on Windows — no shutdown hooks, no flush, the M2
 *       flush-tier semantics are what stands between the crash and the
 *       data.</li>
 *   <li><b>Real graceful shutdown</b>: a QUIT line on stdin walks
 *       BrokerServer's ordered shutdown (flush, step-down) — the
 *       kill -9-vs-graceful comparison the M1 foreshadowing promised.</li>
 *   <li><b>Real revival</b>: a killed node restarts on the SAME storeDir —
 *       Raft state reloads (term/votedFor), the log recovers by CRC, and
 *       epoch arbitration decides the dirty tail's fate.</li>
 * </ul>
 *
 * <p>Cluster observation is deliberately black-box: there is no in-process
 * {@code replication()} handle to peek at. The leader is found by probing
 * each port with a single-node producer (a non-leader answers NOT_LEADER
 * with a hint), ISR health is inferred from acks=all behavior, and the
 * election moment comes from tailing the child's stderr log for the
 * {@code elected leader, epoch N} line — the same telemetry an operator
 * outside the box would have.
 */
@Timeout(240)
class MultiProcessClusterTest {

    private static final String TOPIC = "orders";
    private static final long REQUEST_TIMEOUT_MS = 5_000;
    /** The playbook's failover budget: new leader serving within 8s. */
    private static final long FAILOVER_BUDGET_MS = 8_000;
    private static final byte[] PROBE_PAYLOAD = new byte[4];

    @TempDir
    File root;

    private ProcCluster cluster;

    @BeforeEach
    void startCluster() throws Exception {
        // FailoverTransport logs its redirect trail at FINE — without this a
        // failing scene is a silent hang instead of a story.
        java.util.logging.Logger rootLog = java.util.logging.Logger.getLogger("");
        rootLog.setLevel(java.util.logging.Level.FINE);
        for (java.util.logging.Handler h : rootLog.getHandlers()) {
            h.setLevel(java.util.logging.Level.FINE);
        }
        cluster = new ProcCluster(root);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.close();
        // Windows: TerminateProcess tears the children's mmap sections down
        // asynchronously, so JUnit's TempDir cleanup can race the OS and
        // report an ERROR even though every assertion passed. Wipe the tree
        // ourselves with retries first; when we succeed, JUnit's own pass
        // has nothing left to trip over.
        for (int attempt = 0; attempt < 50; attempt++) {
            try {
                deleteRecursively(root);
                return;
            } catch (IOException stillLocked) {
                Thread.sleep(100);
            }
        }
        // still locked after 5s: let JUnit's cleanup surface whatever holds it
    }

    private static void deleteRecursively(File f) throws IOException {
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) {
                deleteRecursively(k);
            }
        }
        Files.delete(f.toPath()); // throws while a child mmap is still held
    }

    // ------------------------------------------------------------------
    // scene 1+2: kill -9 the leader under load — 8s budget with a timing
    // breakdown — then the dead leader revives as a follower
    // ------------------------------------------------------------------

    @Test
    void killNineLeaderFailsOverWithinBudgetThenRevivedLeaderRejoins() throws Exception {
        int leader = cluster.awaitLeader(20_000);

        final MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            final Set<Long> acked = new ConcurrentSkipListSet<>();
            acked.addAll(collect(sends(producer, 20), 20_000)); // steady state

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
                            // failover window: the transport chases the next leader
                        }
                        i++;
                    }
                }
            }, "m3-proc-load-writer");
            writer.setDaemon(true);
            writer.start();
            Thread.sleep(500); // cruising speed

            cluster.baselineElectedLines();
            cluster.killNode(leader); // REAL kill -9: TerminateProcess
            killedAt.set(System.nanoTime());

            long electedAt = cluster.awaitNewElection(FAILOVER_BUDGET_MS);
            long deadline = System.currentTimeMillis() + FAILOVER_BUDGET_MS + 2_000;
            while (recoveredAt.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            long elapsedMs = (recoveredAt.get() - killedAt.get()) / 1_000_000L;
            assertNotEquals(0L, recoveredAt.get(),
                    "producer never recovered after kill -9 of the leader");
            assertTrue(elapsedMs <= FAILOVER_BUDGET_MS,
                    "failover took " + elapsedMs + "ms, budget " + FAILOVER_BUDGET_MS + "ms");

            // the timing breakdown the m3-failover.md archive is built from
            long detectAndElectMs = (electedAt - killedAt.get()) / 1_000_000L;
            long redirectMs = elapsedMs - detectAndElectMs;
            System.out.println("M3-9 failover breakdown: detect+elect=" + detectAndElectMs
                    + "ms, client-redirect=" + redirectMs + "ms, total=" + elapsedMs
                    + "ms (budget " + FAILOVER_BUDGET_MS + "ms)");

            int newLeader = cluster.awaitLeaderOtherThan(leader, 10_000);
            assertNotEquals(leader, newLeader, "a NEW leader took over");

            running.set(false);
            writer.join(5_000);
            Thread.sleep(300); // last in-flight ack lands

            // zero-loss reconcile through the failover consumer
            List<ClientRecord> records = cluster.drainAll(TOPIC, 1_000, 20_000);
            assertTrue(records.size() >= acked.size(),
                    "consumed " + records.size() + " < acked " + acked.size());
            assertTrue(offsets(records).containsAll(acked),
                    "lost acks=all data across kill -9 failover: "
                            + missing(acked, offsets(records)));

            // ---- scene 2: the dead leader revives on the same storeDir ----
            cluster.restartNode(leader);
            Thread.sleep(2_000); // fetch, arbitrate, truncate, rejoin

            // writes keep flowing (the revived node must not disturb service)
            acked.addAll(collect(sends(producer, 10), 20_000));

            // it came back as a FOLLOWER: the incumbent (higher epoch) is
            // still the leader the probes find
            int leaderAfterRevival = cluster.awaitLeaderOtherThan(leader, 10_000);
            assertEquals(newLeader, leaderAfterRevival,
                    "the revived node must not preempt the incumbent leader");

            List<ClientRecord> afterRevival = cluster.drainAll(TOPIC, 1_000, 20_000);
            assertTrue(offsets(afterRevival).containsAll(acked),
                    "committed data lost after the stale leader revived: "
                            + missing(acked, offsets(afterRevival)));
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 3: one follower kill -9 for ~10s — writes continue, then it
    // catches up and rejoins
    // ------------------------------------------------------------------

    @Test
    void followerKillNineOutageWritesContinueThenRejoins() throws Exception {
        int leader = cluster.awaitLeader(20_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);
            Set<Long> acked = new LinkedHashSet<>(collect(sends(producer, 10), 20_000));

            int deadFollower = cluster.someFollowerOf(leader);
            cluster.killNode(deadFollower); // REAL kill -9

            // writes continue THROUGH the stall window: the ISR sweep evicts
            // the dead replica within the lag window (3s here) and the HW
            // starts moving again — acks=all is never hostage to a corpse
            acked.addAll(collect(sends(producer, 10), 20_000));

            Thread.sleep(7_000); // ride out the rest of the 10s outage
            acked.addAll(collect(sends(producer, 10), 20_000));
            assertEquals(30, acked.size());

            // revival on the same storeDir: recover by CRC, fetch, rejoin
            cluster.restartNode(deadFollower);
            Thread.sleep(3_000); // catch up + rejoin

            acked.addAll(collect(sends(producer, 10), 20_000));
            assertEquals(40, acked.size());

            List<ClientRecord> records = cluster.drainAll(TOPIC, 200, 20_000);
            assertTrue(records.size() >= acked.size(),
                    "consumed " + records.size() + " < acked " + acked.size());
            assertTrue(offsets(records).containsAll(acked),
                    "lost data across the follower outage: "
                            + missing(acked, offsets(records)));
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // scene 4: graceful shutdown vs kill -9 — the M1 foreshadowing cashed
    // ------------------------------------------------------------------

    @Test
    void gracefulShutdownAndKillNineBothKeepAckedAllData() throws Exception {
        cluster.awaitLeader(20_000);

        MQProducer producer = MQProducer.connectCluster(
                cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
        try {
            producer.acks(MqProto.AckMode.ACKS_ALL);

            // ---- A: graceful shutdown of the WHOLE cluster ----
            Set<Long> gracefulAcked = new LinkedHashSet<>(
                    collect(sends(producer, 10), 20_000));
            producer.close(); // no in-flight writes when the nodes go down
            for (int i = 0; i < ProcCluster.NODES; i++) {
                cluster.gracefulStop(i); // QUIT: flush + step-down, in order
            }

            // ---- B: the same cluster, killed with -9 mid-life ----
            MQProducer second = MQProducer.connectCluster(
                    cluster.addresses, REQUEST_TIMEOUT_MS, 64 * 1024, 20);
            try {
                second.acks(MqProto.AckMode.ACKS_ALL);
                for (int i = 0; i < ProcCluster.NODES; i++) {
                    cluster.restartNode(i);
                }
                cluster.awaitLeader(20_000);
                Set<Long> killedAcked = new LinkedHashSet<>(
                        collect(sends(second, 10), 20_000));
                second.close();

                for (int i = 0; i < ProcCluster.NODES; i++) {
                    cluster.killNode(i); // REAL kill -9: no hooks, no flush
                }

                // both regimes restart on the same disks and must account
                // for every acks=all claim they ever made
                for (int i = 0; i < ProcCluster.NODES; i++) {
                    cluster.restartNode(i);
                }
                cluster.awaitLeader(20_000);

                Set<Long> allAcked = new LinkedHashSet<>(gracefulAcked);
                allAcked.addAll(killedAcked);
                List<ClientRecord> records = cluster.drainAll(TOPIC, 200, 20_000);
                assertTrue(records.size() >= allAcked.size(),
                        "consumed " + records.size() + " < acked " + allAcked.size());
                assertTrue(offsets(records).containsAll(allAcked),
                        "acks=all data lost across graceful+kill-9: "
                                + missing(allAcked, offsets(records)));
            } finally {
                second.close();
            }
        } finally {
            producer.close();
        }
    }

    // ------------------------------------------------------------------
    // helpers (shared shape with ClusterIntegrationTest)
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

    private static byte[] payload(int seq) {
        return ByteBuffer.allocate(4).putInt(seq).array();
    }

    // ------------------------------------------------------------------
    // the cluster itself: 3 real OS processes
    // ------------------------------------------------------------------

    private static final class ProcCluster implements java.io.Closeable {
        private static final int NODES = 3;
        private static final String ELECTED_MARKER = "elected leader";

        private final File root;
        private final int[] ports = new int[NODES];
        private final List<InetSocketAddress> addresses = new ArrayList<>();
        private final File[] storeDirs = new File[NODES];
        private final File[] logs = new File[NODES];
        private final Process[] procs = new Process[NODES];
        private final boolean[] alive = new boolean[NODES];
        private final int[] electedBaseline = new int[NODES];
        private final File offsetsDir;

        ProcCluster(File root) throws IOException {
            this.root = root;
            for (int i = 0; i < NODES; i++) {
                ports[i] = freePort();
                addresses.add(new InetSocketAddress("127.0.0.1", ports[i]));
            }
            for (int i = 0; i < NODES; i++) {
                storeDirs[i] = new File(new File(root, "node" + i), "commitlog");
                logs[i] = new File(root, "node" + i + ".log");
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

        private void startNode(int i) throws Exception {
            if (alive[i]) {
                return;
            }
            // the child's working directory must exist BEFORE CreateProcess
            if (!new File(root, "node" + i).exists() && !new File(root, "node" + i).mkdirs()) {
                throw new IOException("cannot create " + new File(root, "node" + i));
            }
            StringBuilder peers = new StringBuilder();
            for (int j = 0; j < NODES; j++) {
                peers.append(j > 0 ? "," : "").append("127.0.0.1:").append(ports[j]);
            }
            String javaBin = System.getProperty("java.home") + File.separator + "bin"
                    + File.separator + "java";
            ProcessBuilder pb = new ProcessBuilder(javaBin,
                    "-Xms16m", "-Xmx256m",
                    "-cp", System.getProperty("java.class.path"),
                    "com.minimq.broker.ClusterMain",
                    String.valueOf(i), String.valueOf(ports[i]),
                    storeDirs[i].getAbsolutePath(), "OS_FLUSH", peers.toString());
            pb.directory(new File(root, "node" + i));
            pb.redirectErrorStream(false);
            pb.redirectError(logs[i]);
            procs[i] = pb.start();
            alive[i] = true;

            // READY:<port> on stdout; anything else means the node never came up
            final CompletableFuture<String> ready = new CompletableFuture<>();
            final Process p = procs[i];
            Thread reader = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(p.getInputStream(), "UTF-8"));
                        String line = in.readLine();
                        ready.complete(line == null ? "<eof>" : line);
                        // keep draining so the child never blocks on a full pipe
                        while (in.readLine() != null) {
                            // discard
                        }
                    } catch (IOException e) {
                        ready.completeExceptionally(e);
                    }
                }
            }, "m3-proc-ready-" + i);
            reader.setDaemon(true);
            reader.start();
            String line = ready.get(30, TimeUnit.SECONDS);
            if (!line.startsWith("READY:")) {
                throw new AssertionError("node-" + i + " never became READY, stdout: "
                        + line + ", log tail: " + tail(i));
            }
        }

        /** REAL kill -9: TerminateProcess — no shutdown hooks, no flush. */
        void killNode(int i) {
            if (!alive[i]) {
                return;
            }
            procs[i].destroyForcibly();
            try {
                procs[i].waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            alive[i] = false;
        }

        /** Graceful: QUIT on stdin walks BrokerServer's ordered shutdown. */
        void gracefulStop(int i) throws Exception {
            if (!alive[i]) {
                return;
            }
            procs[i].getOutputStream().write("QUIT\n".getBytes(StandardCharsets.US_ASCII));
            procs[i].getOutputStream().flush();
            if (!procs[i].waitFor(20, TimeUnit.SECONDS)) {
                throw new AssertionError("node-" + i + " ignored QUIT; log tail: " + tail(i));
            }
            alive[i] = false;
        }

        /** Same storeDir, same port: the revival IS the point. */
        void restartNode(int i) throws Exception {
            killNode(i); // ensure clean slate if somehow still up
            startNode(i);
        }

        // ---- black-box observation ----

        /**
         * Probes every live port with a single-node producer: the leader
         * accepts the write; a follower answers NOT_LEADER ("not the
         * leader"); a dead node refuses the connection.
         */
        private int probeLeaderOnce() {
            for (int i = 0; i < NODES; i++) {
                if (!alive[i]) {
                    continue;
                }
                try {
                    MQProducer probe = MQProducer.connect(
                            "127.0.0.1", ports[i], 1_500, 2_048, 20);
                    try {
                        probe.send("probe", PROBE_PAYLOAD).get(2, TimeUnit.SECONDS);
                        return i;
                    } catch (Exception notLeaderOrDown) {
                        // NOT_LEADER or connect refused: try the next node
                    } finally {
                        probe.close();
                    }
                } catch (Exception connectFailed) {
                    // node down or election in flight
                }
            }
            return -1;
        }

        int awaitLeader(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int leader = probeLeaderOnce();
                if (leader >= 0) {
                    return leader;
                }
                Thread.sleep(150);
            }
            throw new AssertionError("no leader elected within " + timeoutMs
                    + "ms; logs: " + allRoles());
        }

        int awaitLeaderOtherThan(int deadId, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                int leader = probeLeaderOnce();
                if (leader >= 0 && leader != deadId) {
                    return leader;
                }
                Thread.sleep(150);
            }
            throw new AssertionError("no leader other than node-" + deadId
                    + " within " + timeoutMs + "ms; logs: " + allRoles());
        }

        /** Records elected-line counts so the next election is detectable. */
        void baselineElectedLines() throws IOException {
            for (int i = 0; i < NODES; i++) {
                electedBaseline[i] = countElectedLines(i);
            }
        }

        /**
         * Waits for a fresh {@code elected leader} line in any live node's
         * log; returns the nano-timestamp of first sight (the election
         * moment for the failover breakdown).
         */
        long awaitNewElection(long timeoutMs) throws IOException, InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < NODES; i++) {
                    if (alive[i] && countElectedLines(i) > electedBaseline[i]) {
                        return System.nanoTime();
                    }
                }
                Thread.sleep(20);
            }
            throw new AssertionError("no new election within " + timeoutMs
                    + "ms; logs: " + allRoles());
        }

        private int countElectedLines(int i) throws IOException {
            if (!logs[i].exists()) {
                return 0;
            }
            // lenient decode on purpose: the child JVM logs in the platform
            // charset (GBK on this box); the marker is pure ASCII, so
            // ISO-8859-1 never throws and still matches
            int count = 0;
            for (String line : Files.readAllLines(logs[i].toPath(), StandardCharsets.ISO_8859_1)) {
                if (line.contains(ELECTED_MARKER)) {
                    count++;
                }
            }
            return count;
        }

        private String tail(int i) {
            try {
                if (!logs[i].exists()) {
                    return "<no log>";
                }
                List<String> lines = Files.readAllLines(logs[i].toPath(), StandardCharsets.ISO_8859_1);
                StringBuilder sb = new StringBuilder();
                for (int j = Math.max(0, lines.size() - 15); j < lines.size(); j++) {
                    sb.append(lines.get(j)).append('\n');
                }
                return sb.toString();
            } catch (IOException e) {
                return "<unreadable: " + e + ">";
            }
        }

        private String allRoles() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < NODES; i++) {
                sb.append("--- node-").append(i).append(alive[i] ? "" : " (DOWN)")
                        .append(" ---\n").append(tail(i));
            }
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
            try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
                return s.getLocalPort();
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < NODES; i++) {
                if (alive[i]) {
                    procs[i].destroyForcibly();
                    alive[i] = false;
                }
            }
            for (int i = 0; i < NODES; i++) {
                try {
                    if (procs[i] != null) {
                        procs[i].waitFor(5, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
