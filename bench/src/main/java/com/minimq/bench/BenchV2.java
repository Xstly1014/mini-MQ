package com.minimq.bench;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;
import com.minimq.storage.FlushMode;

/**
 * M2 task 4 benchmark suite: the three experiments that quantify the M2
 * mechanisms against the M1 baseline (docs/bench-v1.md).
 *
 * <ol>
 *   <li><b>Group-commit matrix</b> (SYNC_FLUSH): linger x batchCount grid —
 *       how each aggregation window trades latency for force amortization.
 *       The observable is msgs/force-sweep: the group size the window buys.</li>
 *   <li><b>Concurrency curve</b>: 1..16 producers under OS_FLUSH (M1 shape)
 *       vs SYNC_FLUSH — the counter-intuitive group-commit property is that
 *       SYNC throughput RISES with concurrency (busier window = bigger
 *       groups per force), while OS_FLUSH just saturates.</li>
 *   <li><b>Zero-copy A/B</b>: same workload, zeroCopyFetch on/off — consume
 *       throughput and process CPU time; sendfile should cut the broker's
 *       user-space bytes materially.</li>
 * </ol>
 *
 * Every cell boots a fresh broker on a fresh store (BenchV1 methodology:
 * independent cells, single-variable deltas). Usage:
 * {@code BenchV2 [msgsPerCell=20000] [payload=1024]}.
 */
public final class BenchV2 {

    private static final long[] LINGERS_US = {0, 200, 500, 1000, 2000};
    private static final int[] BATCH_COUNTS = {16, 64, 256, 1024};
    private static final int[] PRODUCER_COUNTS = {1, 2, 4, 8, 16};
    private static final int UNACKED_WINDOW_PER_PRODUCER = 4_096;
    private static final long IDLE_TIMEOUT_MS = 5_000;

    public static void main(String[] args) throws Exception {
        int msgsPerCell = intArg(args, 0, 20_000);
        int payloadSize = intArg(args, 1, 1024);
        String which = args != null && args.length > 2 ? args[2] : "all";

        System.out.printf("# mini-MQ bench-v2  msgs/cell=%d  payload=%dB  loopback (WSL2)%n",
                msgsPerCell, payloadSize);
        if (which.equals("all") || which.equals("1")) {
            experiment1GroupCommitMatrix(msgsPerCell, payloadSize);
        }
        if (which.equals("all") || which.equals("2")) {
            experiment2ConcurrencyCurve(msgsPerCell, payloadSize);
        }
        if (which.equals("all") || which.equals("3")) {
            experiment3ZeroCopyAB(Math.max(msgsPerCell, 50_000), payloadSize);
        }
        System.out.println("\n# all experiments done");
    }

    // ------------------------------------------------------------------
    // experiment 1: linger x batchCount matrix under SYNC_FLUSH
    // ------------------------------------------------------------------

    private static void experiment1GroupCommitMatrix(int msgs, int payloadSize) throws Exception {
        System.out.println();
        System.out.println("## experiment 1: group-commit matrix (SYNC_FLUSH, 1 producer)");
        System.out.println();
        ProduceResult[][] grid = new ProduceResult[LINGERS_US.length][BATCH_COUNTS.length];
        for (int li = 0; li < LINGERS_US.length; li++) {
            for (int ci = 0; ci < BATCH_COUNTS.length; ci++) {
                grid[li][ci] = produceRound("mtx", msgs, payloadSize, 1,
                        FlushMode.SYNC_FLUSH, LINGERS_US[li], BATCH_COUNTS[ci], false);
            }
        }
        // table A: throughput
        System.out.println("### throughput (msg/s)");
        System.out.println();
        System.out.println("| linger(us) \\ count | " + joinCounts() + " |");
        System.out.println("|--------------------|" + repeat("--------:|", BATCH_COUNTS.length));
        long bestThroughput = -1;
        String bestCell = "-";
        for (int li = 0; li < LINGERS_US.length; li++) {
            StringBuilder row = new StringBuilder(String.format("| %18d | ", LINGERS_US[li]));
            for (int ci = 0; ci < BATCH_COUNTS.length; ci++) {
                ProduceResult r = grid[li][ci];
                row.append(String.format("%,8d |", r.msgsPerSec));
                if (r.msgsPerSec > bestThroughput) {
                    bestThroughput = r.msgsPerSec;
                    bestCell = "linger=" + LINGERS_US[li] + "us,count=" + BATCH_COUNTS[ci];
                }
            }
            System.out.println(row);
        }
        System.out.println();
        System.out.println("best cell: " + bestCell + " -> " + String.format("%,d", bestThroughput) + " msg/s");
        // table B: send latency percentiles (the P99 knob view — DoD k02)
        System.out.println();
        System.out.println("### send latency P50 / P99 / max (ms)");
        System.out.println();
        System.out.println("| linger(us) \\ count | " + joinCounts() + " |");
        System.out.println("|--------------------|" + repeat("---------------:|", BATCH_COUNTS.length));
        for (int li = 0; li < LINGERS_US.length; li++) {
            StringBuilder row = new StringBuilder(String.format("| %18d | ", LINGERS_US[li]));
            for (int ci = 0; ci < BATCH_COUNTS.length; ci++) {
                ProduceResult r = grid[li][ci];
                row.append(String.format("%2d/%2d/%3d |", r.p50SendMs, r.p99SendMs, r.maxSendMs));
            }
            System.out.println(row);
        }
        System.out.println();
        // table C: aggregation (msgs per force sweep)
        System.out.println("### msgs/force (aggregation the window buys)");
        System.out.println();
        System.out.println("| linger(us) \\ count | " + joinCounts() + " |");
        System.out.println("|--------------------|" + repeat("--------:|", BATCH_COUNTS.length));
        for (int li = 0; li < LINGERS_US.length; li++) {
            StringBuilder row = new StringBuilder(String.format("| %18d | ", LINGERS_US[li]));
            for (int ci = 0; ci < BATCH_COUNTS.length; ci++) {
                ProduceResult r = grid[li][ci];
                double groupSize = r.sweeps > 0 ? (double) msgs / r.sweeps : 0;
                row.append(String.format("%,8.1f |", groupSize));
            }
            System.out.println(row);
        }
    }

    // ------------------------------------------------------------------
    // experiment 2: concurrency curve, OS_FLUSH vs SYNC_FLUSH
    // ------------------------------------------------------------------

    private static void experiment2ConcurrencyCurve(int msgs, int payloadSize) throws Exception {
        System.out.println();
        System.out.println("## experiment 2: concurrency curve (OS_FLUSH vs SYNC_FLUSH 500us/64)");
        System.out.println();
        System.out.println("| producers | OS_FLUSH msg/s | SYNC_FLUSH msg/s | SYNC msgs/force | SYNC P50/P99 (ms) |");
        System.out.println("|----------:|---------------:|-----------------:|----------------:|------------------:|");
        for (int producers : PRODUCER_COUNTS) {
            ProduceResult os = produceRound("cc-os", msgs, payloadSize, producers,
                    FlushMode.OS_FLUSH, 0, 1, false);
            ProduceResult sync = produceRound("cc-sync", msgs, payloadSize, producers,
                    FlushMode.SYNC_FLUSH, 500, 64, false);
            double groupSize = sync.sweeps > 0 ? (double) msgs / sync.sweeps : 0;
            System.out.printf("| %9d | %,14d | %,16d | %15.1f | %d/%d |%n",
                    producers, os.msgsPerSec, sync.msgsPerSec, groupSize,
                    sync.p50SendMs, sync.p99SendMs);
        }
    }

    // ------------------------------------------------------------------
    // experiment 3: zero-copy fetch A/B
    // ------------------------------------------------------------------

    private static void experiment3ZeroCopyAB(int msgs, int payloadSize) throws Exception {
        System.out.println();
        System.out.println("## experiment 3: zero-copy fetch A/B (1 producer, 1 consumer, OS_FLUSH)");
        System.out.println();
        System.out.println("3 rounds per path, median reported (single-cell variance on this");
        System.out.println("loopback setup was measured at ~75% between runs — medians only).");
        System.out.println();
        final int ROUNDS = 3;
        long[][] throughput = new long[2][ROUNDS]; // [0]=inline, [1]=sendfile
        long[][] cpuUsPerMsg = new long[2][ROUNDS];
        for (int round = 0; round < ROUNDS; round++) {
            for (int path = 0; path < 2; path++) {
                boolean zeroCopy = path == 1;
                ConsumeResult r = consumeRound("zc-" + zeroCopy + "-r" + round, msgs, payloadSize, zeroCopy);
                throughput[path][round] = r.msgsPerSec;
                cpuUsPerMsg[path][round] = (long) (r.cpuMs * 1000.0 / msgs);
            }
        }
        System.out.println("| fetch path | rounds (msg/s) | median msg/s | CPU us/msg |");
        System.out.println("|------------|----------------|-------------:|-----------:|");
        for (int path = 0; path < 2; path++) {
            String name = path == 1 ? "sendfile" : "inline";
            java.util.Arrays.sort(throughput[path]);
            java.util.Arrays.sort(cpuUsPerMsg[path]);
            System.out.printf("| %10s | %d / %d / %d | %,12d | %10.2f |%n",
                    name,
                    throughput[path][0], throughput[path][1], throughput[path][2],
                    throughput[path][1],
                    cpuUsPerMsg[path][1] / 1.0);
        }
    }

    // ------------------------------------------------------------------
    // harness
    // ------------------------------------------------------------------

    private static final class ProduceResult {
        long msgsPerSec;
        long sweeps;
        long p50SendMs;
        long p99SendMs;
        long maxSendMs;
    }

    /** One fresh-broker cell: N producers, all acked, throughput + sweeps + send latency. */
    private static ProduceResult produceRound(String tag, int msgs, int payloadSize, int producers,
            FlushMode mode, long lingerUs, int batchCount, boolean zeroCopy) throws Exception {
        Path root = Files.createTempDirectory("mini-mq-v2-" + tag + "-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                .flushMode(mode)
                .flushLingerUs(lingerUs)
                .flushBatchCount(batchCount)
                .zeroCopyFetch(zeroCopy));
        server.start();
        ProduceResult result = new ProduceResult();
        try {
            long start = System.nanoTime();
            List<Long> latenciesMs = runProducers(server.port(), "bench", msgs, payloadSize, producers);
            long tookMs = Math.max(1, (System.nanoTime() - start) / 1_000_000);
            result.msgsPerSec = msgs * 1000L / tookMs;
            fillPercentiles(result, latenciesMs);
            if (server.flushService() != null) {
                result.sweeps = server.flushService().forceSweeps();
            }
        } finally {
            server.shutdown();
            cleanupQuietly(root);
        }
        return result;
    }

    /** P50/P99/max of the ack latency distribution (send() to future complete). */
    private static void fillPercentiles(ProduceResult r, List<Long> latenciesMs) {
        if (latenciesMs.isEmpty()) {
            return;
        }
        long[] a = new long[latenciesMs.size()];
        for (int i = 0; i < a.length; i++) {
            a[i] = latenciesMs.get(i);
        }
        java.util.Arrays.sort(a);
        r.p50SendMs = a[(int) ((a.length - 1) * 0.50)];
        r.p99SendMs = a[(int) ((a.length - 1) * 0.99)];
        r.maxSendMs = a[a.length - 1];
    }

    private static List<Long> runProducers(int port, String topic, int msgs, int payloadSize, int producers)
            throws Exception {
        final int perProducer = msgs / producers;
        final int remainder = msgs - perProducer * producers;
        final CountDownLatch done = new CountDownLatch(producers);
        final java.util.concurrent.ConcurrentLinkedQueue<Long> latenciesMs =
                new java.util.concurrent.ConcurrentLinkedQueue<Long>();
        for (int p = 0; p < producers; p++) {
            final int mine = perProducer + (p == 0 ? remainder : 0);
            final int seed = p;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQProducer producer = MQProducer.connect("127.0.0.1", port,
                                20_000, 256 * 1024, 1L);
                        try {
                            List<CompletableFuture<Long>> window =
                                    new ArrayList<CompletableFuture<Long>>(UNACKED_WINDOW_PER_PRODUCER);
                            for (int i = 0; i < mine; i++) {
                                if (window.size() >= UNACKED_WINDOW_PER_PRODUCER) {
                                    window.get(0).get(20, TimeUnit.SECONDS);
                                    window.clear();
                                }
                                final long t0 = System.nanoTime();
                                CompletableFuture<Long> f = producer.send(topic,
                                        BenchV1Payload.payload(seed * 1_000_000L + i,
                                                System.nanoTime(), payloadSize));
                                f.whenComplete((v, err) -> {
                                    if (err == null) {
                                        latenciesMs.add((System.nanoTime() - t0) / 1_000_000);
                                    }
                                });
                                window.add(f);
                            }
                            for (CompletableFuture<Long> f : window) {
                                f.get(20, TimeUnit.SECONDS);
                            }
                        } finally {
                            producer.close();
                        }
                        done.countDown();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, "bench-producer-" + p);
            t.start();
        }
        if (!done.await(300, TimeUnit.SECONDS)) {
            throw new IllegalStateException("producers timed out");
        }
        return new ArrayList<Long>(latenciesMs);
    }

    private static final class ConsumeResult {
        long msgsPerSec;
        long cpuMs;
    }

    /** Fresh broker, produce once, then consume all: consume throughput + CPU. */
    private static ConsumeResult consumeRound(String tag, int msgs, int payloadSize, boolean zeroCopy)
            throws Exception {
        Path root = Files.createTempDirectory("mini-mq-v2-" + tag + "-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                .flushMode(FlushMode.OS_FLUSH)
                .zeroCopyFetch(zeroCopy));
        server.start();
        ConsumeResult result = new ConsumeResult();
        try {
            runProducers(server.port(), "bench", msgs, payloadSize, 1);

            com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean();
            MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", server.port(),
                    "bench", "g", root.resolve("offsets").toFile(), 256, 20_000);
            try {
                long cpuStart = os.getProcessCpuTime();
                long start = System.nanoTime();
                long received = 0;
                long lastProgress = System.currentTimeMillis();
                while (received < msgs) {
                    List<ClientRecord> batch = consumer.poll();
                    if (batch.isEmpty()) {
                        if (System.currentTimeMillis() - lastProgress >= IDLE_TIMEOUT_MS) {
                            throw new IllegalStateException("consume stalled at " + received);
                        }
                        continue;
                    }
                    // verify order: the payload's first 8 bytes are the seq
                    for (ClientRecord rec : batch) {
                        long seq = seqOf(rec.getPayload());
                        if (seq != received) {
                            throw new IllegalStateException("out of order: " + seq + " at " + received);
                        }
                        received++;
                    }
                    lastProgress = System.currentTimeMillis();
                }
                long tookMs = Math.max(1, (System.nanoTime() - start) / 1_000_000);
                result.msgsPerSec = msgs * 1000L / tookMs;
                result.cpuMs = (os.getProcessCpuTime() - cpuStart) / 1_000_000;
            } finally {
                consumer.close();
            }
        } finally {
            server.shutdown();
            cleanupQuietly(root);
        }
        return result;
    }

    /** [8B seq][8B sendNanos][filler] — see BenchV1Payload. */
    private static long seqOf(byte[] payload) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(payload);
        return buf.getLong(0);
    }

    private static String joinCounts() {
        StringBuilder sb = new StringBuilder();
        for (int c : BATCH_COUNTS) {
            sb.append(c).append(" | ");
        }
        return sb.toString().trim();
    }

    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    private static void cleanupQuietly(Path root) {
        try {
            Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (java.io.IOException ignored) {
            System.err.println("note: temp dir left behind: " + root);
        }
    }

    private static int intArg(String[] args, int idx, int dflt) {
        return args != null && args.length > idx ? Integer.parseInt(args[idx]) : dflt;
    }

    /** Payload shape shared with BenchV1: [8B seq][8B sendNanos][filler]. */
    static final class BenchV1Payload {
        static byte[] payload(long seq, long sendNanos, int size) {
            byte[] p = new byte[size];
            java.nio.ByteBuffer.wrap(p).putLong(seq).putLong(sendNanos);
            java.util.Arrays.fill(p, 16, size, (byte) 'x');
            return p;
        }
    }

    private BenchV2() {
    }
}
