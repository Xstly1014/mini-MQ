package com.minimq.bench;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;

/**
 * End-to-end baseline benchmark (M1 task 4): how fast is the FULL path —
 * SDK batching, proto encode, loopback TCP, broker decode, business pool,
 * CommitLog append, ack, and the pull path back — now that M0 proved the
 * bare storage fast? These numbers are the control group for M2 group
 * commit.
 *
 * Methodology (M1 manual): fixed 1KB payload, one producer one consumer,
 * 1 warmup round discarded, 5 measured rounds, take the median. Three
 * numbers per round:
 * <ol>
 *   <li><b>End-to-end produce throughput</b> — N / (first send .. last ack),
 *       i.e. the closed loop send→append→ack;</li>
 *   <li><b>Producer send latency</b> — per-message send()→callback P50/P99/
 *       max, including batch-queue wait (that is what the caller feels);</li>
 *   <li><b>Consume throughput</b> — N / (first poll .. last record), pull
 *       view, plus max end-to-end send→consumed latency.</li>
 * </ol>
 *
 * Every round boots a fresh broker on a fresh store so rounds are
 * independent and the disk footprint stays bounded. Exit code 1 if the
 * median end-to-end throughput is below the 10k msg/s gate.
 */
public final class BenchV1 {

    private static final int ROUNDS = 5;
    private static final int WARMUP_ROUNDS = 1;
    private static final long PRODUCE_GATE_MSGS_PER_SEC = 10_000;
    private static final int IDLE_TIMEOUT_MS = 3_000;
    private static final long HARD_DEADLINE_MS = 120_000;
    private static final int UNACKED_WINDOW = 16_384;

    public static void main(String[] args) throws Exception {
        int msgsPerRound = intArg(args, 0, 50_000);
        int payloadSize = intArg(args, 1, 1024);

        System.out.printf("# mini-MQ bench-v1  msgs/round=%d  payload=%dB  rounds=%d+%d warmup  loopback%n",
                msgsPerRound, payloadSize, ROUNDS, WARMUP_ROUNDS);
        System.out.println();
        System.out.println("| round | produce msg/s | send P50 (ms) | send P99 (ms) | send max (ms) | consume msg/s | max e2e (ms) |");
        System.out.println("|-------|---------------|---------------|---------------|---------------|---------------|--------------|");

        long[] produceThroughputs = new long[ROUNDS];
        long[] consumeThroughputs = new long[ROUNDS];
        double[] p50s = new double[ROUNDS];
        double[] p99s = new double[ROUNDS];
        double[] maxs = new double[ROUNDS];
        double[] e2eMaxs = new double[ROUNDS];

        for (int round = 0; round < WARMUP_ROUNDS + ROUNDS; round++) {
            RoundResult r = runRound("bench-r" + round, msgsPerRound, payloadSize);
            if (round < WARMUP_ROUNDS) {
                System.out.printf("| warmup | %,d | %.1f | %.1f | %.1f | %,d | %.1f |%n",
                        r.produceMsgsPerSec, r.sendP50Ms, r.sendP99Ms, r.sendMaxMs,
                        r.consumeMsgsPerSec, r.maxE2eMs);
            } else {
                int i = round - WARMUP_ROUNDS;
                produceThroughputs[i] = r.produceMsgsPerSec;
                consumeThroughputs[i] = r.consumeMsgsPerSec;
                p50s[i] = r.sendP50Ms;
                p99s[i] = r.sendP99Ms;
                maxs[i] = r.sendMaxMs;
                e2eMaxs[i] = r.maxE2eMs;
                System.out.printf("| %d | %,d | %.1f | %.1f | %.1f | %,d | %.1f |%n",
                        i + 1, r.produceMsgsPerSec, r.sendP50Ms, r.sendP99Ms, r.sendMaxMs,
                        r.consumeMsgsPerSec, r.maxE2eMs);
            }
        }

        System.out.println();
        System.out.println("## medians over " + ROUNDS + " measured rounds");
        System.out.println();
        System.out.println("| metric | median |");
        System.out.println("|--------|--------|");
        System.out.printf("| end-to-end produce throughput | %,d msg/s |%n", median(produceThroughputs));
        System.out.printf("| producer send P50 | %.1f ms |%n", medianD(p50s));
        System.out.printf("| producer send P99 | %.1f ms |%n", medianD(p99s));
        System.out.printf("| producer send max | %.1f ms |%n", medianD(maxs));
        System.out.printf("| consume throughput | %,d msg/s |%n", median(consumeThroughputs));
        System.out.printf("| max end-to-end latency | %.1f ms |%n", medianD(e2eMaxs));

        long medianProduce = median(produceThroughputs);
        System.out.println();
        if (medianProduce >= PRODUCE_GATE_MSGS_PER_SEC) {
            System.out.printf("GATE: PASS (median produce throughput %,d >= %,d msg/s)%n",
                    medianProduce, PRODUCE_GATE_MSGS_PER_SEC);
        } else {
            System.out.printf("GATE: FAIL (median produce throughput %,d < %,d msg/s)%n",
                    medianProduce, PRODUCE_GATE_MSGS_PER_SEC);
            System.exit(1);
        }
    }

    private static final class RoundResult {
        long produceMsgsPerSec;
        long consumeMsgsPerSec;
        double sendP50Ms;
        double sendP99Ms;
        double sendMaxMs;
        double maxE2eMs;
    }

    private static RoundResult runRound(String topic, int count, int payloadSize) throws Exception {
        Path root = Files.createTempDirectory("mini-mq-bench-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024));
        server.start();
        MQProducer producer = null;
        MQConsumer consumer = null;
        try {
            int port = server.port();
            RoundResult r = new RoundResult();

            // ---- produce: N sends, per-message send->ack latency ----
            producer = MQProducer.connect("127.0.0.1", port, 20_000, 256 * 1024, 1L);
            final LatencyRecorder latencies = new LatencyRecorder(count);
            final CountDownLatch allAcked = new CountDownLatch(count);
            final AtomicLong acked = new AtomicLong();
            long produceStart = System.nanoTime();
            int sent = 0;
            while (sent < count) {
                if (sent - acked.get() > UNACKED_WINDOW) {
                    Thread.sleep(1);
                    continue;
                }
                final long t0 = System.nanoTime();
                final byte[] payload = payload(sent, t0, payloadSize);
                CompletableFuture<Long> f = producer.send(topic, payload);
                f.whenComplete((v, err) -> {
                    if (err == null) {
                        latencies.add(System.nanoTime() - t0);
                        acked.incrementAndGet();
                    }
                    allAcked.countDown();
                });
                sent++;
            }
            allAcked.await();
            long produceElapsed = System.nanoTime() - produceStart;
            producer.close();
            producer = null;

            long[] sorted = latencies.snapshotSorted();
            r.produceMsgsPerSec = msgsPerSec(count, produceElapsed);
            r.sendP50Ms = percentileMs(sorted, 0.50);
            r.sendP99Ms = percentileMs(sorted, 0.99);
            r.sendMaxMs = sorted.length == 0 ? 0 : sorted[sorted.length - 1] / 1_000_000.0;

            // ---- consume: poll until idle or all expected records ----
            consumer = MQConsumer.subscribe("127.0.0.1", port, topic, "bench",
                    root.resolve("offsets").toFile(), 256, 20_000);
            long consumeStart = System.nanoTime();
            long received = 0;
            double maxE2e = 0;
            long lastProgress = System.currentTimeMillis();
            long hardDeadline = lastProgress + HARD_DEADLINE_MS;
            while (received < count && System.currentTimeMillis() < hardDeadline) {
                List<ClientRecord> batch = consumer.poll();
                if (batch.isEmpty()) {
                    // everything is expected to be durable already (producer
                    // closed): 3s without progress means records are MISSING,
                    // not slow — fail fast and point at Reconcile.
                    if (System.currentTimeMillis() - lastProgress >= IDLE_TIMEOUT_MS) {
                        break;
                    }
                    continue;
                }
                long now = System.nanoTime();
                for (ClientRecord rec : batch) {
                    byte[] p = rec.getPayload();
                    if (p.length >= 16) {
                        double ms = (now - ByteBuffer.wrap(p, 8, 8).getLong()) / 1_000_000.0;
                        if (ms > maxE2e) {
                            maxE2e = ms;
                        }
                    }
                }
                received += batch.size();
                lastProgress = System.currentTimeMillis();
            }
            long consumeElapsed = System.nanoTime() - consumeStart;
            consumer.close();
            consumer = null;

            if (received != count) {
                throw new IllegalStateException("round " + topic + ": consumed " + received
                        + " != produced " + count + " — run Reconcile for the failure report");
            }
            r.consumeMsgsPerSec = msgsPerSec(count, consumeElapsed);
            r.maxE2eMs = maxE2e;
            return r;
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }
            server.shutdown();
            cleanupQuietly(root);
        }
    }

    /** synchronized is fine here: 50k uncontended adds per round, nanos each. */
    private static final class LatencyRecorder {
        private final long[] ns;
        private int n;

        LatencyRecorder(int capacity) {
            this.ns = new long[capacity];
        }

        synchronized void add(long v) {
            ns[n++] = v;
        }

        synchronized long[] snapshotSorted() {
            long[] out = Arrays.copyOf(ns, n);
            Arrays.sort(out);
            return out;
        }
    }

    private static double percentileMs(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(q * sorted.length) - 1;
        if (idx < 0) {
            idx = 0;
        }
        return sorted[idx] / 1_000_000.0;
    }

    private static long msgsPerSec(long msgs, long elapsedNanos) {
        return (long) (msgs / (elapsedNanos / 1_000_000_000.0));
    }

    private static long median(long[] v) {
        long[] s = v.clone();
        Arrays.sort(s);
        return s[s.length / 2];
    }

    private static double medianD(double[] v) {
        double[] s = v.clone();
        Arrays.sort(s);
        return s[s.length / 2];
    }

    /** [8B seq][8B sendNanos][filler]. */
    private static byte[] payload(long seq, long sendNanos, int size) {
        byte[] p = new byte[size];
        ByteBuffer.wrap(p).putLong(seq).putLong(sendNanos);
        for (int i = 16; i < size; i++) {
            p[i] = 'x';
        }
        return p;
    }

    private static void cleanupQuietly(Path root) {
        try {
            Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException ignored) {
            System.err.println("note: temp dir left behind: " + root);
        }
    }

    private static int intArg(String[] args, int idx, int dflt) {
        if (args == null || args.length <= idx) {
            return dflt;
        }
        return Integer.parseInt(args[idx]);
    }

    private BenchV1() {
    }
}
