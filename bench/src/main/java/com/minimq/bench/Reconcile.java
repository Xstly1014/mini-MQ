package com.minimq.bench;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * End-to-end reconciliation over a real in-process broker (M1 task 4):
 * the one-vote-veto reliability check — "probably fine" counts as not
 * verified, the report must be exact zeros.
 *
 * <pre>
 * ① Producer sends {@code count} messages with a global sequence number
 *    (seq: 0..count-1) plus a send timestamp baked into the payload
 * ② Consumer polls until idle (3s without new messages) or all received
 * ③ Report: lost (expected seq missing) / duplicates (seq seen more than
 *    once) / out-of-order (seq below the running max) / max end-to-end
 *    latency
 * ④ Verdict must be exactly lost=0, duplicates=0, out-of-order=0
 * </pre>
 *
 * Repeatability: every run gets a fresh temp store and offset directory and
 * a run-stamped topic, so the script can be re-run without manual cleanup.
 * Exit code 0 only on a clean PASS.
 */
public final class Reconcile {

    private static final int IDLE_TIMEOUT_MS = 3_000;
    private static final long HARD_DEADLINE_MS = 180_000;
    /**
     * Client-side local buffer cap: the send loop parks while more than this
     * many messages are un-acked (bounded heap, keeps GC noise out of the
     * latency measurement — the SDK itself never blocks).
     */
    private static final int UNACKED_WINDOW = 16_384;

    public static void main(String[] args) throws Exception {
        int count = intArg(args, 0, 100_000);
        int payloadSize = intArg(args, 1, 1024);

        Path root = Files.createTempDirectory("mini-mq-reconcile-");
        String topic = "reconcile-" + System.currentTimeMillis();
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024));
        server.start();
        MQProducer producer = null;
        MQConsumer consumer = null;
        try {
            int port = server.port();

            // ① produce
            producer = MQProducer.connect("127.0.0.1", port, 20_000, 256 * 1024, 1L);
            final CountDownLatch allAcked = new CountDownLatch(count);
            final AtomicLong acked = new AtomicLong();
            final AtomicLong failed = new AtomicLong();
            long produceStart = System.nanoTime();
            int sent = 0;
            while (sent < count) {
                if (sent - acked.get() > UNACKED_WINDOW) {
                    Thread.sleep(1);
                    continue;
                }
                final byte[] payload = payload(sent, System.nanoTime(), payloadSize);
                CompletableFuture<Long> f = producer.send(topic, payload);
                f.whenComplete((v, err) -> {
                    if (err == null) {
                        acked.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                    allAcked.countDown();
                });
                sent++;
            }
            allAcked.await();
            long produceElapsed = System.nanoTime() - produceStart;
            producer.close(); // flush + terminal state guaranteed before we consume
            producer = null;

            // ② consume until idle
            consumer = MQConsumer.subscribe("127.0.0.1", port, topic, "reconcile",
                    root.resolve("offsets").toFile(), 256, 20_000);
            int[] seqCounts = new int[count];
            Stats stats = new Stats();
            long consumeStart = System.nanoTime();
            long lastNewMessage = System.currentTimeMillis();
            long hardDeadline = lastNewMessage + HARD_DEADLINE_MS;
            while (true) {
                List<ClientRecord> batch = consumer.poll();
                if (!batch.isEmpty()) {
                    long now = System.nanoTime();
                    for (ClientRecord r : batch) {
                        verify(r, seqCounts, stats, now);
                    }
                    lastNewMessage = System.currentTimeMillis();
                }
                if (System.currentTimeMillis() - lastNewMessage >= IDLE_TIMEOUT_MS
                        || System.currentTimeMillis() >= hardDeadline) {
                    break;
                }
            }
            long consumeElapsed = System.nanoTime() - consumeStart;
            consumer.close();
            consumer = null;

            // ③ exact-zero report
            report(count, payloadSize, topic, produceElapsed, consumeElapsed,
                    failed.get(), seqCounts, stats);
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

    private static final class Stats {
        long consumed;
        long outOfOrder;
        long badSeq;
        long maxSeen = -1;
        double maxLatencyMs;
    }

    private static void verify(ClientRecord r, int[] seqCounts, Stats s, long nowNanos) {
        byte[] p = r.getPayload();
        if (p.length < 16) {
            s.badSeq++;
            return;
        }
        long seq = ByteBuffer.wrap(p, 0, 8).getLong();
        if (seq < 0 || seq >= seqCounts.length) {
            s.badSeq++;
            return;
        }
        seqCounts[(int) seq]++;
        if (seq < s.maxSeen) {
            s.outOfOrder++;
        } else {
            s.maxSeen = seq;
        }
        long sendNanos = ByteBuffer.wrap(p, 8, 8).getLong();
        double latMs = (nowNanos - sendNanos) / 1_000_000.0;
        if (latMs > s.maxLatencyMs) {
            s.maxLatencyMs = latMs;
        }
        s.consumed++;
    }

    private static void report(int count, int payloadSize, String topic,
            long produceElapsed, long consumeElapsed, long produceFailed,
            int[] seqCounts, Stats s) {
        long lost = 0;
        long dupSeqs = 0;
        long dupExcess = 0;
        for (int c : seqCounts) {
            if (c == 0) {
                lost++;
            } else if (c > 1) {
                dupSeqs++;
                dupExcess += c - 1;
            }
        }
        double produceSec = produceElapsed / 1_000_000_000.0;
        double consumeSec = consumeElapsed / 1_000_000_000.0;

        System.out.printf("# reconcile  count=%d  payload=%dB  topic=%s%n",
                count, payloadSize, topic);
        System.out.println();
        System.out.println("| item | value |");
        System.out.println("|------|-------|");
        System.out.printf("| produced (acked) | %d |%n", count - produceFailed);
        System.out.printf("| produce failures | %d |%n", produceFailed);
        System.out.printf("| consumed records | %d |%n", s.consumed);
        System.out.printf("| **lost** | **%d** |%n", lost);
        System.out.printf("| **duplicate seqs** | **%d** (excess copies: %d) |%n", dupSeqs, dupExcess);
        System.out.printf("| **out-of-order** | **%d** |%n", s.outOfOrder);
        System.out.printf("| bad payloads (seq out of range) | %d |%n", s.badSeq);
        System.out.printf("| max end-to-end latency | %.1f ms |%n", s.maxLatencyMs);
        System.out.printf("| produce elapsed | %.2f s |%n", produceSec);
        System.out.printf("| consume elapsed | %.2f s |%n", consumeSec);
        System.out.println();

        boolean pass = produceFailed == 0 && lost == 0 && dupSeqs == 0
                && s.outOfOrder == 0 && s.badSeq == 0 && s.consumed == count;
        System.out.println("VERDICT: " + (pass ? "PASS" : "FAIL")
                + "  (lost=0 / duplicates=0 / out-of-order=0 required)");
        if (!pass) {
            System.exit(1);
        }
    }

    /** [8B seq][8B sendNanos][filler] — same JVM clock on both ends. */
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

    private Reconcile() {
    }
}
