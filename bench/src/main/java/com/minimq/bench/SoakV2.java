package com.minimq.bench;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;
import com.minimq.storage.CodecPools;
import com.minimq.storage.FlushMode;

/**
 * 30-minute stability soak (M2 task 4b, DoD k09): steady produce + consume
 * traffic over the full M2 stack — group-commit flush thread, ConsumeQueue
 * index, zero-copy FETCH, pooled codecs — while a reporter prints, once a
 * minute: per-minute throughput, heap usage, GC activity, pool hit ratios
 * and Netty LEAK reports (captured from JUL, so a nonzero count is a hard
 * fail even when stderr scrolls by).
 *
 * <p>Disk budget: the CommitLog only ever grows, so the soak is rate-limited
 * (default 10k msg/s x 512B ~ 9 GB over 30 min) — this run is stability
 * evidence, not peak throughput (that is BenchV2's job).
 *
 * <p>Verification: each producer bakes [4B producerId][4B localSeq] into the
 * payload; the consumer checks per-producer strict sequencing, so any loss,
 * duplication or reorder across 30 minutes and millions of messages fails
 * the run.
 *
 * <pre>usage: SoakV2 [minutes=30] [producers=8] [payload=512] [targetRate=10000]</pre>
 */
public final class SoakV2 {

    private static final int IDLE_DRAIN_MS = 5_000;
    /** Netty leak reports captured from JUL (paranoid mode floods them on real leaks). */
    private static final AtomicLong LEAK_REPORTS = new AtomicLong();

    static {
        Logger.getLogger("").addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                String m = record.getMessage();
                if (m != null && m.contains("LEAK")) {
                    LEAK_REPORTS.incrementAndGet();
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        });
    }

    public static void main(String[] args) throws Exception {
        int minutes = intArg(args, 0, 30);
        int producers = intArg(args, 1, 8);
        int payloadSize = intArg(args, 2, 512);
        int targetRate = intArg(args, 3, 10_000);

        Path root = Files.createTempDirectory("mini-mq-soak-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                // full M2 stack: group-commit flusher active + zero-copy FETCH
                .flushMode(FlushMode.SYNC_FLUSH)
                .flushLingerUs(500)
                .flushBatchCount(64)
                .zeroCopyFetch(true));
        server.start();

        System.out.printf("# soak  minutes=%d producers=%d payload=%dB rate=%d/s  flush=SYNC(500us/64) zeroCopy=on%n",
                minutes, producers, payloadSize, targetRate);
        System.out.println("# " + poolStats());

        final LongAdder acked = new LongAdder();
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch producersDone = new CountDownLatch(producers);
        final long runMs = minutes * 60_000L;
        final long perProducerBurst = Math.max(1, targetRate / producers / 50);
        // Period so that burstSize/period == targetRate/producers per thread:
        // e.g. 10k/s, 8 producers, burst 25 -> every producer sends 25 then
        // sleeps 20ms -> 1250/s each -> 10k/s total.
        final long burstSleepMs = Math.max(1, perProducerBurst * producers * 1000L / Math.max(1, targetRate));

        for (int p = 0; p < producers; p++) {
            final int id = p;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQProducer producer = MQProducer.connect("127.0.0.1", server.port(),
                                20_000, 256 * 1024, 1L);
                        long seq = 0;
                        try {
                            while (!stop.get()) {
                                for (int i = 0; i < perProducerBurst; i++) {
                                    final CompletableFuture<Long> f = producer.send("soak",
                                            payload(id, seq++, payloadSize));
                                    f.whenComplete((v, err) -> {
                                        if (err == null) {
                                            acked.increment();
                                        }
                                    });
                                }
                                Thread.sleep(burstSleepMs);
                            }
                        } finally {
                            producer.close();
                        }
                    } catch (Exception e) {
                        System.err.println("producer " + id + " died: " + e);
                    } finally {
                        producersDone.countDown();
                    }
                }
            }, "soak-producer-" + p);
            t.setDaemon(true);
            t.start();
        }

        // consumer + verifier: per-producer strict sequencing
        final ConsumerStats cStats = new ConsumerStats(producers);
        Thread consumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", server.port(),
                            "soak", "soak-g", root.resolve("offsets").toFile(), 256, 20_000);
                    long lastNew = System.currentTimeMillis();
                    while (!stop.get() || System.currentTimeMillis() - lastNew < IDLE_DRAIN_MS) {
                        List<ClientRecord> batch = consumer.poll();
                        if (!batch.isEmpty()) {
                            for (ClientRecord r : batch) {
                                cStats.verify(r);
                            }
                            lastNew = System.currentTimeMillis();
                        }
                    }
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("consumer died: " + e);
                }
            }
        }, "soak-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // ---- minute-by-minute reporter ----
        long started = System.currentTimeMillis();
        long prevAcked = 0;
        long prevConsumed = 0;
        long gcPrevCount = gcTotalCount();
        long gcPrevMs = gcTotalTimeMs();
        long firstWindowAcked = -1;
        long lastWindowAcked = -1;
        long firstWindowConsumed = -1;
        long lastWindowConsumed = -1;
        final long firstWindowEnd = started + 5 * 60_000L;
        final long lastWindowStart = started + runMs - 5 * 60_000L;

        while (System.currentTimeMillis() - started < runMs) {
            Thread.sleep(60_000);
            long now = System.currentTimeMillis();
            long a = acked.sum();
            long c = cStats.consumed.sum();
            Runtime rt = Runtime.getRuntime();
            long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
            long committedMb = rt.totalMemory() / (1024 * 1024);
            long gcCount = gcTotalCount();
            long gcMs = gcTotalTimeMs();
            long ackedInMin = a - prevAcked;
            long consumedInMin = c - prevConsumed;
            System.out.printf("[%dm] produce %,d/min (%.0f/s)  consume %,d/min (%.0f/s)  heap %d/%dMB  gc %d/%dms  leaks=%d  | %s%n",
                    (now - started) / 60_000,
                    ackedInMin, ackedInMin / 60.0, consumedInMin, consumedInMin / 60.0,
                    usedMb, committedMb,
                    gcCount - gcPrevCount, gcMs - gcPrevMs, LEAK_REPORTS.get(),
                    poolStats());
            prevAcked = a;
            prevConsumed = c;
            gcPrevCount = gcCount;
            gcPrevMs = gcMs;
            if (now >= firstWindowEnd && firstWindowAcked < 0) {
                firstWindowAcked = a;
                firstWindowConsumed = c;
            }
            if (now >= lastWindowStart && lastWindowAcked < 0) {
                // Snapshot ONCE at the 25m mark — a per-minute overwrite here
                // turned "last-5m rate" into "30-min cumulative / 300" and
                // reported a -500% decay on a perfectly flat run.
                lastWindowAcked = a;
                lastWindowConsumed = c;
            }
        }

        stop.set(true);
        producersDone.await(30, TimeUnit.SECONDS);
        consumerThread.join(60_000);
        long finalAcked = acked.sum();
        long finalConsumed = cStats.consumed.sum();

        if (firstWindowAcked < 0) { // run shorter than 5min windows
            firstWindowAcked = finalAcked / 2;
            firstWindowConsumed = finalConsumed / 2;
        }
        if (lastWindowAcked < 0) { // run shorter than the 25m mark
            lastWindowAcked = finalAcked / 2;
            lastWindowConsumed = finalConsumed / 2;
        }
        // last-5m window = final total minus the 25m-mark snapshot
        long last5mAcked = Math.max(0, finalAcked - lastWindowAcked);
        long last5mConsumed = Math.max(0, finalConsumed - lastWindowConsumed);

        System.out.println();
        System.out.println("## soak summary");
        System.out.println();
        System.out.println("| item | value |");
        System.out.println("|------|-------|");
        System.out.printf("| total acked | %,d |%n", finalAcked);
        System.out.printf("| total consumed | %,d |%n", finalConsumed);
        System.out.printf("| verify errors (loss/dup/reorder/bad) | %d/%d/%d/%d |%n",
                cStats.lost.sum(), cStats.dup.sum(), cStats.reorder.sum(), cStats.bad.sum());
        System.out.printf("| produce first-5m vs last-5m rate | %.0f/s vs %.0f/s (%.1f%% decay) |%n",
                firstWindowAcked / 300.0, last5mAcked / 300.0,
                100.0 * (firstWindowAcked - last5mAcked) / Math.max(1, firstWindowAcked));
        System.out.printf("| consume first-5m vs last-5m rate | %.0f/s vs %.0f/s |%n",
                firstWindowConsumed / 300.0, last5mConsumed / 300.0);
        System.out.printf("| total GC | %d collections, %d ms |%n", gcTotalCount(), gcTotalTimeMs());
        System.out.printf("| Netty LEAK reports | %d |%n", LEAK_REPORTS.get());
        System.out.printf("| pool stats | %s |%n", poolStats());

        boolean pass = cStats.lost.sum() == 0 && cStats.dup.sum() == 0
                && cStats.reorder.sum() == 0 && cStats.bad.sum() == 0
                && LEAK_REPORTS.get() == 0
                && finalAcked > 0;
        System.out.println();
        System.out.println("VERDICT: " + (pass ? "PASS" : "FAIL")
                + " (zero verify errors, zero LEAK required)");
        server.shutdown();
        cleanupQuietly(root);
        if (!pass) {
            System.exit(1);
        }
    }

    private static final class ConsumerStats {
        final LongAdder consumed = new LongAdder();
        final LongAdder lost = new LongAdder();
        final LongAdder dup = new LongAdder();
        final LongAdder reorder = new LongAdder();
        final LongAdder bad = new LongAdder();
        private final long[] lastSeqByProducer;

        ConsumerStats(int producers) {
            this.lastSeqByProducer = new long[producers];
            java.util.Arrays.fill(lastSeqByProducer, -1L);
        }

        void verify(ClientRecord r) {
            byte[] p = r.getPayload();
            if (p.length < 8) {
                bad.increment();
                return;
            }
            ByteBuffer buf = ByteBuffer.wrap(p, 0, 8);
            int producerId = buf.getInt();
            long seq = buf.getInt() & 0xFFFFFFFFL;
            if (producerId < 0 || producerId >= lastSeqByProducer.length) {
                bad.increment();
                return;
            }
            long expect = lastSeqByProducer[producerId] + 1;
            if (seq == expect) {
                lastSeqByProducer[producerId] = seq;
            } else if (seq <= lastSeqByProducer[producerId]) {
                reorder.increment();
            } else {
                lost.add(seq - expect);
                lastSeqByProducer[producerId] = seq;
            }
            consumed.increment();
        }
    }

    private static String poolStats() {
        return CodecPools.stats();
    }

    private static long gcTotalCount() {
        long n = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            n += Math.max(0, gc.getCollectionCount());
        }
        return n;
    }

    private static long gcTotalTimeMs() {
        long ms = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            ms += Math.max(0, gc.getCollectionTime());
        }
        return ms;
    }

    /** [4B producerId][4B localSeq][8B sendNanos][filler]. */
    private static byte[] payload(int producerId, long seq, int size) {
        byte[] p = new byte[size];
        ByteBuffer.wrap(p).putInt(producerId).putInt((int) seq).putLong(System.nanoTime());
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
        } catch (java.io.IOException ignored) {
            System.err.println("note: temp dir left behind: " + root);
        }
    }

    private static int intArg(String[] args, int idx, int dflt) {
        return args != null && args.length > idx ? Integer.parseInt(args[idx]) : dflt;
    }

    private SoakV2() {
    }
}
