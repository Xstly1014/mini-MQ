package com.minimq.bench;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/**
 * Allocation profiler (M2 task 3): runs ONE full produce+consume round in
 * this JVM with a JFR allocation recording attached, then ranks allocation
 * sites by sampled bytes. This is the evidence step — pooling decisions
 * (Netty Recycler) come AFTER the Top list, never from guesses.
 *
 * Usage: AllocProfile [msgs=200000] [payload=1024] [out=minimq-alloc.jfr]
 *
 * JFR notes: ObjectAllocationInNewTLAB/OutsideTLAB are byte-weighted
 * samples; percentages are of SAMPLED bytes, not exact totals — good
 * enough to rank sites, which is all we need.
 */
public final class AllocProfile {

    public static void main(String[] args) throws Exception {
        int msgs = intArg(args, 0, 200_000);
        int payloadSize = intArg(args, 1, 1024);
        Path out = Paths.get(stringArg(args, 2, "minimq-alloc.jfr"));
        boolean zeroCopy = Boolean.parseBoolean(stringArg(args, 3, "false"));

        Recording recording = new Recording();
        recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
        recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
        recording.setName("mini-mq-alloc");
        recording.start();
        try {
            runRound(msgs, payloadSize, zeroCopy);
        } finally {
            recording.stop();
            recording.dump(out);
        }
        System.out.println("# recording: " + out);
        System.out.println("# zeroCopyFetch=" + zeroCopy);
        rank(out);
    }

    /** One produce+consume round over a fresh in-process broker. */
    private static void runRound(int count, int payloadSize, boolean zeroCopy) throws Exception {
        Path root = Files.createTempDirectory("mini-mq-alloc-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                .zeroCopyFetch(zeroCopy));
        server.start();
        try {
            String topic = "alloc";
            MQProducer producer = MQProducer.connect("127.0.0.1", server.port(),
                    20_000, 256 * 1024, 1L);
            try {
                CountDownLatch allAcked = new CountDownLatch(count);
                AtomicLong acked = new AtomicLong();
                int sent = 0;
                while (sent < count) {
                    if (sent - acked.get() > 16_384) {
                        Thread.sleep(1);
                        continue;
                    }
                    CompletableFuture<Long> f = producer.send(topic,
                            BenchV1Payload.payload(sent, System.nanoTime(), payloadSize));
                    f.whenComplete((v, err) -> {
                        acked.incrementAndGet();
                        allAcked.countDown();
                    });
                    sent++;
                }
                allAcked.await();
            } finally {
                producer.close();
            }

            MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", server.port(),
                    topic, "alloc", root.resolve("offsets").toFile(), 256, 20_000);
            try {
                long received = 0;
                long lastProgress = System.currentTimeMillis();
                while (received < count) {
                    List<ClientRecord> batch = consumer.poll();
                    if (batch.isEmpty()) {
                        if (System.currentTimeMillis() - lastProgress >= 3_000) {
                            throw new IllegalStateException("consume stalled at " + received);
                        }
                        continue;
                    }
                    received += batch.size();
                    lastProgress = System.currentTimeMillis();
                }
            } finally {
                consumer.close();
            }
        } finally {
            server.shutdown();
            cleanupQuietly(root);
        }
    }

    // ---- aggregation ----

    private static final class Site {
        long bytes;
        long events;
        final Map<String, long[]> frames = new HashMap<>(); // top frame -> {bytes, events}
    }

    private static void rank(Path jfrFile) throws IOException {
        Map<String, Site> byClass = new HashMap<>();
        long totalBytes = 0;
        try (RecordingFile file = new RecordingFile(jfrFile)) {
            while (file.hasMoreEvents()) {
                RecordedEvent e = file.readEvent();
                String name = e.getEventType().getName();
                if (!name.endsWith("ObjectAllocationInNewTLAB")
                        && !name.endsWith("ObjectAllocationOutsideTLAB")) {
                    continue;
                }
                long size = e.getLong("allocationSize");
                String cls = ((jdk.jfr.consumer.RecordedClass) e.getValue("objectClass")).getName();
                Site site = byClass.computeIfAbsent(cls, k -> new Site());
                site.bytes += size;
                site.events++;
                String top = topFrame(e.getStackTrace());
                if (top != null) {
                    long[] agg = site.frames.computeIfAbsent(top, k -> new long[2]);
                    agg[0] += size;
                    agg[1]++;
                }
                totalBytes += size;
            }
        }

        List<Map.Entry<String, Site>> ranked = new java.util.ArrayList<>(byClass.entrySet());
        ranked.sort(Comparator.comparingLong((Map.Entry<String, Site> en) -> en.getValue().bytes).reversed());

        System.out.println();
        System.out.println("# sampled allocation bytes by class (top 25)");
        System.out.println();
        System.out.println("| rank | class | sampled MB | events | % | hottest frames (bytes) |");
        System.out.println("|------|-------|-----------:|-------:|---:|------------------------|");
        for (int i = 0; i < Math.min(25, ranked.size()); i++) {
            Map.Entry<String, Site> en = ranked.get(i);
            Site s = en.getValue();
            StringBuilder frames = new StringBuilder();
            s.frames.entrySet().stream()
                    .sorted(Comparator.comparingLong((Map.Entry<String, long[]> f) -> f.getValue()[0]).reversed())
                    .limit(3)
                    .forEach(f -> {
                        if (frames.length() > 0) {
                            frames.append("<br>");
                        }
                        frames.append(shorten(f.getKey())).append(" (")
                                .append(mb(f.getValue()[0])).append("MB)");
                    });
            System.out.printf("| %d | %s | %.1f | %,d | %.1f%% | %s |%n",
                    i + 1, en.getKey(), s.bytes / 1e6, s.events,
                    100.0 * s.bytes / Math.max(1, totalBytes), frames);
        }
        System.out.printf("%n# total sampled: %.1f MB%n", totalBytes / 1e6);
    }

    private static String topFrame(RecordedStackTrace stack) {
        if (stack == null) {
            return null;
        }
        List<RecordedFrame> frames = stack.getFrames();
        if (frames.isEmpty()) {
            return null;
        }
        // walk past trivial java.base frames to the first project/library frame
        for (int i = 0; i < Math.min(frames.size(), 8); i++) {
            RecordedFrame f = frames.get(i);
            String cls = f.getMethod().getType().getName();
            if (cls.startsWith("java.") || cls.startsWith("jdk.")) {
                continue;
            }
            return cls + "." + f.getMethod().getName() + ":" + f.getLineNumber();
        }
        RecordedFrame f = frames.get(0);
        return f.getMethod().getType().getName() + "." + f.getMethod().getName();
    }

    private static String shorten(String s) {
        return s.replace("com.minimq.", "").replace("io.netty.", "netty.");
    }

    private static double mb(long bytes) {
        return bytes / 1e6;
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
        return args != null && args.length > idx ? Integer.parseInt(args[idx]) : dflt;
    }

    private static String stringArg(String[] args, int idx, String dflt) {
        return args != null && args.length > idx ? args[idx] : dflt;
    }

    /** Payload shape shared with BenchV1: [8B seq][8B sendNanos][filler]. */
    static final class BenchV1Payload {
        static byte[] payload(long seq, long sendNanos, int size) {
            byte[] p = new byte[size];
            java.nio.ByteBuffer.wrap(p).putLong(seq).putLong(sendNanos);
            Arrays.fill(p, 16, size, (byte) 'x');
            return p;
        }
    }

    private AllocProfile() {
    }
}
