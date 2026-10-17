package com.minimq.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.FlushMode;

/**
 * M5 parameterized benchmark runner. Every emitted row is self-contained JSONL.
 */
public final class M5Bench {
    private static final int[] DEFAULT_CONCURRENCY = { 1, 2, 4, 8, 16, 32 };

    public static void main(String[] args) throws Exception {
        String mode = option(args, "mode", "produce");
        String readMode = option(args, "read-mode", "cold");
        int[] concurrency = intList(option(args, "concurrency", "1,2,4,8,16,32"));
        int messages = integer(args, "messages", 20000);
        int payload = integer(args, "payload", 1024);
        int warmup = integer(args, "warmup", Math.min(2000, Math.max(100, messages / 10)));
        int rounds = integer(args, "rounds", 3);
        long lingerUs = Long.parseLong(option(args, "linger-us", "500"));
        int batch = integer(args, "batch", 64);
        boolean zeroCopy = Boolean.parseBoolean(option(args, "zero-copy", "true"));
        String flushModeName = option(args, "flush-mode", "sync").toUpperCase(Locale.ROOT) + "_FLUSH";
        FlushMode flushMode;
        try {
            flushMode = FlushMode.valueOf(flushModeName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("flush-mode must be sync, async, or os");
        }
        String clusterSpec = option(args, "cluster", "");
        List<InetSocketAddress> cluster = new ArrayList<InetSocketAddress>();
        if (!clusterSpec.isEmpty()) {
            for (String spec : clusterSpec.split(",")) {
                String[] hp = spec.trim().split(":");
                cluster.add(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])));
            }
        }
        boolean dropCaches = Boolean.parseBoolean(option(args, "drop-caches", "false"));
        Path output = Paths.get(option(args, "output", "result/m5.jsonl"));
        Files.createDirectories(output.toAbsolutePath().getParent());
        try (BufferedWriter out = Files.newBufferedWriter(output, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (int c : concurrency) {
                List<Round> samples = new ArrayList<Round>();
                for (int r = 0; r < rounds; r++) {
                    samples.add(
                            run(mode, readMode, c, messages, payload, warmup, lingerUs, batch, zeroCopy, flushMode,
                                    cluster, dropCaches));
                }
                Round median = median(samples);
                String row = String.format(Locale.ROOT,
                        "{\"mode\":\"%s\",\"readMode\":\"%s\",\"latencyKind\":\"%s\",\"cluster\":\"%s\",\"concurrency\":%d,\"messages\":%d,\"payload\":%d,"
                                + "\"warmup\":%d,\"rounds\":%d,\"lingerUs\":%d,\"batch\":%d,"
                                + "\"zeroCopy\":%s,\"flushMode\":\"%s\",\"throughputMsgSec\":%d,\"throughputMbSec\":%.3f,"
                                + "\"p50Ms\":%.3f,\"p95Ms\":%.3f,\"p99Ms\":%.3f,\"p999Ms\":%.3f,"
                                + "\"consumed\":%d,\"processCpuCores\":%.4f,\"heapUsedBytesAtEnd\":%d,\"samples\":\"%s\"}%n",
                        mode, readMode, latencyKind(mode, readMode),
                        cluster.isEmpty() ? "embedded" : "rf3", c, messages, payload, warmup, rounds, lingerUs,
                        batch, zeroCopy,
                        flushMode,
                        median.throughput, median.throughput * payload / 1_000_000.0,
                        median.p50, median.p95, median.p99, median.p999, median.consumed,
                        median.cpuCores, median.heapUsed,
                        sampleText(samples));
                out.write(row);
                out.flush();
                System.out.print(row);
            }
        }
    }

    private static Round run(String mode, String readMode, int concurrency, int messages, int payload, int warmup,
            long lingerUs, int batch, boolean zeroCopy, FlushMode flushMode, List<InetSocketAddress> cluster,
            boolean dropCaches)
            throws Exception {
        if (!cluster.isEmpty() && !mode.equals("produce")) {
            throw new IllegalArgumentException("--cluster supports mode=produce only");
        }
        Path root = Files.createTempDirectory("mini-mq-m5-");
        BrokerServer server = null;
        if (cluster.isEmpty()) {
            server = new BrokerServer(BrokerConfig.defaults().port(0)
                    .storeDir(root.resolve("store").toFile()).segmentSize(256 * 1024 * 1024)
                    .flushMode(flushMode).flushLingerUs(lingerUs).flushBatchCount(batch)
                    .zeroCopyFetch(zeroCopy));
            server.start();
        }
        MQProducer producer = null;
        MQConsumer consumer = null;
        try {
            String topic = "m5-measured";
            producer = cluster.isEmpty()
                    ? MQProducer.connect("127.0.0.1", server.port(), 20_000, 256 * 1024, 1L)
                    : MQProducer.connectCluster(cluster, 20_000, 256 * 1024, 1L)
                            .acks(MqProto.AckMode.ACKS_ALL);
            if (warmup > 0) {
                produce(producer, "m5-warmup", warmup, payload, Math.max(1, concurrency), false);
            }
            long[] latencies = new long[0];
            long consumed = 0;
            long elapsed;
            long processCpuNanos;
            if (mode.equals("consume")) {
                consumer = MQConsumer.subscribe("127.0.0.1", server.port(), topic, "m5",
                        root.resolve("offsets").toFile(), 256, 20_000);
                if (readMode.equals("cold")) {
                    produce(producer, topic, messages, payload, Math.max(1, concurrency), false);
                    producer.close();
                    producer = null;
                    consumer.close();
                    consumer = null;
                    server.shutdown();
                    if (dropCaches) {
                        // True disk-cold reads for the page-cache-hit experiment:
                        // evict the OS page cache between shutdown and restart
                        // (Linux/root only; the WSL2 experiment).
                        new ProcessBuilder("sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches")
                                .inheritIO().start().waitFor();
                    }
                    server = new BrokerServer(BrokerConfig.defaults().port(0)
                            .storeDir(root.resolve("store").toFile()).segmentSize(256 * 1024 * 1024)
                            .flushMode(flushMode).flushLingerUs(lingerUs).flushBatchCount(batch)
                            .zeroCopyFetch(zeroCopy));
                    server.start();
                    consumer = MQConsumer.subscribe("127.0.0.1", server.port(), topic, "m5-cold",
                            root.resolve("offsets-cold").toFile(), 256, 20_000);
                    long start = System.nanoTime();
                    long cpuStart = processCpuTime();
                    List<Long> readLatencies = consume(consumer, messages, 300_000);
                    elapsed = System.nanoTime() - start;
                    processCpuNanos = processCpuTime() - cpuStart;
                    consumed = readLatencies.size();
                    latencies = toArray(readLatencies);
                } else if (readMode.equals("tail")) {
                    final MQConsumer activeConsumer = consumer;
                    final AtomicLong read = new AtomicLong();
                    final AtomicReference<Throwable> readerFailure = new AtomicReference<Throwable>();
                    final CountDownLatch readerDone = new CountDownLatch(1);
                    final long[] readLatencies = new long[messages];
                    final Set<Long> consumedIds = java.util.Collections.synchronizedSet(new HashSet<Long>());
                    Thread reader = new Thread(new Runnable() {
                        public void run() {
                            try {
                                long deadline = System.currentTimeMillis() + 300_000;
                                while (read.get() < messages && System.currentTimeMillis() < deadline) {
                                    List<ClientRecord> records = activeConsumer.poll();
                                    if (records.isEmpty())
                                        Thread.sleep(1);
                                    else
                                        for (ClientRecord record : records) {
                                            int index = (int) read.getAndIncrement();
                                            verifyPayloadId(record, messages, consumedIds);
                                            if (index < readLatencies.length) {
                                                readLatencies[index] = Math.max(0L,
                                                        System.nanoTime() - payloadStart(record));
                                            }
                                        }
                                }
                            } catch (Throwable e) {
                                readerFailure.set(e);
                            } finally {
                                readerDone.countDown();
                            }
                        }
                    }, "m5-tail-consumer");
                    long start = System.nanoTime();
                    long cpuStart = processCpuTime();
                    reader.start();
                    latencies = produce(producer, topic, messages, payload, Math.max(1, concurrency), true);
                    producer.close();
                    producer = null;
                    readerDone.await();
                    elapsed = System.nanoTime() - start;
                    processCpuNanos = processCpuTime() - cpuStart;
                    consumed = read.get();
                    if (readerFailure.get() != null)
                        throw new IllegalStateException("tail consumer failed", readerFailure.get());
                    if (consumed != messages)
                        throw new IllegalStateException("tail consume mismatch: " + consumed + " != " + messages);
                    latencies = readLatencies;
                } else {
                    throw new IllegalArgumentException("read-mode must be cold or tail");
                }
                if (consumed != messages)
                    throw new IllegalStateException("consume mismatch: " + consumed + " != " + messages);
            } else if (mode.equals("mixed")) {
                consumer = MQConsumer.subscribe("127.0.0.1", server.port(), topic, "m5",
                        root.resolve("offsets").toFile(), 256, 20_000);
                final MQConsumer activeConsumer = consumer;
                final AtomicLong read = new AtomicLong();
                final AtomicReference<Throwable> readerFailure = new AtomicReference<Throwable>();
                final CountDownLatch readerDone = new CountDownLatch(1);
                final Set<Long> consumedIds = java.util.Collections.synchronizedSet(new HashSet<Long>());
                Thread reader = new Thread(new Runnable() {
                    public void run() {
                        try {
                            long deadline = System.currentTimeMillis() + 300_000;
                            while (read.get() < messages && System.currentTimeMillis() < deadline) {
                                List<ClientRecord> records = activeConsumer.poll();
                                if (records.isEmpty())
                                    Thread.sleep(1);
                                else {
                                    for (ClientRecord record : records) {
                                        int index = (int) read.getAndIncrement();
                                        verifyPayloadId(record, messages, consumedIds);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            readerFailure.set(e);
                        } finally {
                            readerDone.countDown();
                        }
                    }
                }, "m5-consumer");
                long start = System.nanoTime();
                long cpuStart = processCpuTime();
                reader.start();
                latencies = produce(producer, topic, messages, payload, Math.max(1, concurrency), true);
                producer.close();
                producer = null;
                readerDone.await();
                elapsed = System.nanoTime() - start;
                processCpuNanos = processCpuTime() - cpuStart;
                consumed = read.get();
                if (readerFailure.get() != null)
                    throw new IllegalStateException("mixed consumer failed", readerFailure.get());
                if (consumed != messages)
                    throw new IllegalStateException("mixed consume mismatch: " + consumed + " != " + messages);
            } else if (mode.equals("produce")) {
                long start = System.nanoTime();
                long cpuStart = processCpuTime();
                latencies = produce(producer, topic, messages, payload, Math.max(1, concurrency), true);
                elapsed = System.nanoTime() - start;
                processCpuNanos = processCpuTime() - cpuStart;
                producer.close();
                producer = null;
                consumed = messages;
            } else {
                throw new IllegalArgumentException("mode must be produce, consume, or mixed");
            }
            Round result = new Round();
            result.throughput = (long) (messages / (elapsed / 1_000_000_000.0));
            result.consumed = consumed;
            result.p50 = percentile(latencies, .50);
            result.p95 = percentile(latencies, .95);
            result.p99 = percentile(latencies, .99);
            result.p999 = percentile(latencies, .999);
            result.cpuCores = elapsed == 0 ? -1.0 : processCpuNanos / (double) elapsed;
            result.heapUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            return result;
        } finally {
            if (producer != null)
                producer.close();
            if (consumer != null)
                consumer.close();
            if (server != null)
                server.shutdown();
            cleanup(root);
        }
    }

    private static long[] produce(MQProducer producer, String topic, int count, int payload,
            int concurrency, boolean measure) throws Exception {
        final long[] latencies = measure ? new long[count] : new long[0];
        final AtomicLong acked = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        final CountDownLatch done = new CountDownLatch(count);
        int per = count / concurrency;
        for (int p = 0; p < concurrency; p++) {
            final int from = p * per;
            final int to = p == concurrency - 1 ? count : from + per;
            new Thread(new Runnable() {
                public void run() {
                    for (int i = from; i < to; i++) {
                        final int index = i;
                        final long begin = System.nanoTime();
                        byte[] body = new byte[payload];
                        ByteBuffer.wrap(body).putLong(index).putLong(begin);
                        try {
                            CompletableFuture<Long> future = producer.send(topic, body);
                            future.whenComplete((v, e) -> {
                                if (e == null && measure)
                                    latencies[index] = System.nanoTime() - begin;
                                if (e != null)
                                    failures.incrementAndGet();
                                acked.incrementAndGet();
                                done.countDown();
                            });
                        } catch (Throwable e) {
                            failures.incrementAndGet();
                            done.countDown();
                        }
                    }
                }
            }, "m5-producer-" + p).start();
        }
        done.await();
        if (failures.get() != 0 || acked.get() != count) {
            throw new IllegalStateException(
                    "produce failures=" + failures.get() + ", acknowledged=" + acked.get() + "/" + count);
        }
        return latencies;
    }

    private static Round median(List<Round> rounds) {
        Round out = new Round();
        out.throughput = medianLong(rounds, 0);
        out.p50 = medianDouble(rounds, 1);
        out.p95 = medianDouble(rounds, 2);
        out.p99 = medianDouble(rounds, 3);
        out.p999 = medianDouble(rounds, 4);
        out.consumed = medianLong(rounds, 5);
        out.cpuCores = medianDouble(rounds, 6);
        long[] heap = new long[rounds.size()];
        for (int i = 0; i < heap.length; i++)
            heap[i] = rounds.get(i).heapUsed;
        Arrays.sort(heap);
        out.heapUsed = heap[heap.length / 2];
        return out;
    }

    private static List<Long> consume(MQConsumer consumer, int expected, long timeoutMs) throws Exception {
        List<Long> latencies = new ArrayList<Long>(expected);
        Set<Long> consumedIds = new HashSet<Long>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (latencies.size() < expected && System.currentTimeMillis() < deadline) {
            long pollStart = System.nanoTime();
            List<ClientRecord> records = consumer.poll();
            long pollLatency = System.nanoTime() - pollStart;
            if (records.isEmpty())
                Thread.sleep(1L);
            else
                for (ClientRecord record : records) {
                    verifyPayloadId(record, expected, consumedIds);
                    latencies.add(pollLatency);
                }
        }
        if (latencies.size() != expected)
            throw new IllegalStateException("consume mismatch: " + latencies.size() + " != " + expected);
        return latencies;
    }

    private static long payloadStart(ClientRecord record) {
        byte[] payload = record.getPayload();
        if (payload.length < 16)
            throw new IllegalStateException("benchmark payload is missing its monotonic timestamp");
        return ByteBuffer.wrap(payload, 8, 8).getLong();
    }

    private static void verifyPayloadId(ClientRecord record, int expectedCount, Set<Long> seen) {
        byte[] payload = record.getPayload();
        if (payload.length < 8)
            throw new IllegalStateException("benchmark payload is missing its message id");
        long id = ByteBuffer.wrap(payload).getLong();
        if (id < 0 || id >= expectedCount)
            throw new IllegalStateException("unexpected message id: " + id);
        if (!seen.add(Long.valueOf(id)))
            throw new IllegalStateException("duplicate message id: " + id);
    }

    private static long[] toArray(List<Long> values) {
        long[] result = new long[values.size()];
        for (int i = 0; i < values.size(); i++)
            result[i] = values.get(i).longValue();
        return result;
    }

    private static long medianLong(List<Round> rs, int field) {
        long[] v = new long[rs.size()];
        for (int i = 0; i < v.length; i++)
            v[i] = field == 0 ? rs.get(i).throughput : rs.get(i).consumed;
        Arrays.sort(v);
        return v[v.length / 2];
    }

    private static double medianDouble(List<Round> rs, int field) {
        double[] v = new double[rs.size()];
        for (int i = 0; i < v.length; i++) {
            Round r = rs.get(i);
            v[i] = field == 1 ? r.p50 : field == 2 ? r.p95 : field == 3 ? r.p99 : field == 4 ? r.p999 : r.cpuCores;
        }
        Arrays.sort(v);
        return v[v.length / 2];
    }

    private static double percentile(long[] values, double q) {
        if (values.length == 0)
            return 0;
        long[] v = values.clone();
        Arrays.sort(v);
        int i = (int) Math.ceil(q * v.length) - 1;
        return v[Math.max(0, i)] / 1_000_000.0;
    }

    private static String latencyKind(String mode, String readMode) {
        if (mode.equals("consume"))
            return readMode.equals("tail") ? "produce-to-consume" : "consumer-poll";
        return "produce-ack";
    }

    private static long processCpuTime() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        if (bean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) bean).getProcessCpuTime();
        }
        return -1L;
    }

    private static String sampleText(List<Round> rs) {
        StringBuilder b = new StringBuilder();
        for (Round r : rs) {
            if (b.length() > 0)
                b.append(';');
            b.append(r.throughput).append(',').append(String.format(Locale.ROOT, "%.3f", r.p50))
                    .append(',').append(String.format(Locale.ROOT, "%.3f", r.p95))
                    .append(',').append(String.format(Locale.ROOT, "%.3f", r.p99))
                    .append(',').append(String.format(Locale.ROOT, "%.3f", r.p999))
                    .append(',').append(String.format(Locale.ROOT, "%.4f", r.cpuCores))
                    .append(',').append(r.heapUsed);
        }
        return b.toString();
    }

    private static String option(String[] args, String key, String d) {
        String p = "--" + key + "=";
        for (String a : args)
            if (a.startsWith(p))
                return a.substring(p.length());
        return d;
    }

    private static int integer(String[] args, String key, int d) {
        return Integer.parseInt(option(args, key, Integer.toString(d)));
    }

    private static int[] intList(String text) {
        String[] a = text.split(",");
        int[] v = new int[a.length];
        for (int i = 0; i < a.length; i++)
            v[i] = Integer.parseInt(a[i].trim());
        return v;
    }

    private static void cleanup(Path root) {
        try {
            Files.walk(root).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException ignored) {
        }
    }

    private static final class Round {
        long throughput;
        long consumed;
        long heapUsed;
        double p50, p95, p99, p999, cpuCores;
    }

    private M5Bench() {
    }
}
