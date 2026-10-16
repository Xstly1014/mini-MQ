package com.minimq.bench;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;
import com.minimq.storage.FlushMode;

/**
 * Crash reconciliation per durability tier (M2 task 4b): SIGKILL a real
 * broker process mid-send, restart it (the Recovery path), consume whatever
 * survived, and verify each tier's documented contract.
 *
 * <ul>
 *   <li><b>invariants, all tiers</b>: no duplicates, no out-of-order, no
 *       fabricated records — every consumed record was one we sent;</li>
 *   <li><b>SYNC_FLUSH</b>: every ACKED record survived. Ack-after-force is
 *       the tier's whole promise; one lost ack = "fake sync" (red line);</li>
 *   <li><b>ASYNC_FLUSH / OS_FLUSH</b>: acked-but-lost is the documented
 *       crash window — measured and reported as-is, not failed.</li>
 * </ul>
 *
 * <p>The kill lands at a random moment 0.5–1.5 s into the send loop, so it
 * can hit mid-stream (in-flight futures, unflushed tail) or just after the
 * last ack — both shapes are semantically interesting. In-flight messages
 * whose ack never reached the producer are NOT counted as lost: only
 * ack-then-vanish counts against a tier.
 *
 * <pre>usage: Kill9Reconcile &lt;SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH&gt; [msgs=200000] [payload=1024]</pre>
 */
public final class Kill9Reconcile {

    private static final int UNACKED_WINDOW = 16_384;
    private static final int IDLE_TIMEOUT_MS = 5_000;
    private static final long HARD_DEADLINE_MS = 180_000;
    private static final Random RNG = new Random();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: Kill9Reconcile <SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH> [msgs] [payload]");
            System.exit(2);
        }
        FlushMode mode = FlushMode.valueOf(args[0]);
        int total = args.length > 1 ? Integer.parseInt(args[1]) : 200_000;
        int payloadSize = args.length > 2 ? Integer.parseInt(args[2]) : 1024;

        Path root = Files.createTempDirectory("mini-mq-kill9-" + mode + "-");
        File storeDir = root.resolve("store").toFile();
        String topic = "kill9-" + System.currentTimeMillis();

        System.out.printf("# kill9 reconcile  mode=%s  msgs=%d  payload=%dB%n", mode, total, payloadSize);

        Process broker = startBroker(storeDir, mode);
        int port = awaitReady(broker);

        // ---- produce until SIGKILL lands mid-stream ----
        final BitSet ackedSeqs = new BitSet(total);
        final AtomicLong acked = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicBoolean killed = new AtomicBoolean();
        final AtomicLong killedAtMs = new AtomicLong();

        // Connect BEFORE arming the killer: on a cold JVM the first Netty
        // connect (class loading + event-loop startup) can take longer than
        // the killer's minimum delay — the kill then landed before the
        // connection existed and produced a useless Connection-reset run.
        final MQProducer producer = MQProducer.connect("127.0.0.1", port, 20_000, 256 * 1024, 1L);

        Thread killer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Wait until the run has real acked mass before striking:
                    // a cold broker's first force (mmap init + first fsync)
                    // can take hundreds of ms, and an early kill leaves only
                    // a handful of acks to reconcile against — weak evidence.
                    long armDeadline = System.currentTimeMillis() + 15_000;
                    while (acked.get() < 10_000 && System.currentTimeMillis() < armDeadline) {
                        Thread.sleep(20);
                    }
                    Thread.sleep(100 + RNG.nextInt(400)); // still random WITHIN the hot window
                    broker.destroyForcibly(); // SIGKILL on Linux: no shutdown hook, no final force
                    killed.set(true);
                    killedAtMs.set(System.currentTimeMillis());
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "kill9-killer");
        killer.setDaemon(true);
        killer.start();
        long sent = 0;
        try {
            while (sent < total) {
                // let in-flight futures settle for 2s after the kill, then stop
                if (killed.get() && System.currentTimeMillis() - killedAtMs.get() > 2_000) {
                    break;
                }
                if (sent - acked.get() - failed.get() > UNACKED_WINDOW) {
                    Thread.sleep(1);
                    continue;
                }
                final long s = sent++;
                CompletableFuture<Long> f = producer.send(topic, payload(s, payloadSize));
                f.whenComplete((v, err) -> {
                    if (err == null) {
                        acked.incrementAndGet();
                        ackedSeqs.set((int) s);
                    } else {
                        failed.incrementAndGet();
                    }
                });
            }
            long settleDeadline = System.currentTimeMillis() + 3_000;
            while (acked.get() + failed.get() < sent
                    && System.currentTimeMillis() < settleDeadline) {
                Thread.sleep(10);
            }
        } finally {
            producer.close();
        }
        killer.join();
        broker.waitFor(); // reap the corpse
        long killElapsed = killedAtMs.get() > 0 ? System.currentTimeMillis() - killedAtMs.get() : 0;
        System.out.printf("kill landed: sent=%d acked=%d failed=%d (settled %dms after kill)%n",
                sent, acked.get(), failed.get(), killElapsed);

        // ---- restart on the same store: the Recovery path ----
        Process broker2 = startBroker(storeDir, mode);
        int port2 = awaitReady(broker2);

        // ---- consume everything that survived ----
        int[] counts = new int[total];
        final Stats stats = new Stats();
        MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", port2, topic, "kill9",
                root.resolve("offsets").toFile(), 256, 20_000);
        try {
            long lastNew = System.currentTimeMillis();
            long deadline = lastNew + HARD_DEADLINE_MS;
            while (System.currentTimeMillis() < deadline) {
                List<ClientRecord> batch = consumer.poll();
                if (!batch.isEmpty()) {
                    for (ClientRecord r : batch) {
                        verify(r, counts, stats);
                    }
                    lastNew = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastNew >= IDLE_TIMEOUT_MS) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
        stopBroker(broker2);

        // ---- reconcile ----
        report(mode, total, sent, acked.get(), failed.get(), ackedSeqs, counts, stats);
        cleanupQuietly(root);
    }

    private static final class Stats {
        long consumed;
        long outOfOrder;
        long bad;
        long maxSeen = -1;
    }

    private static void verify(ClientRecord r, int[] counts, Stats s) {
        byte[] p = r.getPayload();
        if (p.length < 8) {
            s.bad++;
            return;
        }
        long seq = ByteBuffer.wrap(p, 0, 8).getLong();
        if (seq < 0 || seq >= counts.length) {
            s.bad++; // fabricated: a seq we never sent
            return;
        }
        counts[(int) seq]++;
        if (seq < s.maxSeen) {
            s.outOfOrder++;
        } else {
            s.maxSeen = seq;
        }
        s.consumed++;
    }

    private static void report(FlushMode mode, int total, long sent, long acked, long failed,
            BitSet ackedSeqs, int[] counts, Stats s) {
        long lostAcked = 0;
        long dupSeqs = 0;
        long dupExcess = 0;
        for (int i = 0; i < counts.length; i++) {
            if (ackedSeqs.get(i) && counts[i] == 0) {
                lostAcked++;
            }
            if (counts[i] > 1) {
                dupSeqs++;
                dupExcess += counts[i] - 1;
            }
        }
        boolean invariantsOk = dupSeqs == 0 && s.outOfOrder == 0 && s.bad == 0;
        boolean syncRedLine = mode != FlushMode.SYNC_FLUSH || lostAcked == 0;
        boolean pass = invariantsOk && syncRedLine;

        System.out.println();
        System.out.println("| item | value |");
        System.out.println("|------|-------|");
        System.out.printf("| sent (send attempted) | %d |%n", sent);
        System.out.printf("| acked (OK from broker) | %d |%n", acked);
        System.out.printf("| send failures (post-kill) | %d |%n", failed);
        System.out.printf("| consumed after restart | %d |%n", s.consumed);
        System.out.printf("| **duplicate seqs** | **%d** (excess: %d) |%n", dupSeqs, dupExcess);
        System.out.printf("| **out-of-order** | **%d** |%n", s.outOfOrder);
        System.out.printf("| **fabricated (never sent)** | **%d** |%n", s.bad);
        System.out.printf("| **acked but lost** | **%d** |%n", lostAcked);

        if (mode == FlushMode.SYNC_FLUSH) {
            System.out.println();
            System.out.println("SYNC_FLUSH red line: every acked record must survive the SIGKILL.");
        } else {
            System.out.println();
            System.out.printf("%s crash window: acked-but-lost is the documented, measured loss window.%n", mode);
        }
        System.out.println();
        System.out.println("VERDICT: " + (pass ? "PASS" : "FAIL")
                + (mode == FlushMode.SYNC_FLUSH ? " (acked-lost must be 0)" : " (invariants must hold)"));
        if (!pass) {
            System.exit(1);
        }
    }

    private static Process startBroker(File storeDir, FlushMode mode) throws IOException {
        String javaBin = System.getProperty("java.home") + File.separator + "bin"
                + (File.pathSeparatorChar == ';' ? File.separator + "java.exe" : File.separator + "java");
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp",
                System.getProperty("java.class.path"),
                "com.minimq.bench.BrokerMain",
                "0", storeDir.getAbsolutePath(), mode.name(), "500", "64");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT); // broker JUL logs -> our stderr
        return pb.start();
    }

    /** Reads the child's stdout until {@code READY:<port>} and returns the port. */
    private static int awaitReady(Process broker) throws IOException, InterruptedException {
        BufferedReader out = new BufferedReader(new InputStreamReader(broker.getInputStream()));
        long deadline = System.currentTimeMillis() + 30_000;
        String line;
        while ((line = out.readLine()) != null) {
            if (line.startsWith("READY:")) {
                return Integer.parseInt(line.substring("READY:".length()).trim());
            }
            if (System.currentTimeMillis() > deadline) {
                break;
            }
        }
        throw new IllegalStateException("broker never became READY (exit? "
                + broker.isAlive() + ")");
    }

    /** Graceful stop: send the QUIT line, the broker drains and exits itself. */
    private static void stopBroker(Process broker) throws IOException, InterruptedException {
        broker.getOutputStream().write("QUIT\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        broker.getOutputStream().flush();
        broker.getOutputStream().close();
        if (!broker.waitFor(15, TimeUnit.SECONDS)) {
            broker.destroyForcibly();
        }
    }

    /** [8B seq][8B nanos][filler] — same shape as the other bench harnesses. */
    private static byte[] payload(long seq, int size) {
        byte[] p = new byte[size];
        ByteBuffer.wrap(p).putLong(seq).putLong(System.nanoTime());
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

    private Kill9Reconcile() {
    }
}
