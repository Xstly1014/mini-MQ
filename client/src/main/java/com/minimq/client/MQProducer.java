package com.minimq.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * Producer SDK: batching + async callbacks.
 *
 * <ul>
 * <li><b>Batch aggregation</b> — sends accumulate in a buffer and leave as
 * one PRODUCE when the byte budget fills or linger expires (the client-side
 * mirror of Kafka's batch.size / linger.ms, and of M2 group commit).</li>
 * <li><b>maxInFlight = 1</b> — the next batch waits for the previous ack.
 * With a single broker business pool this keeps same-topic order strict
 * (order is append order). Kafka buys pipelining back with broker-side
 * sequence numbers; that trade arrives later.</li>
 * <li><b>RETRY_LATER / timeout</b> — retried with linear backoff on a
 * scheduler (never a sleeping callback thread), bounded by maxRetries;
 * exhausted batches fail their futures. At-least-once until M4 dedup.</li>
 * </ul>
 */
public final class MQProducer implements Closeable {

    private static final int CLOSE_TIMEOUT_MS = 10_000;

    private final Transport transport;
    private final int batchSizeBytes;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final ScheduledExecutorService scheduler;
    /**
     * Durability span for every batch (M3). ACKS_1 is the M1/M2 default: an
     * unset wire field means the same thing, so old producers against a new
     * broker (and vice versa) keep identical semantics.
     */
    private volatile MqProto.AckMode acks = MqProto.AckMode.ACKS_1;

    private final Object lock = new Object();
    private final ArrayDeque<PendingSend> buffer = new ArrayDeque<PendingSend>();
    private long bufferedBytes;
    private Batch inFlight;
    private boolean closed;
    private long seqCounter;
    /**
     * Per-instance identity inside every seq key (M4). The broker's dedup
     * window is keyed by seqKey hash ALONE — a bare counter ("seq-0") is
     * instance-local, so two producers (probe + storm, app-A + app-B)
     * collide at key 0 and the second send is swallowed as a DUPLICATE
     * with the FIRST sender's offset: a silent message loss. Mirroring
     * Kafka's producerId, a random 64-bit id makes default keys globally
     * unique while retries from THIS instance still collide with
     * themselves — exactly the at-least-once -> dedup convergence M4 needs.
     */
    private final String producerId = Long.toUnsignedString(
            java.util.concurrent.ThreadLocalRandom.current().nextLong());

    public MQProducer(Transport transport, int batchSizeBytes, long lingerMs,
            int maxRetries, long retryBackoffMs) {
        this.transport = transport;
        this.batchSizeBytes = batchSizeBytes;
        this.lingerMs = lingerMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "mini-mq-producer-timer");
                t.setDaemon(true);
                return t;
            }
        });
    }

    public static MQProducer connect(String host, int port, long requestTimeoutMs,
            int batchSizeBytes, long lingerMs)
            throws InterruptedException {
        return new MQProducer(ClientChannel.connect(host, port, requestTimeoutMs),
                batchSizeBytes, lingerMs, 5, 100L);
    }

    /**
     * Cluster flavor (M3): the transport follows NOT_LEADER redirects and
     * bootstrap fallbacks on its own, so the producer's own retry logic only
     * sees genuine failures (RETRY_LATER, exhausted redirect budgets).
     */
    public static MQProducer connectCluster(java.util.List<java.net.InetSocketAddress> nodes,
            long requestTimeoutMs, int batchSizeBytes, long lingerMs) {
        return new MQProducer(FailoverTransport.open(nodes, requestTimeoutMs),
                batchSizeBytes, lingerMs, 5, 100L);
    }

    /** Sets the durability span for subsequent sends (fluent). */
    public MQProducer acks(MqProto.AckMode acks) {
        this.acks = acks;
        return this;
    }

    /**
     * Enqueues a message; the future completes with its committed offset.
     * Never blocks: slow paths (flush, retry) all live on callbacks/scheduler.
     * The critical section covers buffer bookkeeping only — the proto build
     * and transport write of a dispatched batch happen OUTSIDE the lock, so
     * concurrent send() callers serialize on enqueue, not on network I/O.
     */
    public CompletableFuture<Long> send(String topic, byte[] payload) {
        CompletableFuture<Long> future = new CompletableFuture<Long>();
        Batch toDispatch = null;
        synchronized (lock) {
            if (closed) {
                future.completeExceptionally(new IllegalStateException("producer closed"));
                return future;
            }
            buffer.addLast(new PendingSend(topic, payload,
                    "seq-" + producerId + "-" + (seqCounter++), future));
            bufferedBytes += payload.length + topic.length() + 48; // ~ proto overhead
            if (buffer.size() == 1) {
                // first message of a fresh batch: arm the linger timer
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        onLinger();
                    }
                }, lingerMs, TimeUnit.MILLISECONDS);
            }
            if (bufferedBytes >= batchSizeBytes) {
                toDispatch = drainLocked();
            }
        }
        if (toDispatch != null) {
            sendBatch(toDispatch);
        }
        return future;
    }

    private void onLinger() {
        Batch toDispatch = null;
        synchronized (lock) {
            // busy in-flight: leave the buffer, the ack path re-flushes
            if (!closed && inFlight == null) {
                toDispatch = drainLocked();
            }
        }
        if (toDispatch != null) {
            sendBatch(toDispatch);
        }
    }

    /**
     * Caller must hold {@code lock}. Drains up to one byte-budget worth of
     * the buffer into a batch, claims the in-flight slot, and returns the
     * batch so the caller can dispatch it OUTSIDE the lock (proto encoding
     * and the transport write are pure with respect to shared state: they
     * read the batch and the volatile acks, and the ack path re-checks
     * inFlight under the lock). Whatever overflows stays buffered and is
     * relayed by the next {@link #finish}. Slicing here (not only on send())
     * matters: while a batch is in flight the buffer keeps growing, and the
     * ack-time relay must not ship it as one oversized frame — the wire
     * protocol caps frames at 1 MiB.
     */
    private Batch drainLocked() {
        if (buffer.isEmpty() || inFlight != null) {
            return null;
        }
        List<PendingSend> takes = new ArrayList<PendingSend>();
        long takenBytes = 0;
        while (!buffer.isEmpty()) {
            PendingSend ps = buffer.peekFirst();
            long cost = ps.payload.length + ps.topic.length() + 48; // ~ proto overhead
            if (!takes.isEmpty() && takenBytes + cost > batchSizeBytes) {
                break;
            }
            takes.add(buffer.pollFirst());
            takenBytes += cost;
        }
        bufferedBytes -= takenBytes;
        inFlight = new Batch(takes);
        return inFlight;
    }

    private void sendBatch(final Batch batch) {
        MqProto.ProduceRequest.Builder req = MqProto.ProduceRequest.newBuilder()
                .setAcks(acks);
        long now = System.currentTimeMillis();
        for (PendingSend ps : batch.sends) {
            req.addMessages(MqProto.MessageData.newBuilder()
                    .setTopic(ps.topic)
                    .setPayload(ByteString.copyFrom(ps.payload))
                    .setSeqKey(ps.seqKey)
                    .setTimestamp(now));
        }
        transport.request(MessageType.PRODUCE, req.build().toByteArray())
                .whenComplete((resp, err) -> onAck(batch, resp, err));
    }

    private void onAck(Batch batch, Frame resp, Throwable err) {
        if (err != null) {
            retry(batch, String.valueOf(err));
            return;
        }
        MqProto.ProduceAck ack;
        try {
            ack = MqProto.ProduceAck.parseFrom(resp.body());
        } catch (InvalidProtocolBufferException e) {
            retry(batch, "malformed ack");
            return;
        }
        switch (ack.getStatus()) {
            case OK:
            case DUPLICATE:
                // DUPLICATE (v1.4): a retry converged against the leader's
                // dedup window — the offsets are the ORIGINAL ones, so the
                // futures complete with them exactly as if the first append
                // had succeeded. To the caller the two statuses differ in
                // nothing observable.
                if (ack.getOffsetsCount() != batch.sends.size()) {
                    retry(batch, "offset count mismatch: "
                            + ack.getOffsetsCount() + " != " + batch.sends.size());
                    return;
                }
                for (int i = 0; i < batch.sends.size(); i++) {
                    batch.sends.get(i).future.complete(ack.getOffsets(i));
                }
                finish(batch);
                break;
            case RETRY_LATER:
                retry(batch, "broker busy (RETRY_LATER)");
                break;
            case ERROR:
            default:
                if (ack.getErrorCode() == MqProto.ErrorCode.ISR_TOO_SMALL) {
                    // v1.4: transient by contract — the ISR re-forms (or the
                    // leader changes and the transport redirects); resend
                    // with backoff instead of surfacing a durability error
                    // the caller cannot act on.
                    retry(batch, "ISR below min.insync.replicas (auto-resend)");
                } else {
                    fail(batch, new IOException("broker error: " + ack.getError()));
                }
                break;
        }
    }

    private void retry(final Batch batch, String reason) {
        batch.attempts++;
        if (batch.attempts > maxRetries) {
            fail(batch, new IOException("send failed after " + maxRetries
                    + " retries, last reason: " + reason));
            return;
        }
        // linear backoff on the scheduler; the retrying batch keeps the
        // in-flight slot so ordering is preserved across retries
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                sendBatch(batch);
            }
        }, retryBackoffMs * batch.attempts, TimeUnit.MILLISECONDS);
    }

    private void fail(Batch batch, Throwable cause) {
        for (PendingSend ps : batch.sends) {
            ps.future.completeExceptionally(cause);
        }
        finish(batch);
    }

    private void finish(Batch batch) {
        batch.done.countDown();
        Batch toDispatch = null;
        synchronized (lock) {
            if (inFlight == batch) {
                inFlight = null;
                if (!buffer.isEmpty()) {
                    toDispatch = drainLocked(); // relay the next accumulated batch
                }
            }
        }
        if (toDispatch != null) {
            sendBatch(toDispatch);
        }
    }

    /**
     * Flushes the pending buffer, waits (bounded) for in-flight batches to
     * reach a terminal state, then closes the transport. After close()
     * returns, every send() future is complete — the reconciliation harness
     * depends on this ("close and wait for all callbacks" pit rule).
     */
    @Override
    public void close() throws IOException {
        Batch finalBatch = null;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            if (!buffer.isEmpty() && inFlight == null) {
                finalBatch = drainLocked(); // final batch, closed flag does not block it
            }
        }
        if (finalBatch != null) {
            sendBatch(finalBatch);
        }
        long deadline = System.currentTimeMillis() + CLOSE_TIMEOUT_MS;
        try {
            while (true) {
                Batch batch;
                synchronized (lock) {
                    batch = inFlight;
                }
                if (batch == null) {
                    break;
                }
                long left = deadline - System.currentTimeMillis();
                if (left <= 0 || !batch.done.await(left, TimeUnit.MILLISECONDS)) {
                    fail(batch, new IOException("close timeout waiting for batch"));
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (lock) {
            if (inFlight != null) {
                fail(inFlight, new IOException("producer closed with batch in flight"));
                inFlight = null;
            }
            for (PendingSend ps : buffer) {
                ps.future.completeExceptionally(new IllegalStateException("producer closed"));
            }
            buffer.clear();
        }
        scheduler.shutdownNow();
        transport.close();
    }

    private static final class PendingSend {
        final String topic;
        final byte[] payload;
        final String seqKey;
        final CompletableFuture<Long> future;

        PendingSend(String topic, byte[] payload, String seqKey, CompletableFuture<Long> future) {
            this.topic = topic;
            this.payload = payload;
            this.seqKey = seqKey;
            this.future = future;
        }
    }

    private static final class Batch {
        final List<PendingSend> sends;
        final CountDownLatch done = new CountDownLatch(1);
        int attempts;

        Batch(List<PendingSend> sends) {
            this.sends = sends;
        }
    }
}
