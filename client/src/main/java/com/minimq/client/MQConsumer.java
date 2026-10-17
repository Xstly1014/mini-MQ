package com.minimq.client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * Consumer SDK: pull model + offset persistence.
 *
 * <b>Why pull, not push</b>: the consume rate is set by how fast the consumer
 * polls — the broker can never overwhelm a slow consumer (natural
 * backpressure), and one FETCH carries a whole batch (amortized RTT).
 *
 * <b>Consume-then-commit</b>: the offset persisted for a batch is its end
 * position, and it is only written into the store at the NEXT poll (when the
 * previous batch must have been handled) or on explicit {@link #commit()}.
 * Crashing mid-processing therefore replays the in-flight batch:
 * at-least-once, duplicates deduped by seq key (M4 broker window) and
 * consumer-side idempotency.
 *
 * <b>Poison pills &amp; dead letters (M4)</b>: a record whose processing
 * throws is reported via {@link #reportFailure}. The consumer rewinds to
 * that record and retries it up to {@code maxRetries} times (the counter
 * persists in a sidecar next to the offsets, so a crash-looping consumer
 * still converges); one attempt over the limit and the record is appended
 * to the dead-letter log, the alarm listener fires, and the stream moves
 * past it — one bad record can no longer wedge a topic forever.
 *
 * <b>Zero-copy fetch responses (M2)</b>: the broker may answer a FETCH with
 * a body of shape [4B metaLen][meta][raw] where {@code zero_copy = true} and
 * records carry no inline payload — the raw tail is the verbatim CommitLog
 * span, and each record's payload is sliced out at
 * {@code (offset - firstOffset) + payload_offset} for {@code payload_len}
 * bytes (both fields broker-supplied in the meta). The consumer therefore
 * needs zero knowledge of the storage record layout. Legacy responses
 * ({@code zero_copy = false}) keep the inline payload and an empty tail.
 */
public final class MQConsumer implements Closeable {

    /** Flush the offset file every N polls (bounded replay window on crash). */
    private static final int FLUSH_EVERY_POLLS = 100;

    private final Transport transport;
    private final String topic;
    private final OffsetStore offsetStore;
    private final int fetchMaxCount;
    private final long requestTimeoutMs;
    /** M4: poison-record failure counter (persisted sidecar). */
    private final RetryTracker retryTracker;
    /** M4: dead-letter log (append-only, per consumer group). */
    private final DeadLetterStore deadLetters;
    /** M4: retries before a record is dead-lettered (>= 1). */
    private final int maxRetries;
    /** M4: alarm hook, may be null. */
    private final DeadLetterListener deadLetterListener;

    /** Global scan position: where the next FETCH starts. */
    private long currentOffset;
    /** End position of the last returned batch; committed on next poll(). */
    private long lastBatchEnd = -1;
    private int pollsSinceFlush;

    /**
     * M1 shape preserved (tests): no retry tracking, no dead-lettering —
     * failures are the caller's business.
     */
    public MQConsumer(Transport transport, String topic, OffsetStore offsetStore,
            int fetchMaxCount, long requestTimeoutMs) {
        this(transport, topic, offsetStore, fetchMaxCount, requestTimeoutMs,
                null, null, 0, null);
    }

    public MQConsumer(Transport transport, String topic, OffsetStore offsetStore,
            int fetchMaxCount, long requestTimeoutMs,
            RetryTracker retryTracker, DeadLetterStore deadLetters,
            int maxRetries, DeadLetterListener deadLetterListener) {
        this.transport = transport;
        this.topic = topic;
        this.offsetStore = offsetStore;
        this.fetchMaxCount = fetchMaxCount;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryTracker = retryTracker;
        this.deadLetters = deadLetters;
        this.maxRetries = Math.max(1, maxRetries);
        this.deadLetterListener = deadLetterListener;
        this.currentOffset = offsetStore.get(topic);
    }

    public static MQConsumer subscribe(String host, int port, String topic, String group,
            File offsetsDir, int fetchMaxCount, long requestTimeoutMs)
            throws InterruptedException {
        return subscribe(host, port, topic, group, offsetsDir, fetchMaxCount,
                requestTimeoutMs, 3, null);
    }

    /** M4 flavor: retry limit + dead-letter alarm, sidecars next to the offsets. */
    public static MQConsumer subscribe(String host, int port, String topic, String group,
            File offsetsDir, int fetchMaxCount, long requestTimeoutMs,
            int maxRetries, DeadLetterListener deadLetterListener)
            throws InterruptedException {
        ClientChannel channel = ClientChannel.connect(host, port, requestTimeoutMs);
        return wire(channel, topic, group, offsetsDir, fetchMaxCount, requestTimeoutMs,
                maxRetries, deadLetterListener);
    }

    /**
     * Cluster flavor (M3): a {@link FailoverTransport} underneath, so a
     * leader change shows up here only as a briefly slow poll — the
     * transport chases the hint (or re-bootstraps) and re-issues the FETCH.
     */
    public static MQConsumer subscribeCluster(java.util.List<java.net.InetSocketAddress> nodes,
            String topic, String group, File offsetsDir, int fetchMaxCount,
            long requestTimeoutMs) {
        return subscribeCluster(nodes, topic, group, offsetsDir, fetchMaxCount,
                requestTimeoutMs, 3, null);
    }

    /** M4 cluster flavor: retry limit + dead-letter alarm. */
    public static MQConsumer subscribeCluster(java.util.List<java.net.InetSocketAddress> nodes,
            String topic, String group, File offsetsDir, int fetchMaxCount,
            long requestTimeoutMs, int maxRetries, DeadLetterListener deadLetterListener) {
        return wire(FailoverTransport.open(nodes, requestTimeoutMs), topic, group,
                offsetsDir, fetchMaxCount, requestTimeoutMs, maxRetries, deadLetterListener);
    }

    private static MQConsumer wire(Transport transport, String topic, String group,
            File offsetsDir, int fetchMaxCount, long requestTimeoutMs,
            int maxRetries, DeadLetterListener deadLetterListener) {
        OffsetStore store = OffsetStore.load(
                new File(offsetsDir, "consumer-offsets-" + group + ".properties"));
        RetryTracker retries = RetryTracker.load(
                new File(offsetsDir, "retry-counts-" + group + ".properties"));
        DeadLetterStore deadLetters;
        try {
            deadLetters = DeadLetterStore.open(
                    new File(offsetsDir, "deadletter-" + group + ".log"));
        } catch (IOException e) {
            throw new IllegalStateException("cannot open dead-letter log for group " + group, e);
        }
        return new MQConsumer(transport, topic, store, fetchMaxCount, requestTimeoutMs,
                retries, deadLetters, maxRetries, deadLetterListener);
    }

    /**
     * One FETCH round. Returns the (possibly empty) records; an empty result
     * means "caught up for now" — the caller decides whether to retry.
     */
    public List<ClientRecord> poll() throws IOException {
        if (lastBatchEnd >= 0) {
            // previous batch is now assumed handled: advance the store
            offsetStore.update(topic, lastBatchEnd);
            // and stop tracking retries for records it covers (they made it)
            if (retryTracker != null) {
                retryTracker.pruneBelow(topic, lastBatchEnd);
            }
            if (++pollsSinceFlush >= FLUSH_EVERY_POLLS) {
                offsetStore.flush();
                if (retryTracker != null) {
                    retryTracker.flush();
                }
                pollsSinceFlush = 0;
            }
        }
        MqProto.FetchRequest req = MqProto.FetchRequest.newBuilder()
                .setTopic(topic)
                .setOffset(currentOffset)
                .setMaxCount(fetchMaxCount)
                .build();
        Frame resp;
        try {
            resp = transport.request(MessageType.FETCH, req.toByteArray())
                    .get(requestTimeoutMs + 500, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IOException("fetch failed", e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("fetch timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while fetching", e);
        }
        ParsedFetch fr = parseFetchBody(resp.body());
        currentOffset = fr.meta.getNextOffset();
        lastBatchEnd = fr.meta.getNextOffset();

        List<ClientRecord> out = new ArrayList<ClientRecord>(fr.meta.getRecordsCount());
        long rawBase = fr.meta.getZeroCopy() && fr.meta.getRecordsCount() > 0
                ? fr.meta.getRecords(0).getOffset()
                : -1L;
        for (MqProto.RecordData r : fr.meta.getRecordsList()) {
            if (!topic.equals(r.getTopic())) { // defensive: broker already filters
                continue;
            }
            byte[] payload;
            if (fr.meta.getZeroCopy()) {
                payload = slicePayload(fr.body, fr.rawStart, fr.rawLen, r, rawBase);
            } else {
                payload = r.getPayload().toByteArray();
            }
            out.add(new ClientRecord(r.getOffset(), r.getTimestamp(),
                    r.getTopic(), payload, r.getKeyHash()));
        }
        return out;
    }

    /**
     * Body layout v1.2: [4B big-endian metaLen][meta protobuf][raw tail].
     * The raw tail is empty for inline ({@code zero_copy = false}) responses.
     * No copy here: the raw tail is read in place from the body — the extra
     * full-tail array copy was the #2 client-side allocation site (JFR).
     */
    static ParsedFetch parseFetchBody(byte[] body) throws IOException {
        if (body.length < 4) {
            throw new IOException("fetch response body too short: " + body.length);
        }
        int metaLen = ByteBuffer.wrap(body).getInt(0);
        if (metaLen < 0 || 4 + metaLen > body.length) {
            throw new IOException("malformed fetch response: metaLen " + metaLen
                    + " exceeds body " + body.length);
        }
        MqProto.FetchResponse meta;
        try {
            meta = MqProto.FetchResponse.parseFrom(ByteBuffer.wrap(body, 4, metaLen));
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("malformed fetch response meta", e);
        }
        int rawStart = 4 + metaLen;
        return new ParsedFetch(meta, body, rawStart, body.length - rawStart);
    }

    /**
     * Slices one record's payload out of the raw tail (in place, at
     * {@code rawStart + (record.offset - rawBase) + record.payload_offset}
     * for {@code record.payload_len} bytes). Out-of-range values mean a
     * broker bug — fail loudly instead of returning garbage. Bounds use
     * rawLen, never body.length: a pooled/oversized carrier must not widen
     * the slice check.
     */
    private static byte[] slicePayload(byte[] body, int rawStart, int rawLen,
            MqProto.RecordData r, long rawBase) throws IOException {
        long rel = r.getOffset() - rawBase + r.getPayloadOffset();
        if (rel < 0 || rel + r.getPayloadLen() > rawLen) {
            throw new IOException("zero-copy record outside raw tail: offset "
                    + r.getOffset() + ", payload at " + rel
                    + "+" + r.getPayloadLen() + ", raw " + rawLen);
        }
        byte[] payload = new byte[r.getPayloadLen()];
        System.arraycopy(body, rawStart + (int) rel, payload, 0, r.getPayloadLen());
        return payload;
    }

    /** Meta plus a VIEW of the raw tail inside the body (see parseFetchBody). */
    static final class ParsedFetch {
        final MqProto.FetchResponse meta;
        final byte[] body;
        /** Start of the raw tail within body: 4 + metaLen. */
        final int rawStart;
        /** Raw tail length: body.length - rawStart. */
        final int rawLen;

        ParsedFetch(MqProto.FetchResponse meta, byte[] body, int rawStart, int rawLen) {
            this.meta = meta;
            this.body = body;
            this.rawStart = rawStart;
            this.rawLen = rawLen;
        }
    }

    /** Explicitly persists the end of the last returned batch. */
    public void commit() throws IOException {
        if (lastBatchEnd >= 0) {
            offsetStore.update(topic, lastBatchEnd);
            if (retryTracker != null) {
                retryTracker.pruneBelow(topic, lastBatchEnd);
                retryTracker.flush();
            }
        }
        offsetStore.flush();
        pollsSinceFlush = 0;
    }

    /**
     * Reports that processing this record failed (M4). The consumer takes
     * it from here:
     *
     * <ul>
     * <li><b>attempts &le; maxRetries</b>: rewinds to the record — the next
     * poll re-fetches from it (records before it in the batch are
     * redelivered too: at-least-once, the usual dedup rules apply). The
     * attempt counter is persisted BEFORE the rewind, so a consumer
     * crash-looping on the same poison pill still converges.</li>
     * <li><b>attempts &gt; maxRetries</b>: appends the record to the
     * dead-letter log (durable), fires the alarm listener, and lets the
     * stream move past it — one poison record can no longer wedge a
     * topic forever.</li>
     * </ul>
     *
     * @return the failure count now on record for this record (1 = first)
     * @throws IllegalStateException when constructed without the M4
     *                               dead-letter machinery (the M1 test shape)
     */
    public int reportFailure(ClientRecord record, Throwable cause) throws IOException {
        if (retryTracker == null || deadLetters == null) {
            throw new IllegalStateException(
                    "dead-letter machinery not enabled: use the M4 constructor");
        }
        int attempts = retryTracker.bump(topic, record.getOffset());
        retryTracker.flush(); // crash here => the attempt still counts
        if (attempts <= maxRetries) {
            // Rewind: next poll re-fetches starting AT the poison record.
            // Committing lastBatchEnd = record offset (not next) keeps
            // everything BEFORE it durable, so a crash mid-retry replays
            // the poison record and nothing earlier.
            currentOffset = record.getOffset();
            lastBatchEnd = record.getOffset();
            return attempts;
        }
        deadLetters.append(record, attempts, cause);
        retryTracker.clear(topic, record.getOffset());
        retryTracker.flush();
        if (deadLetterListener != null) {
            try {
                deadLetterListener.onDeadLetter(record, attempts, cause);
            } catch (RuntimeException alarmBlewUp) {
                // the dead letter is already durable; an alarm that takes
                // the consumer down defeats its own purpose
                // (log to stderr: the client SDK has no logger dependency)
                System.err.println("dead-letter listener threw for " + record
                        + ": " + alarmBlewUp);
            }
        }
        // Offset stays where poll() left it (past the batch): the poison
        // record is skipped, consumption proceeds.
        return attempts;
    }

    /** Failure count on record for this record (0 = never failed; tests). */
    public int attemptsOn(ClientRecord record) {
        if (retryTracker == null) {
            return 0;
        }
        return retryTracker.attempts(topic, record.getOffset());
    }

    /** Persisted (durable) position — what a restart resumes from. */
    public long committedOffset() {
        return offsetStore.get(topic);
    }

    @Override
    public void close() throws IOException {
        try {
            // flush the store as-is: the last batch is NOT committed here,
            // it is only safe to persist once handled (explicit commit()).
            offsetStore.flush();
        } finally {
            try {
                if (deadLetters != null) {
                    deadLetters.close();
                }
            } finally {
                transport.close();
            }
        }
    }
}
