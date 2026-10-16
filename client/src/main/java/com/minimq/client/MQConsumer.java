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
 * at-least-once, duplicates to be deduped by seq key until M4.
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

    /** Global scan position: where the next FETCH starts. */
    private long currentOffset;
    /** End position of the last returned batch; committed on next poll(). */
    private long lastBatchEnd = -1;
    private int pollsSinceFlush;

    public MQConsumer(Transport transport, String topic, OffsetStore offsetStore,
                      int fetchMaxCount, long requestTimeoutMs) {
        this.transport = transport;
        this.topic = topic;
        this.offsetStore = offsetStore;
        this.fetchMaxCount = fetchMaxCount;
        this.requestTimeoutMs = requestTimeoutMs;
        this.currentOffset = offsetStore.get(topic);
    }

    public static MQConsumer subscribe(String host, int port, String topic, String group,
                                       File offsetsDir, int fetchMaxCount, long requestTimeoutMs)
            throws InterruptedException {
        ClientChannel channel = ClientChannel.connect(host, port, requestTimeoutMs);
        OffsetStore store = OffsetStore.load(
                new File(offsetsDir, "consumer-offsets-" + group + ".properties"));
        return new MQConsumer(channel, topic, store, fetchMaxCount, requestTimeoutMs);
    }

    /**
     * Cluster flavor (M3): a {@link FailoverTransport} underneath, so a
     * leader change shows up here only as a briefly slow poll — the
     * transport chases the hint (or re-bootstraps) and re-issues the FETCH.
     */
    public static MQConsumer subscribeCluster(java.util.List<java.net.InetSocketAddress> nodes,
            String topic, String group, File offsetsDir, int fetchMaxCount,
            long requestTimeoutMs) {
        OffsetStore store = OffsetStore.load(
                new File(offsetsDir, "consumer-offsets-" + group + ".properties"));
        return new MQConsumer(FailoverTransport.open(nodes, requestTimeoutMs),
                topic, store, fetchMaxCount, requestTimeoutMs);
    }

    /**
     * One FETCH round. Returns the (possibly empty) records; an empty result
     * means "caught up for now" — the caller decides whether to retry.
     */
    public List<ClientRecord> poll() throws IOException {
        if (lastBatchEnd >= 0) {
            // previous batch is now assumed handled: advance the store
            offsetStore.update(topic, lastBatchEnd);
            if (++pollsSinceFlush >= FLUSH_EVERY_POLLS) {
                offsetStore.flush();
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
                ? fr.meta.getRecords(0).getOffset() : -1L;
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
        }
        offsetStore.flush();
        pollsSinceFlush = 0;
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
            transport.close();
        }
    }
}
