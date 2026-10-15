package com.minimq.client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
        MqProto.FetchResponse fr;
        try {
            fr = MqProto.FetchResponse.parseFrom(resp.body());
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("malformed fetch response", e);
        }
        currentOffset = fr.getNextOffset();
        lastBatchEnd = fr.getNextOffset();

        List<ClientRecord> out = new ArrayList<ClientRecord>(fr.getRecordsCount());
        for (MqProto.RecordData r : fr.getRecordsList()) {
            if (topic.equals(r.getTopic())) { // defensive: broker already filters
                out.add(new ClientRecord(r.getOffset(), r.getTimestamp(),
                        r.getTopic(), r.getPayload().toByteArray(), r.getKeyHash()));
            }
        }
        return out;
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
