package com.minimq.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.SharedFileRegion;
import com.minimq.protocol.ZeroCopyFrame;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueue;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;
import com.minimq.storage.Message;
import com.minimq.storage.MessageCodec;

import io.netty.channel.FileRegion;

/**
 * Business-pool request processing. Everything here may block (mmap append,
 * segment roll, batch decode, SYNC_FLUSH durability wait) and therefore never
 * runs on an EventLoop.
 *
 * Group-commit ack gating (M2): in SYNC_FLUSH the business thread parks in
 * {@link CommitLog#awaitDurable} after appending — MySQL Follower semantics;
 * one force by the flush thread wakes every producer whose batch it covers.
 * The park occupies a business-pool thread, which is exactly the aggregation
 * mechanism: more concurrent producers -> bigger groups per force.
 *
 * Indexed FETCH (M2 task 2a): the request offset goes through the topic's
 * {@link ConsumeQueueStore} — O(log n) locate, then exact per-record reads.
 * next_offset advances to the END of the last returned entry; when the index
 * has nothing more, next_offset stays put (the dispatcher is at most
 * milliseconds behind the log; the consumer simply polls again).
 *
 * Zero-copy FETCH (M2 task 2b, {@code zeroCopyFetch}): when enabled the
 * response is a {@link ZeroCopyFrame} — meta (headers only, CRC-validated
 * via {@link CommitLog#decodeHeaderAt}) travels in the frame head while the
 * raw record span goes out as FileRegions (sendfile: page cache -> socket,
 * the broker never copies the payload into user space). Both paths share the
 * on-wire body layout [4B metaLen][meta][raw] (see mq.proto), so the
 * consumer parses them identically; the legacy path simply inlines payloads
 * into the meta and ships an empty raw tail. The raw span may contain
 * foreign-topic records in between — the consumer decodes only the offsets
 * listed in meta.
 *
 * The handler is a pure request -> response function; writing the response
 * back is the dispatcher's job, which keeps this class testable without
 * Netty. null means "no response": the client's request timeout retries.
 */
public class BrokerRequestHandler {

    private static final Logger LOG = Logger.getLogger(BrokerRequestHandler.class.getName());

    /**
     * Frame-budget arithmetic: BODY_LENGTH must stay under the decoder's
     * max-frame guard even for the zero-copy trailer. Meta is protobuf
     * (varint-heavy) — reserve a generous fixed estimate plus per-record
     * slack; overestimating only shortens a batch, never breaks a frame.
     */
    private static final int META_FIXED_RESERVE = 512;
    private static final int META_PER_RECORD_RESERVE = 128;

    private final CommitLog commitLog;
    private final ConsumeQueueStore consumeQueues;
    private final FlushMode flushMode;
    private final long flushAwaitTimeoutMs;
    private final boolean zeroCopyFetch;

    public BrokerRequestHandler(CommitLog commitLog, ConsumeQueueStore consumeQueues,
            FlushMode flushMode, long flushAwaitTimeoutMs, boolean zeroCopyFetch) {
        this.commitLog = commitLog;
        this.consumeQueues = consumeQueues;
        this.flushMode = flushMode;
        this.flushAwaitTimeoutMs = flushAwaitTimeoutMs;
        this.zeroCopyFetch = zeroCopyFetch;
    }

    /** Processes one request frame; null = no response. */
    public Object handle(Frame request) {
        switch (request.type()) {
            case PRODUCE:
                return handleProduce(request);
            case FETCH:
                return handleFetch(request);
            case COMMIT_OFFSET:
                return handleCommitOffset(request);
            case HEARTBEAT:
            default:
                return null; // HEARTBEAT is reserved for M3 leases
        }
    }

    /**
     * Response sent when the business pool rejects the task (backpressure):
     * RETRY_LATER tells the producer to back off and resend; an empty fetch
     * response echoes the request offset so the consumer simply polls again.
     */
    public Frame busyResponse(Frame request) {
        switch (request.type()) {
            case PRODUCE: {
                MqProto.ProduceAck.Builder ack = MqProto.ProduceAck.newBuilder()
                        .setStatus(MqProto.ProduceStatus.RETRY_LATER)
                        .setError("broker busy");
                try {
                    // Small protobuf parse (microseconds): acceptable on the
                    // EventLoop-only rejection path, keeps the ack contract
                    // exact (one offset entry per message, -1 = not appended).
                    MqProto.ProduceRequest req = MqProto.ProduceRequest.parseFrom(request.body());
                    for (int i = 0; i < req.getMessagesCount(); i++) {
                        ack.addOffsets(-1L);
                    }
                } catch (InvalidProtocolBufferException ignored) {
                    // offsets stay empty; the status carries the signal
                }
                return new Frame(MessageType.PRODUCE_ACK, request.requestId(), ack.build().toByteArray());
            }
            case FETCH: {
                try {
                    MqProto.FetchRequest req = MqProto.FetchRequest.parseFrom(request.body());
                    MqProto.FetchResponse resp = MqProto.FetchResponse.newBuilder()
                            .setNextOffset(req.getOffset())
                            .setHighWatermark(commitLog.maxOffset())
                            .build();
                    // v1.2 layout like every other FETCH_RESP (the consumer
                    // parses the 4B metaLen prefix unconditionally).
                    return prefixedFrame(MessageType.FETCH_RESP, request.requestId(), resp);
                } catch (InvalidProtocolBufferException e) {
                    return null;
                }
            }
            case COMMIT_OFFSET: {
                MqProto.CommitOffsetAck ack = MqProto.CommitOffsetAck.newBuilder().setOk(false).build();
                return new Frame(MessageType.COMMIT_OFFSET_ACK, request.requestId(), ack.toByteArray());
            }
            default:
                return null;
        }
    }

    /** Response for a request whose processing threw; null = stay silent. */
    public Frame errorResponse(Frame request, Throwable error) {
        switch (request.type()) {
            case PRODUCE: {
                MqProto.ProduceAck ack = MqProto.ProduceAck.newBuilder()
                        .setStatus(MqProto.ProduceStatus.ERROR)
                        .setError(String.valueOf(error))
                        .build();
                return new Frame(MessageType.PRODUCE_ACK, request.requestId(), ack.toByteArray());
            }
            case FETCH:
                // FetchResponse has no error field; silence is safer than a
                // misleading next_offset. The client timeout retries the fetch.
                return null;
            case COMMIT_OFFSET: {
                MqProto.CommitOffsetAck ack = MqProto.CommitOffsetAck.newBuilder().setOk(false).build();
                return new Frame(MessageType.COMMIT_OFFSET_ACK, request.requestId(), ack.toByteArray());
            }
            default:
                return null;
        }
    }

    private Frame handleProduce(Frame request) {
        MqProto.ProduceAck ack;
        try {
            MqProto.ProduceRequest req = MqProto.ProduceRequest.parseFrom(request.body());
            MqProto.ProduceAck.Builder builder = MqProto.ProduceAck.newBuilder()
                    .setStatus(MqProto.ProduceStatus.OK);
            long now = System.currentTimeMillis();
            for (MqProto.MessageData data : req.getMessagesList()) {
                Message msg = new Message(
                        data.getTimestamp() != 0 ? data.getTimestamp() : now,
                        hash64(data.getSeqKey()),
                        data.getTopic(),
                        data.getPayload().toByteArray());
                builder.addOffsets(commitLog.append(msg));
            }
            // SYNC_FLUSH gate: the batch is durable only when the flush
            // thread's frontier passes it. Parking here is the group-commit
            // aggregation (Follower waits for the Leader's force).
            if (flushMode == FlushMode.SYNC_FLUSH && req.getMessagesCount() > 0
                    && !commitLog.awaitDurable(commitLog.maxOffset(), flushAwaitTimeoutMs)) {
                // Timeout: the records ARE appended (a resend will duplicate —
                // at-least-once, dedup arrives in M4), but this ack must not
                // claim durability it cannot prove.
                ack = MqProto.ProduceAck.newBuilder()
                        .setStatus(MqProto.ProduceStatus.ERROR)
                        .setError("flush await timeout")
                        .build();
                return new Frame(MessageType.PRODUCE_ACK, request.requestId(), ack.toByteArray());
            }
            ack = builder.build();
        } catch (Exception e) {
            // Partial batch failure: messages appended before the failure stay
            // (offsets list is short); the client resends the whole batch and
            // may duplicate — at-least-once until the M4 dedup window.
            LOG.log(Level.WARNING, "PRODUCE failed: " + request, e);
            ack = MqProto.ProduceAck.newBuilder()
                    .setStatus(MqProto.ProduceStatus.ERROR)
                    .setError(String.valueOf(e))
                    .build();
        }
        return new Frame(MessageType.PRODUCE_ACK, request.requestId(), ack.toByteArray());
    }

    private Object handleFetch(Frame request) {
        MqProto.FetchRequest req;
        try {
            req = MqProto.FetchRequest.parseFrom(request.body());
        } catch (InvalidProtocolBufferException e) {
            // A malformed fetch has no trustworthy next_offset to echo: drop
            // it and let the client timeout retry.
            LOG.log(Level.WARNING, "malformed FETCH: " + request, e);
            return null;
        }
        try {
            int maxCount = Math.max(1, req.getMaxCount());
            List<ConsumeQueue.Entry> entries =
                    consumeQueues.readFrom(req.getTopic(), req.getOffset(), maxCount);

            if (zeroCopyFetch && !entries.isEmpty()) {
                ZeroCopyFrame zc = zeroCopyFetchResponse(request, req, entries);
                if (zc != null) {
                    return zc;
                }
                // else: the batch could not fit the frame budget (single
                // near-MAX_MESSAGE_SIZE record) — fall through to inline.
            }
            return inlineFetchResponse(request, req, entries);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "FETCH failed: " + request, e);
            return null;
        }
    }

    /**
     * Zero-copy path: meta carries headers only (CRC-validated, payload
     * untouched), the raw span [first.offset, last.end) rides out as
     * FileRegions. Batches are truncated to the frame budget BEFORE the
     * regions are built — a trailer that overshoots the decoder's
     * max-frame guard would kill the connection.
     */
    private ZeroCopyFrame zeroCopyFetchResponse(Frame request, MqProto.FetchRequest req,
            List<ConsumeQueue.Entry> entries) throws IOException {
        // spanEnd(i) = entries[i].end(): the raw tail covers everything up
        // to and including record i (foreign records in between included).
        long rawStart = entries.get(0).offset;
        long budget = FrameDecoder.MAX_FRAME_LENGTH - Frame.HEADER_SIZE - 4
                - META_FIXED_RESERVE - META_PER_RECORD_RESERVE;
        int keep = 0;
        while (keep < entries.size()
                && entries.get(keep).end() - rawStart + (keep + 1) * META_PER_RECORD_RESERVE <= budget) {
            keep++;
        }
        if (keep == 0) {
            // A single record cannot fit the frame budget (payload close to
            // MAX_MESSAGE_SIZE): fall back to the inline path, which can at
            // least return the record if it fits, and drops it otherwise.
            return null; // caller falls through to inlineFetchResponse
        }
        List<ConsumeQueue.Entry> kept = entries.subList(0, keep);
        long rawEnd = kept.get(kept.size() - 1).end();
        int rawLen = (int) (rawEnd - rawStart);

        MqProto.FetchResponse.Builder meta = MqProto.FetchResponse.newBuilder()
                .setZeroCopy(true)
                .setNextOffset(rawEnd)
                .setHighWatermark(commitLog.maxOffset());
        for (ConsumeQueue.Entry e : kept) {
            MessageCodec.HeaderResult h = commitLog.decodeHeaderAt(e.offset);
            if (h.status != MessageCodec.DecodeStatus.OK || h.size != e.size) {
                // index/log divergence must be impossible after open
                // alignment + replay; skip the entry, keep the cursor moving
                LOG.warning("index entry at " + e.offset + " fails header check: " + h.status);
                continue;
            }
            meta.addRecords(MqProto.RecordData.newBuilder()
                    .setOffset(e.offset)
                    .setTimestamp(h.timestamp)
                    .setTopic(h.topic)
                    .setKeyHash(h.keyHash)
                    .setSize(e.size)
                    .setPayloadOffset(h.payloadOffset)
                    .setPayloadLen(h.payloadLen)
                    .build());
        }

        byte[] metaBytes = meta.build().toByteArray();
        byte[] headBody = new byte[4 + metaBytes.length];
        ByteBuffer.wrap(headBody).putInt(metaBytes.length).put(metaBytes);

        List<CommitLog.FileSlice> slices = commitLog.fileSlicesFor(rawStart, rawEnd);
        List<FileRegion> regions = new ArrayList<FileRegion>(slices.size());
        long covered = 0;
        for (CommitLog.FileSlice s : slices) {
            // SharedFileRegion: the slice borrows the segment's live channel;
            // DefaultFileRegion would CLOSE it on release (M2-2b bug: the
            // first zero-copy FETCH killed the segment channel, the second
            // died with ClosedChannelException and dropped the connection).
            regions.add(new SharedFileRegion(s.channel, s.position, s.count));
            covered += s.count;
        }
        if (covered != rawLen) {
            // Segments must tile the span exactly; anything else is a bug we
            // refuse to put on the wire as a frame with a lying length.
            for (FileRegion r : regions) {
                r.release();
            }
            throw new IllegalStateException("slices covered " + covered
                    + "B, expected " + rawLen + "B for span [" + rawStart + "," + rawEnd + ")");
        }
        Frame head = new Frame(MessageType.FETCH_RESP, request.requestId(), headBody, rawLen);
        return new ZeroCopyFrame(head, regions);
    }

    /**
     * Inline path (M1 shape under the v1.2 body layout): payloads travel
     * inside the protobuf meta, no raw tail. Also the fallback when the
     * zero-copy batch cannot fit the frame budget.
     */
    private Frame inlineFetchResponse(Frame request, MqProto.FetchRequest req,
            List<ConsumeQueue.Entry> entries) {
        MqProto.FetchResponse.Builder resp = MqProto.FetchResponse.newBuilder();
        // next_offset stays at the request offset when nothing matched: the
        // dispatcher may simply be behind, and rewinding/jumping the consumer
        // cursor would skip records the index hasn't reached.
        long nextOffset = req.getOffset();
        int budget = FrameDecoder.MAX_FRAME_LENGTH - Frame.HEADER_SIZE - 4
                - META_FIXED_RESERVE - META_PER_RECORD_RESERVE;
        int used = 0;
        for (ConsumeQueue.Entry e : entries) {
            List<Message> one = commitLog.readFrom(e.offset, 1);
            if (!one.isEmpty() && one.get(0).getOffset() == e.offset) {
                Message msg = one.get(0);
                int cost = MessageCodec.sizeOf(msg) + META_PER_RECORD_RESERVE;
                if (used + cost > budget && used > 0) {
                    break; // next fetch resumes from this entry
                }
                used += cost;
                resp.addRecords(MqProto.RecordData.newBuilder()
                        .setOffset(msg.getOffset())
                        .setTimestamp(msg.getTimestamp())
                        .setTopic(msg.getTopic())
                        .setPayload(com.google.protobuf.ByteString.copyFrom(msg.getPayload()))
                        .setKeyHash(msg.getKeyHash())
                        .build());
            } else {
                // Index/log divergence must be impossible after the open
                // alignment + replay; skip the entry, keep the cursor moving.
                LOG.warning("index entry at " + e.offset + " has no backing record");
            }
            nextOffset = e.end();
        }
        resp.setNextOffset(nextOffset);
        // Equals the log end until M3 replication introduces a real HW.
        resp.setHighWatermark(commitLog.maxOffset());
        return prefixedFrame(MessageType.FETCH_RESP, request.requestId(), resp.build());
    }

    /**
     * Body layout v1.2: [4B metaLen][meta]. Single allocation:
     * getSerializedSize() precomputes the length, then the proto streams
     * straight into the body tail — no intermediate toByteArray() copy (JFR
     * ranked that double allocation #2/#3 on the inline FETCH path).
     */
    private static Frame prefixedFrame(MessageType type, long requestId,
            com.google.protobuf.GeneratedMessageV3 message) {
        int metaLen = message.getSerializedSize();
        byte[] body = new byte[4 + metaLen];
        ByteBuffer.wrap(body).putInt(metaLen);
        try {
            message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(body, 4, metaLen));
        } catch (java.io.IOException e) {
            throw new RuntimeException("protobuf write to heap array failed", e); // cannot happen
        }
        return new Frame(type, requestId, body);
    }

    private Frame handleCommitOffset(Frame request) {
        // Validated and acknowledged; broker-side offset storage arrives in M3
        // (M1 consumers persist offsets locally via the SDK OffsetStore).
        MqProto.CommitOffsetAck.Builder ack = MqProto.CommitOffsetAck.newBuilder();
        try {
            MqProto.CommitOffsetRequest.parseFrom(request.body());
            ack.setOk(true);
        } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.WARNING, "malformed COMMIT_OFFSET: " + request, e);
            ack.setOk(false);
        }
        return new Frame(MessageType.COMMIT_OFFSET_ACK, request.requestId(), ack.build().toByteArray());
    }

    /**
     * FNV-1a 64 of the seq key, stored in the CommitLog KEY_HASH field.
     * Deterministic within this codebase; cross-language stability is not
     * required before the M4 dedup window.
     */
    private static long hash64(String key) {
        if (key == null || key.isEmpty()) {
            return 0L;
        }
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            hash ^= key.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
