package com.minimq.broker;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.Message;
import com.minimq.storage.MessageCodec;

/**
 * Business-pool request processing. Everything here may block (mmap append,
 * segment roll, batch decode) and therefore never runs on an EventLoop.
 *
 * The handler is a pure Frame -> Frame function; writing the response back is
 * the dispatcher's job, which keeps this class testable without Netty.
 * null means "no response": the client's request timeout retries.
 */
public class BrokerRequestHandler {

    private static final Logger LOG = Logger.getLogger(BrokerRequestHandler.class.getName());

    private final CommitLog commitLog;
    private final int fetchScanBudget;

    public BrokerRequestHandler(CommitLog commitLog, int fetchScanBudget) {
        this.commitLog = commitLog;
        this.fetchScanBudget = fetchScanBudget;
    }

    /** Processes one request frame; null = no response. */
    public Frame handle(Frame request) {
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
                    return new Frame(MessageType.FETCH_RESP, request.requestId(), resp.toByteArray());
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

    private Frame handleFetch(Frame request) {
        MqProto.FetchRequest req;
        try {
            req = MqProto.FetchRequest.parseFrom(request.body());
        } catch (InvalidProtocolBufferException e) {
            // A malformed fetch has no trustworthy next_offset to echo: drop
            // it and let the client timeout retry.
            LOG.log(Level.WARNING, "malformed FETCH: " + request, e);
            return null;
        }
        MqProto.FetchResponse.Builder resp = MqProto.FetchResponse.newBuilder();
        try {
            int maxCount = Math.max(1, req.getMaxCount());
            int budget = Math.max(maxCount, fetchScanBudget);
            List<Message> batch = commitLog.readFrom(req.getOffset(), budget);

            Message lastExamined = null;
            int taken = 0;
            for (Message msg : batch) {
                lastExamined = msg;
                if (req.getTopic().equals(msg.getTopic())) {
                    resp.addRecords(MqProto.RecordData.newBuilder()
                            .setOffset(msg.getOffset())
                            .setTimestamp(msg.getTimestamp())
                            .setTopic(msg.getTopic())
                            .setPayload(com.google.protobuf.ByteString.copyFrom(msg.getPayload()))
                            .setKeyHash(msg.getKeyHash())
                            .build());
                    if (++taken >= maxCount) {
                        break; // quota filled at this record
                    }
                }
            }

            long nextOffset;
            if (taken >= maxCount && lastExamined != null) {
                // Quota filled at the last examined record: resume right
                // after it (records of other topics in between are skipped
                // only after being examined, never skipped unread).
                nextOffset = endOf(lastExamined);
            } else if (lastExamined == null || batch.size() < budget) {
                // Nothing to examine, or the scan reached the log end.
                // Never move the cursor backwards, even for a bogus offset.
                nextOffset = Math.max(req.getOffset(), commitLog.maxOffset());
            } else {
                // The whole scan budget was examined without filling the
                // quota: resume after the last examined record.
                nextOffset = endOf(lastExamined);
            }
            resp.setNextOffset(nextOffset);
            // Equals the log end until M3 replication introduces a real HW.
            resp.setHighWatermark(commitLog.maxOffset());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "FETCH failed: " + request, e);
            return null;
        }
        return new Frame(MessageType.FETCH_RESP, request.requestId(), resp.build().toByteArray());
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

    /** Global byte offset just past this record. */
    private static long endOf(Message msg) {
        return msg.getOffset() + MessageCodec.sizeOf(msg);
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
