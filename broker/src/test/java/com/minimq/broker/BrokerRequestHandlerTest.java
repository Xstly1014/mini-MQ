package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;
import com.minimq.storage.Message;

class BrokerRequestHandlerTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    private CommitLog commitLog;
    private ConsumeQueueStore consumeQueues;
    private BrokerRequestHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        commitLog = CommitLog.open(tempDir.resolve("log").toFile(), SEGMENT_SIZE);
        consumeQueues = ConsumeQueueStore.open(tempDir.resolve("cq").toFile(), commitLog);
        handler = new BrokerRequestHandler(commitLog, consumeQueues, FlushMode.OS_FLUSH, 5_000L, false);
    }

    @AfterEach
    void tearDown() throws IOException {
        consumeQueues.close();
        commitLog.close();
    }

    /** FETCH correctness depends on the index; wait for the dispatcher. */
    private void awaitIndex() throws InterruptedException {
        assertTrue(consumeQueues.awaitDispatchedTo(commitLog.maxOffset(), 5_000),
                "index dispatch fell behind the log");
    }

    @Test
    void produceAppendsAndAcksMonotonicOffsets() throws Exception {
        Frame ack = (Frame) handler.handle(TestFrames.produce(1, "orders", "a", "b", "c"));

        assertEquals(MessageType.PRODUCE_ACK, ack.type());
        assertEquals(1, ack.requestId());
        MqProto.ProduceAck parsed = TestFrames.parseAck(ack);
        assertEquals(MqProto.ProduceStatus.OK, parsed.getStatus());
        assertEquals(3, parsed.getOffsetsCount());
        long prev = -1;
        for (long o : parsed.getOffsetsList()) {
            assertTrue(o > prev, "offsets must be strictly increasing");
            prev = o;
        }

        // the batch really hit the disk
        List<Message> stored = commitLog.readFrom(parsed.getOffsets(0), 3);
        assertEquals(3, stored.size());
        assertEquals("orders", stored.get(0).getTopic());
        assertEquals("a", new String(stored.get(0).getPayload(),
                java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void produceGarbageBodyReturnsErrorAck() throws Exception {
        Frame ack = (Frame) handler.handle(new Frame(MessageType.PRODUCE, 7, new byte[]{1, 2, 3}));

        assertEquals(MessageType.PRODUCE_ACK, ack.type());
        assertEquals(MqProto.ProduceStatus.ERROR, TestFrames.parseAck(ack).getStatus());
        assertEquals(7, ack.requestId());
    }

    @Test
    void fetchFiltersByTopicAndReportsLogEnd() throws Exception {
        handler.handle(TestFrames.produce(1, "t1", "m1"));
        handler.handle(TestFrames.produce(2, "t2", "m2"));
        handler.handle(TestFrames.produce(3, "t1", "m3"));
        awaitIndex();

        Frame resp = (Frame) handler.handle(TestFrames.fetch(10, "t1", 0, 10));
        assertEquals(MessageType.FETCH_RESP, resp.type());
        assertEquals(10, resp.requestId());

        MqProto.FetchResponse parsed = TestFrames.parseFetch(resp);
        assertEquals(2, parsed.getRecordsCount());
        assertEquals("m1", parsed.getRecords(0).getPayload().toStringUtf8());
        assertEquals("m3", parsed.getRecords(1).getPayload().toStringUtf8());
        assertEquals("t1", parsed.getRecords(0).getTopic());
        // t1's last entry is the log's last record: cursor lands on the log end
        assertEquals(commitLog.maxOffset(), parsed.getNextOffset());
        assertEquals(commitLog.maxOffset(), parsed.getHighWatermark());
        // key hash flows end-to-end (M4 idempotency dependency)
        assertTrue(parsed.getRecords(0).getKeyHash() != 0);
    }

    @Test
    void fetchQuotaStopsRightAfterLastMatchedRecord() throws Exception {
        long o1 = TestFrames.parseAck((Frame) handler.handle(TestFrames.produce(1, "t1", "m1")))
                .getOffsets(0);
        long o2 = TestFrames.parseAck((Frame) handler.handle(TestFrames.produce(2, "t2", "m2")))
                .getOffsets(0);
        long o3 = TestFrames.parseAck((Frame) handler.handle(TestFrames.produce(3, "t1", "m3")))
                .getOffsets(0);
        assertTrue(o3 > o2 && o2 > o1);
        awaitIndex();

        // maxCount = 1: quota fills at m1; next_offset must sit exactly on
        // m2 (the next record), NOT on m3 — the t2 record in between must
        // not be skipped unread.
        Frame resp = (Frame) handler.handle(TestFrames.fetch(10, "t1", 0, 1));
        MqProto.FetchResponse parsed = TestFrames.parseFetch(resp);
        assertEquals(1, parsed.getRecordsCount());
        assertEquals("m1", parsed.getRecords(0).getPayload().toStringUtf8());
        assertEquals(o2, parsed.getNextOffset(), "resume position must be the record after m1");

        // continuing from there returns exactly m3
        MqProto.FetchResponse next = TestFrames.parseFetch(
                (Frame) handler.handle(TestFrames.fetch(11, "t1", parsed.getNextOffset(), 10)));
        assertEquals(1, next.getRecordsCount());
        assertEquals("m3", next.getRecords(0).getPayload().toStringUtf8());
        assertEquals(commitLog.maxOffset(), next.getNextOffset());
    }

    @Test
    void fetchJumpsPastForeignTopicFloodViaIndex() throws Exception {
        // The M1 killer: a flood of foreign-topic records before the wanted
        // one. The M1 scan had to examine them all (bounded by a budget, in
        // rounds); the index hands the t1 entry over directly — one FETCH,
        // zero foreign records decoded.
        for (int i = 0; i < 500; i++) {
            handler.handle(TestFrames.produce(100 + i, "t2", "flood-" + i));
        }
        long t1Offset = TestFrames.parseAck((Frame) handler.handle(
                TestFrames.produce(200, "t1", "wanted"))).getOffsets(0);
        awaitIndex();

        Frame resp = (Frame) handler.handle(TestFrames.fetch(10, "t1", 0, 1));
        MqProto.FetchResponse parsed = TestFrames.parseFetch(resp);
        assertEquals(1, parsed.getRecordsCount(), "index must reach t1 in one hop");
        assertEquals("wanted", parsed.getRecords(0).getPayload().toStringUtf8());
        assertEquals(t1Offset, parsed.getRecords(0).getOffset());
        assertEquals(commitLog.maxOffset(), parsed.getNextOffset());
    }

    @Test
    void fetchBeyondEndReturnsEmptyWithoutMovingBackwards() throws Exception {
        handler.handle(TestFrames.produce(1, "t", "x"));
        long maxOffset = commitLog.maxOffset();

        MqProto.FetchResponse parsed = TestFrames.parseFetch(
                (Frame) handler.handle(TestFrames.fetch(10, "t", maxOffset + 1000, 10)));
        assertEquals(0, parsed.getRecordsCount());
        assertEquals(maxOffset + 1000, parsed.getNextOffset(),
                "beyond-end fetch must not rewind the consumer cursor");
    }

    @Test
    void fetchMalformedBodyIsDropped() {
        assertNull(handler.handle(new Frame(MessageType.FETCH, 1, new byte[]{9, 9})));
    }

    @Test
    void commitOffsetIsAcknowledged() throws Exception {
        MqProto.CommitOffsetRequest req = MqProto.CommitOffsetRequest.newBuilder()
                .setTopic("t").setConsumerGroup("g").setOffset(42).build();
        Frame ack = (Frame) handler.handle(
                new Frame(MessageType.COMMIT_OFFSET, 5, req.toByteArray()));

        assertEquals(MessageType.COMMIT_OFFSET_ACK, ack.type());
        assertEquals(5, ack.requestId());
        assertTrue(MqProto.CommitOffsetAck.parseFrom(ack.body()).getOk());
    }

    @Test
    void heartbeatHasNoResponse() {
        assertNull(handler.handle(new Frame(MessageType.HEARTBEAT, 1, new byte[0])));
    }

    @Test
    void busyProduceIsRetryLaterWithNothingAppended() throws Exception {
        Frame busy = handler.busyResponse(TestFrames.produce(3, "t", "x", "y"));

        assertEquals(MessageType.PRODUCE_ACK, busy.type());
        assertEquals(3, busy.requestId());
        MqProto.ProduceAck parsed = TestFrames.parseAck(busy);
        assertEquals(MqProto.ProduceStatus.RETRY_LATER, parsed.getStatus());
        assertEquals(2, parsed.getOffsetsCount());
        assertEquals(-1L, parsed.getOffsets(0));
        assertEquals(-1L, parsed.getOffsets(1));
    }

    @Test
    void busyFetchEchoesRequestOffsetAsEmptyResponse() throws Exception {
        Frame busy = handler.busyResponse(TestFrames.fetch(4, "t", 77, 8));

        assertEquals(MessageType.FETCH_RESP, busy.type());
        MqProto.FetchResponse parsed = TestFrames.parseFetch(busy);
        assertEquals(0, parsed.getRecordsCount());
        assertEquals(77, parsed.getNextOffset());
        assertEquals(4, busy.requestId());
    }
}
