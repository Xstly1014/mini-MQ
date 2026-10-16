package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.ZeroCopyFrame;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;
import com.minimq.storage.Message;
import com.minimq.storage.MessageCodec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * Zero-copy FETCH semantics (M2 task 2b): the response is a head frame whose
 * BODY_LENGTH covers a raw tail that travels as FileRegions. The wire shape
 * must stay ONE frame (the peer's decoder reassembles it), the meta must be
 * self-describing enough for a consumer to slice payloads without knowing
 * the storage layout, and the raw tail must be the verbatim CommitLog bytes.
 */
class ZeroCopyFetchTest {

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
        handler = new BrokerRequestHandler(commitLog, consumeQueues,
                FlushMode.OS_FLUSH, 5_000L, true);
    }

    @AfterEach
    void tearDown() throws IOException {
        consumeQueues.close();
        commitLog.close();
    }

    private void awaitIndex() throws InterruptedException {
        assertTrue(consumeQueues.awaitDispatchedTo(commitLog.maxOffset(), 5_000),
                "index dispatch fell behind the log");
    }

    /** Pulls the region's bytes through transferTo (what the socket does). */
    private static byte[] drain(FileRegion r) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WritableByteChannel out = Channels.newChannel(baos);
        try {
            long pos = 0;
            while (pos < r.count()) {
                long n = r.transferTo(out, pos);
                if (n <= 0) {
                    break;
                }
                pos += n;
            }
        } finally {
            out.close();
        }
        return baos.toByteArray();
    }

    @Test
    void zeroCopyResponseCarriesMetaHeadAndVerbatimRawRegions() throws Exception {
        Frame ack1 = (Frame) handler.handle(TestFrames.produce(1, "t1", "a"));
        Frame ack2 = (Frame) handler.handle(TestFrames.produce(2, "t2", "b"));
        Frame ack3 = (Frame) handler.handle(TestFrames.produce(3, "t1", "c"));
        long oA = TestFrames.parseAck(ack1).getOffsets(0);
        long oB = TestFrames.parseAck(ack2).getOffsets(0);
        long oC = TestFrames.parseAck(ack3).getOffsets(0);
        awaitIndex();

        Object resp = handler.handle(TestFrames.fetch(10, "t1", 0, 10));
        ZeroCopyFrame zc = assertInstanceOf(ZeroCopyFrame.class, resp,
                "zeroCopyFetch=true must answer with a ZeroCopyFrame");

        // ---- head: header + meta, trailerLength announces the raw tail ----
        Frame head = zc.head();
        assertEquals(MessageType.FETCH_RESP, head.type());
        assertEquals(10, head.requestId());
        assertTrue(head.trailerLength() > 0, "raw tail length must be announced");

        MqProto.FetchResponse meta = TestFrames.parseFetch(head);
        assertTrue(meta.getZeroCopy());
        assertEquals(2, meta.getRecordsCount(), "only t1 records are listed");
        assertEquals(commitLog.maxOffset(), meta.getHighWatermark());
        for (MqProto.RecordData r : meta.getRecordsList()) {
            assertEquals("t1", r.getTopic());
            assertTrue(r.getPayload().isEmpty(), "zero-copy records carry no inline payload");
            assertTrue(r.getSize() > 0);
            assertTrue(r.getPayloadOffset() > 0, "payload position must be self-describing");
            assertTrue(r.getPayloadLen() > 0);
        }
        assertEquals(oA, meta.getRecords(0).getOffset());
        assertEquals(oC, meta.getRecords(1).getOffset());
        // next_offset = end of the LAST listed record, not the raw span end
        long aEnd = oA + MessageCodec.sizeOf(readMsg(oA));
        long cEnd = oC + MessageCodec.sizeOf(readMsg(oC));
        assertEquals(cEnd, meta.getNextOffset());

        // ---- regions: together exactly the CommitLog span [oA, cEnd) ----
        StringBuilder raw = new StringBuilder();
        long covered = 0;
        for (FileRegion region : zc.regions()) {
            byte[] bytes = drain(region);
            raw.append(new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1));
            covered += bytes.length;
        }
        assertEquals(head.trailerLength(), (int) covered,
                "regions must cover exactly the announced trailer");
        byte[] rawBytes = raw.toString().getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);

        // The raw tail is the VERBATIM log span: [a][b][c] in log order,
        // including the foreign t2 record in between (oB is inside [oA,cEnd)).
        assertTrue(oB > oA && oB < cEnd, "t2 record must sit inside the raw span");
        ByteBuffer buf = ByteBuffer.wrap(rawBytes).order(ByteOrder.LITTLE_ENDIAN);
        Message m1 = MessageCodec.decode(buf, 0).message;
        assertEquals("t1", m1.getTopic());
        assertEquals("a", new String(m1.getPayload(), "UTF-8"));
        Message m2 = MessageCodec.decode(buf, MessageCodec.sizeOf(m1)).message;
        assertEquals("t2", m2.getTopic());
        assertEquals("b", new String(m2.getPayload(), "UTF-8"));
        Message m3 = MessageCodec.decode(buf,
                MessageCodec.sizeOf(m1) + MessageCodec.sizeOf(m2)).message;
        assertEquals("t1", m3.getTopic());
        assertEquals("c", new String(m3.getPayload(), "UTF-8"));

        // Meta's payload_offset/payload_len must slice exactly the payloads
        assertEquals("a", new String(java.util.Arrays.copyOfRange(rawBytes,
                (int) (meta.getRecords(0).getOffset() - oA) + meta.getRecords(0).getPayloadOffset(),
                (int) (meta.getRecords(0).getOffset() - oA) + meta.getRecords(0).getPayloadOffset()
                        + meta.getRecords(0).getPayloadLen()), "UTF-8"));
        assertEquals("c", new String(java.util.Arrays.copyOfRange(rawBytes,
                (int) (meta.getRecords(1).getOffset() - oA) + meta.getRecords(1).getPayloadOffset(),
                (int) (meta.getRecords(1).getOffset() - oA) + meta.getRecords(1).getPayloadOffset()
                        + meta.getRecords(1).getPayloadLen()), "UTF-8"));
    }

    private Message readMsg(long offset) throws IOException {
        List<Message> one = commitLog.readFrom(offset, 1);
        assertEquals(1, one.size());
        return one.get(0);
    }

    @Test
    void zeroCopyWritesOutAsOneWireFrameThroughThePipeline() throws Exception {
        handler.handle(TestFrames.produce(1, "t1", "hello"));
        awaitIndex();

        // Direct executor: handle() runs inline on the test thread, so the
        // outbound queue is immediately readable — no polling needed.
        Executor direct = new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };
        EmbeddedChannel broker = new EmbeddedChannel(
                new FrameEncoder(), new DispatcherHandler(direct, handler));
        broker.writeInbound(TestFrames.fetch(21, "t1", 0, 10));

        // Outbound sequence: the encoded head ByteBuf, then FileRegions.
        Object first = broker.readOutbound();
        ByteBuf headBuf = assertInstanceOf(ByteBuf.class, first,
                "head must go out as an ordinary encoded frame");
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        byte[] headBytes = new byte[headBuf.readableBytes()];
        headBuf.readBytes(headBytes);
        wire.write(headBytes);
        int regionCount = 0;
        for (Object o = broker.readOutbound(); o != null; o = broker.readOutbound()) {
            FileRegion region = assertInstanceOf(FileRegion.class, o);
            wire.write(drain(region));
            region.release();
            regionCount++;
        }
        assertTrue(regionCount >= 1);
        broker.close();

        // Feed the concatenated bytes through the REAL decoder: it must see
        // one ordinary frame whose body is [metaLen][meta][raw].
        EmbeddedChannel decoder = new EmbeddedChannel(new FrameDecoder());
        decoder.writeInbound(Unpooled.wrappedBuffer(wire.toByteArray()));
        Frame reassembled = (Frame) decoder.readInbound();
        assertNotNull(reassembled, "head + regions must decode as ONE frame");
        decoder.close();

        assertEquals(MessageType.FETCH_RESP, reassembled.type());
        assertEquals(21, reassembled.requestId());
        MqProto.FetchResponse meta = TestFrames.parseFetch(reassembled);
        assertTrue(meta.getZeroCopy());
        assertEquals(1, meta.getRecordsCount());

        // Slice the payload from the reassembled body, consumer-style: the
        // raw tail follows [4B metaLen][meta] inside the SAME body.
        byte[] body = reassembled.body();
        int metaLen = ByteBuffer.wrap(body).getInt(0);
        int rawStart = 4 + metaLen;
        MqProto.RecordData rec = meta.getRecords(0);
        assertEquals("hello", new String(body, rawStart + rec.getPayloadOffset(),
                rec.getPayloadLen(), "UTF-8"));
    }

    @Test
    void singleRecordThatCannotFitTheBudgetFallsBackToInline() throws Exception {
        // Budget arithmetic: the record's rawLen + per-record reserve must
        // exceed the zero-copy budget (MAX_FRAME - HEADER - 4 - 512), while
        // BOTH the produce frame and the inline FETCH_RESP frame still fit
        // under MAX_FRAME_LENGTH. The window is ~600 bytes wide; 1_048_050
        // sits in the middle of it. The record also needs a segment bigger
        // than the class default (256KB) — a single record may not span
        // segments — hence this test's own 2MB fixture.
        byte[] big = new byte[1_048_050];
        java.util.Arrays.fill(big, (byte) 'x');
        CommitLog bigLog = CommitLog.open(tempDir.resolve("biglog").toFile(), 2 * 1024 * 1024);
        ConsumeQueueStore bigQueues =
                ConsumeQueueStore.open(tempDir.resolve("bigcq").toFile(), bigLog);
        try {
            BrokerRequestHandler bigHandler = new BrokerRequestHandler(
                    bigLog, bigQueues, FlushMode.OS_FLUSH, 5_000L, true);
            Frame produce = new Frame(MessageType.PRODUCE, 1,
                    MqProto.ProduceRequest.newBuilder()
                            .addMessages(MqProto.MessageData.newBuilder()
                                    .setTopic("t1")
                                    .setPayload(com.google.protobuf.ByteString.copyFrom(big))
                                    .setSeqKey("big")
                                    .setTimestamp(1L))
                            .build().toByteArray());
            Frame ack = (Frame) bigHandler.handle(produce);
            assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(ack).getStatus());
            assertTrue(bigQueues.awaitDispatchedTo(bigLog.maxOffset(), 5_000),
                    "index dispatch fell behind the log");

            Object resp = bigHandler.handle(TestFrames.fetch(10, "t1", 0, 10));
            // The fallback is the inline path: an ordinary Frame with payload.
            Frame inline = assertInstanceOf(Frame.class, resp,
                    "oversized zero-copy batch must fall back to inline");
            MqProto.FetchResponse meta = TestFrames.parseFetch(inline);
            assertFalse(meta.getZeroCopy());
            assertEquals(1, meta.getRecordsCount());
            assertEquals(big.length, meta.getRecords(0).getPayload().size());
        } finally {
            bigQueues.close();
            bigLog.close();
        }
    }
}
