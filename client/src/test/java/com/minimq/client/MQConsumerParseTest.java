package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import com.minimq.protocol.gen.MqProto;

/**
 * FETCH_RESP body layout v1.2 parsing: [4B metaLen][meta][raw]. The consumer
 * must slice zero-copy payloads out of the raw tail purely from meta fields,
 * and reject malformed bodies loudly (a wrong slice is silent data
 * corruption — the worst possible broker-client bug).
 */
class MQConsumerParseTest {

    private static byte[] body(MqProto.FetchResponse meta, byte[] raw) {
        byte[] metaBytes = meta.toByteArray();
        byte[] body = new byte[4 + metaBytes.length + raw.length];
        ByteBuffer.wrap(body).putInt(metaBytes.length).put(metaBytes)
                .put(raw);
        return body;
    }

    /** A CommitLog-like record: fake layout, only offsets matter for slicing. */
    private static byte[] record(String payload) {
        byte[] p = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // [4B total][2B junk][8B timestamp][8B keyHash][2B topicLen][4B payloadLen][topic][payload]
        byte[] topic = "t".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 2 + 8 + 8 + 2 + 4 + topic.length + p.length);
        buf.putInt(buf.capacity()).putShort((short) 1).putLong(1L).putLong(2L)
                .putShort((short) topic.length).putInt(p.length).put(topic).put(p);
        return buf.array();
    }

    @Test
    void parsesInlineResponseWithEmptyRawTail() throws Exception {
        MqProto.FetchResponse meta = MqProto.FetchResponse.newBuilder()
                .setZeroCopy(false)
                .setNextOffset(100)
                .addRecords(MqProto.RecordData.newBuilder()
                        .setOffset(0).setTopic("t")
                        .setPayload(com.google.protobuf.ByteString.copyFromUtf8("hello")))
                .build();
        MQConsumer.ParsedFetch parsed = MQConsumer.parseFetchBody(body(meta, new byte[0]));

        assertEquals(100, parsed.meta.getNextOffset());
        assertEquals(0, parsed.rawLen);
        assertEquals("hello", parsed.meta.getRecords(0).getPayload().toStringUtf8());
    }

    @Test
    void slicesZeroCopyPayloadsFromTheRawTail() throws Exception {
        // Two wanted records around one foreign record, like a mixed log:
        // raw = [rec1][recForeign][rec2], meta lists rec1 + rec2 only.
        byte[] rec1 = record("alpha");
        byte[] recForeign = record("FOREIGN-TOPIC-RECORD");
        byte[] rec2 = record("beta");
        byte[] raw = new byte[rec1.length + recForeign.length + rec2.length];
        System.arraycopy(rec1, 0, raw, 0, rec1.length);
        System.arraycopy(recForeign, 0, raw, rec1.length, recForeign.length);
        System.arraycopy(rec2, 0, raw, rec1.length + recForeign.length, rec2.length);

        long base = 500; // first listed record's global offset
        MqProto.RecordData r1 = MqProto.RecordData.newBuilder()
                .setOffset(base).setTopic("t")
                .setSize(rec1.length)
                .setPayloadOffset(rec1.length - "alpha".getBytes().length)
                .setPayloadLen("alpha".getBytes().length)
                .build();
        MqProto.RecordData r2 = MqProto.RecordData.newBuilder()
                .setOffset(base + rec1.length + recForeign.length).setTopic("t")
                .setSize(rec2.length)
                .setPayloadOffset(rec2.length - "beta".getBytes().length)
                .setPayloadLen("beta".getBytes().length)
                .build();
        MqProto.FetchResponse meta = MqProto.FetchResponse.newBuilder()
                .setZeroCopy(true)
                .setNextOffset(r2.getOffset() + rec2.length)
                .addRecords(r1).addRecords(r2)
                .build();

        MQConsumer.ParsedFetch parsed = MQConsumer.parseFetchBody(body(meta, raw));
        assertEquals(1, r1.getPayloadOffset() + r1.getPayloadLen(), rec1.length);
        assertArrayEquals(rec1, java.util.Arrays.copyOfRange(raw, 0, rec1.length));
        // the tail is a view into body, not a copy
        assertEquals(raw.length, parsed.rawLen);
        assertEquals(parsed.body.length, parsed.rawStart + parsed.rawLen);

        // slicing math the consumer performs in poll() — in place, at rawStart
        assertEquals("alpha", new String(java.util.Arrays.copyOfRange(parsed.body,
                parsed.rawStart + (int) (r1.getOffset() - base) + r1.getPayloadOffset(),
                parsed.rawStart + (int) (r1.getOffset() - base) + r1.getPayloadOffset() + r1.getPayloadLen()),
                "UTF-8"));
        assertEquals("beta", new String(java.util.Arrays.copyOfRange(parsed.body,
                parsed.rawStart + (int) (r2.getOffset() - base) + r2.getPayloadOffset(),
                parsed.rawStart + (int) (r2.getOffset() - base) + r2.getPayloadOffset() + r2.getPayloadLen()),
                "UTF-8"));
    }

    @Test
    void rejectsBodiesTooShortForThePrefix() {
        assertThrows(IOException.class, () -> MQConsumer.parseFetchBody(new byte[3]));
        assertThrows(IOException.class, () -> MQConsumer.parseFetchBody(new byte[0]));
    }

    @Test
    void rejectsMetaLenBeyondTheBody() {
        byte[] body = new byte[8];
        ByteBuffer.wrap(body).putInt(100); // claims 100B meta in an 8B body
        assertThrows(IOException.class, () -> MQConsumer.parseFetchBody(body));
    }
}
