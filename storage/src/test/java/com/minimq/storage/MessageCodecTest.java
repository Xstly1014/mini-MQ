package com.minimq.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MessageCodecTest {

    private static ByteBuffer wrap(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    void roundTripPreservesAllFields() {
        Random random = new Random(42);
        for (int i = 0; i < 1000; i++) {
            byte[] payload = new byte[random.nextInt(200)];
            random.nextBytes(payload);
            long ts = random.nextLong();
            long keyHash = random.nextLong();
            Message msg = new Message(ts, keyHash, payload);

            byte[] encoded = MessageCodec.encode(msg);
            assertEquals(MessageCodec.HEADER_SIZE + payload.length, encoded.length);

            MessageCodec.DecodeResult r = MessageCodec.decode(wrap(encoded), 0);
            assertTrue(r.isOk(), "record " + i + " should decode");
            assertEquals(encoded.length, r.size);
            assertEquals(ts, r.message.getTimestamp());
            assertEquals(keyHash, r.message.getKeyHash());
            assertArrayEquals(payload, r.message.getPayload());
        }
    }

    @Test
    void flippedPayloadByteIsCorrupt() {
        Message msg = new Message(123L, 456L, "hello mini-mq".getBytes());
        byte[] encoded = MessageCodec.encode(msg);
        encoded[MessageCodec.OFFSET_PAYLOAD + 3] ^= 0x55;
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(wrap(encoded), 0).status);
    }

    @Test
    void flippedHeaderByteIsCorrupt() {
        Message msg = new Message(123L, 456L, new byte[64]);
        byte[] encoded = MessageCodec.encode(msg);
        encoded[MessageCodec.OFFSET_TIMESTAMP + 1] ^= 0x01; // inside CRC coverage
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(wrap(encoded), 0).status);
    }

    @Test
    void tornTailIsIncomplete() {
        Message msg = new Message(1L, 2L, new byte[100]);
        byte[] encoded = MessageCodec.encode(msg);
        byte[] torn = Arrays.copyOf(encoded, encoded.length - 30); // last 30 bytes lost
        assertEquals(MessageCodec.DecodeStatus.INCOMPLETE, MessageCodec.decode(wrap(torn), 0).status);
    }

    @Test
    void tornHeaderIsIncomplete() {
        Message msg = new Message(1L, 2L, new byte[100]);
        byte[] encoded = MessageCodec.encode(msg);
        byte[] torn = Arrays.copyOf(encoded, MessageCodec.HEADER_SIZE - 1);
        assertEquals(MessageCodec.DecodeStatus.INCOMPLETE, MessageCodec.decode(wrap(torn), 0).status);
    }

    @Test
    void zeroRegionIsEmpty() {
        Message msg = new Message(1L, 2L, new byte[10]);
        byte[] encoded = MessageCodec.encode(msg);
        byte[] withHole = Arrays.copyOf(encoded, encoded.length + 64); // trailing zeros
        MessageCodec.DecodeResult ok = MessageCodec.decode(wrap(withHole), 0);
        assertTrue(ok.isOk());
        assertEquals(MessageCodec.DecodeStatus.EMPTY,
                MessageCodec.decode(wrap(withHole), ok.size).status);
    }

    @Test
    void wrongMagicIsCorrupt() {
        Message msg = new Message(1L, 2L, new byte[10]);
        byte[] encoded = MessageCodec.encode(msg);
        encoded[MessageCodec.OFFSET_MAGIC] = (byte) (MessageCodec.MAGIC + 1);
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(wrap(encoded), 0).status);
    }

    @Test
    void inconsistentPayloadLenIsCorrupt() {
        Message msg = new Message(1L, 2L, new byte[10]);
        byte[] encoded = MessageCodec.encode(msg);
        ByteBuffer buf = wrap(encoded);
        buf.putInt(MessageCodec.OFFSET_PAYLOAD_LEN, 11); // lies about payload length
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(buf, 0).status);
    }

    @Test
    void garbageTotalSizeIsCorrupt() {
        ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0, -1); // negative size
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(buf, 0).status);
        buf.putInt(0, MessageCodec.MAX_MESSAGE_SIZE + 1);
        assertEquals(MessageCodec.DecodeStatus.CORRUPT, MessageCodec.decode(buf, 0).status);
    }

    @Test
    void oversizeMessageRejectedAtEncode() {
        assertThrows(IllegalArgumentException.class,
                () -> MessageCodec.encode(new Message(1L, 2L, new byte[MessageCodec.MAX_MESSAGE_SIZE])));
    }
}
