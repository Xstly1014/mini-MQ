package com.minimq.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Wire format v1.1 (little-endian), fixed 31-byte header + variable topic and
 * payload:
 *
 * <pre>
 * | field        | offset | size | notes                                        |
 * |--------------|--------|------|----------------------------------------------|
 * | TOTAL_SIZE   | 0      | 4B   | whole record size (header + topic + payload) |
 * | MAGIC        | 4      | 1B   | 0x6E (format v1.1; v1.0 was 0x6D, no topic)  |
 * | CRC32        | 5      | 4B   | over MAGIC + [TIMESTAMP .. end of record]    |
 * | TIMESTAMP    | 9      | 8B   | producer-side epoch millis                   |
 * | KEY_HASH     | 17     | 8B   | hash of the idempotency (seq) key            |
 * | TOPIC_LEN    | 25     | 2B   | topic length in bytes                        |
 * | PAYLOAD_LEN  | 27     | 4B   | payload length in bytes                      |
 * | TOPIC        | 31     | n B  | UTF-8 topic                                  |
 * | PAYLOAD      | 31+n   | m B  | opaque bytes                                 |
 * </pre>
 *
 * v1.0 -> v1.1: TOPIC_LEN + TOPIC added for topic routing over the network
 * path (M1). MAGIC bumped so the two layouts can never be confused.
 *
 * Decode distinguishes:
 * <ul>
 * <li>{@code OK} - full valid record,</li>
 * <li>{@code EMPTY} - 4-byte zero header, i.e. untouched pre-allocated
 * space,</li>
 * <li>{@code INCOMPLETE} - torn write (record header present, tail
 * missing),</li>
 * <li>{@code CORRUPT} - magic / size / length / CRC mismatch.</li>
 * </ul>
 */
public final class MessageCodec {

    public static final int MAGIC = 0x6E;

    public static final int HEADER_SIZE = 31;
    public static final int OFFSET_TOTAL_SIZE = 0;
    public static final int OFFSET_MAGIC = 4;
    public static final int OFFSET_CRC = 5;
    public static final int OFFSET_TIMESTAMP = 9;
    public static final int OFFSET_KEY_HASH = 17;
    public static final int OFFSET_TOPIC_LEN = 25;
    public static final int OFFSET_PAYLOAD_LEN = 27;
    public static final int FIXED_HEADER_SIZE = HEADER_SIZE; // topic starts here

    /**
     * Upper bound of one record; guards against garbage TOTAL_SIZE values during
     * recovery.
     */
    public static final int MAX_MESSAGE_SIZE = 4 * 1024 * 1024;

    /** Upper bound of one topic string (bytes). */
    public static final int MAX_TOPIC_LENGTH = 1024;

    private MessageCodec() {
    }

    public static int sizeOf(Message msg) {
        byte[] topicBytes = topicBytes(msg.getTopic());
        return HEADER_SIZE + topicBytes.length + msg.getPayload().length;
    }

    private static byte[] topicBytes(String topic) {
        if (topic == null) {
            topic = "";
        }
        byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("topic too long: " + bytes.length + " > " + MAX_TOPIC_LENGTH);
        }
        return bytes;
    }

    /**
     * Encodes the message into a fresh heap array. The CRC covers everything
     * except TOTAL_SIZE and the CRC field itself, i.e. the MAGIC byte plus
     * [TIMESTAMP .. record end] (two contiguous ranges around the CRC slot),
     * mirroring decode() exactly.
     */
    public static byte[] encode(Message msg) {
        byte[] payload = msg.getPayload();
        byte[] topic = topicBytes(msg.getTopic());
        int total = HEADER_SIZE + topic.length + payload.length;
        if (total > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("message too large: " + total + " > " + MAX_MESSAGE_SIZE);
        }
        byte[] out = new byte[total];
        ByteBuffer buf = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(OFFSET_TOTAL_SIZE, total);
        buf.put(OFFSET_MAGIC, (byte) MAGIC);
        buf.putLong(OFFSET_TIMESTAMP, msg.getTimestamp());
        buf.putLong(OFFSET_KEY_HASH, msg.getKeyHash());
        buf.putShort(OFFSET_TOPIC_LEN, (short) topic.length);
        buf.putInt(OFFSET_PAYLOAD_LEN, payload.length);
        System.arraycopy(topic, 0, out, HEADER_SIZE, topic.length);
        System.arraycopy(payload, 0, out, HEADER_SIZE + topic.length, payload.length);

        CRC32 crc32 = new CRC32();
        crc32.update(out, OFFSET_MAGIC, OFFSET_CRC - OFFSET_MAGIC);
        crc32.update(out, OFFSET_TIMESTAMP, total - OFFSET_TIMESTAMP);
        buf.putInt(OFFSET_CRC, (int) crc32.getValue());
        return out;
    }

    public enum DecodeStatus {
        OK, EMPTY, INCOMPLETE, CORRUPT
    }

    public static final class DecodeResult {
        public final DecodeStatus status;
        /** Record size in bytes; only meaningful when status == OK. */
        public final int size;
        public final Message message;

        private DecodeResult(DecodeStatus status, int size, Message message) {
            this.status = status;
            this.size = size;
            this.message = message;
        }

        static final DecodeResult EMPTY_RES = new DecodeResult(DecodeStatus.EMPTY, 0, null);
        static final DecodeResult INCOMPLETE_RES = new DecodeResult(DecodeStatus.INCOMPLETE, 0, null);
        static final DecodeResult CORRUPT_RES = new DecodeResult(DecodeStatus.CORRUPT, 0, null);

        public boolean isOk() {
            return status == DecodeStatus.OK;
        }
    }

    /**
     * Decodes the record starting at absolute position {@code pos} of {@code buf}.
     * The buffer must be in little-endian order (see {@link MappedFile#slice});
     * this method never moves {@code buf.position()}.
     */
    public static DecodeResult decode(ByteBuffer buf, int pos) {
        int limit = buf.limit();
        if (limit - pos < HEADER_SIZE) {
            return DecodeResult.INCOMPLETE_RES;
        }
        int total = buf.getInt(pos + OFFSET_TOTAL_SIZE);
        if (total == 0) {
            return DecodeResult.EMPTY_RES;
        }
        if (total < HEADER_SIZE || total > MAX_MESSAGE_SIZE) {
            return DecodeResult.CORRUPT_RES;
        }
        if (limit - pos < total) {
            return DecodeResult.INCOMPLETE_RES;
        }
        int magic = buf.get(pos + OFFSET_MAGIC) & 0xFF;
        if (magic != MAGIC) {
            return DecodeResult.CORRUPT_RES;
        }
        int topicLen = buf.getShort(pos + OFFSET_TOPIC_LEN) & 0xFFFF;
        int payloadLen = buf.getInt(pos + OFFSET_PAYLOAD_LEN);
        if (topicLen > MAX_TOPIC_LENGTH
                || payloadLen < 0
                || HEADER_SIZE + topicLen + payloadLen != total) {
            return DecodeResult.CORRUPT_RES;
        }
        CRC32 crc32 = new CRC32();
        ByteBuffer head = buf.duplicate();
        head.position(pos + OFFSET_MAGIC).limit(pos + OFFSET_CRC);
        crc32.update(head);
        ByteBuffer body = buf.duplicate();
        body.position(pos + OFFSET_TIMESTAMP).limit(pos + total);
        crc32.update(body);
        if ((int) crc32.getValue() != buf.getInt(pos + OFFSET_CRC)) {
            return DecodeResult.CORRUPT_RES;
        }
        byte[] topic = new byte[topicLen];
        byte[] payload = new byte[payloadLen];
        ByteBuffer topicView = buf.duplicate();
        topicView.position(pos + HEADER_SIZE);
        topicView.get(topic);
        if (payloadLen > 0) {
            ByteBuffer payloadView = buf.duplicate();
            payloadView.position(pos + HEADER_SIZE + topicLen);
            payloadView.get(payload);
        }
        long timestamp = buf.getLong(pos + OFFSET_TIMESTAMP);
        long keyHash = buf.getLong(pos + OFFSET_KEY_HASH);
        return new DecodeResult(DecodeStatus.OK, total,
                new Message(timestamp, keyHash, new String(topic, StandardCharsets.UTF_8), payload));
    }
}
