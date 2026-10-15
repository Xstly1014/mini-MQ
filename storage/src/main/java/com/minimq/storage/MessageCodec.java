package com.minimq.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Wire format (little-endian), fixed 29-byte header + variable payload:
 *
 * <pre>
 * | field        | offset | size | notes                                   |
 * |--------------|--------|------|-----------------------------------------|
 * | TOTAL_SIZE   | 0      | 4B   | header + payload, i.e. the record size |
 * | MAGIC        | 4      | 1B   | 0x6D ('m')                              |
 * | CRC32        | 5      | 4B   | over bytes [MAGIC .. end of record]     |
 * | TIMESTAMP    | 9      | 8B   | producer-side epoch millis              |
 * | KEY_HASH     | 17     | 8B   | hash of the message key                 |
 * | PAYLOAD_LEN  | 25     | 4B   | must equal TOTAL_SIZE - HEADER_SIZE     |
 * | PAYLOAD      | 29     | n B  | opaque bytes                            |
 * </pre>
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

    public static final int MAGIC = 0x6D;

    public static final int HEADER_SIZE = 29;
    public static final int OFFSET_TOTAL_SIZE = 0;
    public static final int OFFSET_MAGIC = 4;
    public static final int OFFSET_CRC = 5;
    public static final int OFFSET_TIMESTAMP = 9;
    public static final int OFFSET_KEY_HASH = 17;
    public static final int OFFSET_PAYLOAD_LEN = 25;
    public static final int OFFSET_PAYLOAD = HEADER_SIZE;

    /**
     * Upper bound of one record; guards against garbage TOTAL_SIZE values during
     * recovery.
     */
    public static final int MAX_MESSAGE_SIZE = 4 * 1024 * 1024;

    private MessageCodec() {
    }

    public static int sizeOf(Message msg) {
        return HEADER_SIZE + msg.getPayload().length;
    }

    /**
     * Encodes the message into a fresh heap array. The CRC covers everything
     * except TOTAL_SIZE and the CRC field itself, i.e. the MAGIC byte plus
     * [TIMESTAMP .. payload end] (two contiguous ranges around the CRC slot),
     * mirroring decode() exactly.
     */
    public static byte[] encode(Message msg) {
        byte[] payload = msg.getPayload();
        int total = HEADER_SIZE + payload.length;
        if (total > MAX_MESSAGE_SIZE) {
            throw new IllegalArgumentException("message too large: " + total + " > " + MAX_MESSAGE_SIZE);
        }
        byte[] out = new byte[total];
        ByteBuffer buf = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(OFFSET_TOTAL_SIZE, total);
        buf.put(OFFSET_MAGIC, (byte) MAGIC);
        buf.putLong(OFFSET_TIMESTAMP, msg.getTimestamp());
        buf.putLong(OFFSET_KEY_HASH, msg.getKeyHash());
        buf.putInt(OFFSET_PAYLOAD_LEN, payload.length);
        System.arraycopy(payload, 0, out, OFFSET_PAYLOAD, payload.length);

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
        int payloadLen = buf.getInt(pos + OFFSET_PAYLOAD_LEN);
        if (payloadLen != total - HEADER_SIZE) {
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
        byte[] payload = new byte[payloadLen];
        ByteBuffer payloadView = buf.duplicate();
        payloadView.position(pos + OFFSET_PAYLOAD);
        payloadView.get(payload);
        long timestamp = buf.getLong(pos + OFFSET_TIMESTAMP);
        long keyHash = buf.getLong(pos + OFFSET_KEY_HASH);
        return new DecodeResult(DecodeStatus.OK, total, new Message(timestamp, keyHash, payload));
    }
}
