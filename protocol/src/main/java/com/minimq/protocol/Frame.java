package com.minimq.protocol;

import java.util.Arrays;

/**
 * Immutable decoded frame: type + request id + body bytes. The body is a
 * defensive copy, so the frame is safe to pass across threads (business pool
 * on the broker, pending-future completion on the client).
 */
public final class Frame {

    public static final int MAGIC = 0xCACE;
    public static final byte VERSION = 0x01;

    /** magic(2) + version(1) + type(1) + requestId(8) = bytes before BODY_LENGTH. */
    public static final int HEADER_SIZE = 16;

    private final MessageType type;
    private final long requestId;
    private final byte[] body;

    public Frame(MessageType type, long requestId, byte[] body) {
        this.type = type;
        this.requestId = requestId;
        this.body = body == null ? new byte[0] : body.clone();
    }

    public MessageType type() {
        return type;
    }

    public long requestId() {
        return requestId;
    }

    public byte[] body() {
        return body;
    }

    @Override
    public String toString() {
        return "Frame{type=" + type + ", requestId=" + requestId
                + ", bodySize=" + body.length + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Frame)) {
            return false;
        }
        Frame f = (Frame) o;
        return requestId == f.requestId && type == f.type && Arrays.equals(body, f.body);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (int) (requestId ^ (requestId >>> 32));
        result = 31 * result + Arrays.hashCode(body);
        return result;
    }
}
