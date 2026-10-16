package com.minimq.protocol;

import java.util.Arrays;

/**
 * Immutable decoded frame: type + request id + body bytes. Safe to pass
 * across threads (business pool on the broker, pending-future completion on
 * the client) because nobody rewrites the body after construction.
 *
 * <b>Ownership transfer (M2 task 3)</b>: the constructor does NOT copy the
 * body — ownership moves from the caller to the frame, and the caller must
 * not keep or modify the array afterwards. Every in-repo call site hands
 * over a freshly built array (decoder read, protobuf toByteArray), so the
 * clone-on-construct that lived here was pure throughput loss: JFR ranked
 * it the #1 allocation site (686MB sampled over one 200k-msg round, 51% of
 * all sampled bytes; see docs/bench-v2.md). If an external caller ever
 * needs to share a mutable buffer, it clones before calling.
 *
 * M2 zero-copy: a frame may declare {@link #trailerLength} extra bytes that
 * the writer appends after {@code body} (see {@link ZeroCopyFrame}). The
 * on-wire BODY_LENGTH covers body + trailer, so the peer's FrameDecoder
 * reads the trailer into its body as ordinary bytes — the split is purely a
 * sender-side optimization (the trailer goes out via sendfile).
 */
public final class Frame {
    public static final int MAGIC = 0xCACE;
    public static final byte VERSION = 0x01;

    /** magic(2) + version(1) + type(1) + requestId(8) = bytes before BODY_LENGTH. */
    public static final int HEADER_SIZE = 16;

    private final MessageType type;
    private final long requestId;
    private final byte[] body;
    /** Bytes the writer appends after body (zero-copy raw tail), see class doc. */
    private final int trailerLength;

    public Frame(MessageType type, long requestId, byte[] body) {
        this(type, requestId, body, 0);
    }

    public Frame(MessageType type, long requestId, byte[] body, int trailerLength) {
        this.type = type;
        this.requestId = requestId;
        this.body = body == null ? new byte[0] : body;
        this.trailerLength = trailerLength;
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

    /** Bytes following body on the wire (sender-side zero-copy tail). */
    public int trailerLength() {
        return trailerLength;
    }

    /** Total BODY_LENGTH on the wire: body + trailer. */
    public int frameBodyLength() {
        return body.length + trailerLength;
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
