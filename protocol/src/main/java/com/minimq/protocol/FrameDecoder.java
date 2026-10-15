package com.minimq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Splits the byte stream into frames and validates them.
 *
 * LengthFieldBasedFrameDecoder parameters (see docs/protocol.md for the
 * derivation):
 * <ul>
 *   <li>maxFrameLength = 1 MiB — DoS guard, larger frames close the connection,</li>
 *   <li>lengthFieldOffset = 12 — magic(2) + version(1) + type(1) + requestId(8),</li>
 *   <li>lengthFieldLength = 4,</li>
 *   <li>lengthAdjustment = 0 — the length value is the body size, so the full
 *       frame is 12 + 4 + body,</li>
 *   <li>initialBytesToStrip = 0 — the header is still needed to build the Frame.</li>
 * </ul>
 *
 * MAGIC and VERSION are checked as soon as their bytes arrive (before waiting
 * for the whole frame), so garbage connections are rejected early.
 */
public class FrameDecoder extends LengthFieldBasedFrameDecoder {

    public static final int MAX_FRAME_LENGTH = 1024 * 1024;
    private static final int LENGTH_FIELD_OFFSET = 12;
    private static final int LENGTH_FIELD_LENGTH = 4;

    public FrameDecoder() {
        super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, 0, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        // Early magic check: reject garbage as soon as 2 bytes are readable,
        // without waiting for the (possibly huge) frame to complete.
        if (in.readableBytes() >= 2) {
            int magic = in.getUnsignedShort(in.readerIndex());
            if (magic != Frame.MAGIC) {
                throw new CorruptedFrameException("bad magic: 0x" + Integer.toHexString(magic));
            }
        }
        if (in.readableBytes() >= 3 && in.getUnsignedByte(in.readerIndex() + 2) != Frame.VERSION) {
            throw new CorruptedFrameException("unsupported version");
        }

        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null; // half frame: keep waiting, this is normal
        }
        try {
            frame.skipBytes(2); // magic, already validated
            frame.skipBytes(1); // version, already validated
            byte typeValue = frame.readByte();
            MessageType type = MessageType.of(typeValue);
            if (type == null) {
                throw new CorruptedFrameException("unknown type: " + typeValue);
            }
            long requestId = frame.readLong();
            int bodyLength = frame.readInt();
            byte[] body = new byte[bodyLength];
            frame.readBytes(body);
            return new Frame(type, requestId, body);
        } finally {
            frame.release();
        }
    }
}
