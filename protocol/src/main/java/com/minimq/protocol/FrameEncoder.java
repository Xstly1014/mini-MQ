package com.minimq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Writes a frame: MAGIC | VERSION | TYPE | REQUEST_ID | BODY_LENGTH | BODY.
 * All multi-byte fields are big-endian (network convention; the storage layer
 * is little-endian by design — the two conventions never mix, see
 * docs/protocol.md).
 *
 * BODY_LENGTH covers {@link Frame#frameBodyLength()} — body plus the
 * zero-copy trailer the writer appends after this buffer (see
 * {@link ZeroCopyFrame}); the peer therefore reads the whole thing as one
 * frame. The trailer bytes themselves are NOT written here.
 *
 * The length field is written here by hand; do NOT add a
 * LengthFieldPrepender to the pipeline (double length prefix bug).
 */
public class FrameEncoder extends MessageToByteEncoder<Frame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame msg, ByteBuf out) {
        byte[] body = msg.body();
        out.writeShort(Frame.MAGIC);
        out.writeByte(Frame.VERSION);
        out.writeByte(msg.type().value());
        out.writeLong(msg.requestId());
        out.writeInt(msg.frameBodyLength());
        out.writeBytes(body);
    }
}
