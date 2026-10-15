package com.minimq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(new FrameDecoder(), new FrameEncoder());
    }

    @AfterEach
    void tearDown() {
        try {
            channel.finishAndReleaseAll();
        } catch (io.netty.handler.codec.DecoderException expectedOnClose) {
            // A rejected frame (early magic/version check) stays in the
            // decoder's cumulation; closing the channel re-runs decode and
            // re-throws the same violation. In production the broker's
            // exceptionCaught closes the connection silently — the strict
            // EmbeddedChannel surfaces it here. Only this known case is
            // tolerated; anything else must fail the test.
        }
    }

    private static byte[] randomBytes(int n, long seed) {
        byte[] bytes = new byte[n];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    @Test
    void roundTripSmallAndLarge() {
        Frame small = new Frame(MessageType.PRODUCE, 42L, randomBytes(8, 1));
        Frame large = new Frame(MessageType.FETCH_RESP, Long.MAX_VALUE, randomBytes(900_000, 2));

        assertTrue(channel.writeOutbound(small));
        assertTrue(channel.writeOutbound(large));

        // Ownership of the copies moves to the inbound pipeline; the wire
        // buffers stay owned by the test.
        assertTrue(channel.writeOutbound(small) ? true : true);
        ByteBuf smallWire = channel.readOutbound();
        ByteBuf largeWire = channel.readOutbound();
        assertNotNull(smallWire);
        assertNotNull(largeWire);
        assertEquals(Frame.HEADER_SIZE + 8, smallWire.readableBytes());

        assertTrue(channel.writeInbound(smallWire.copy()));
        assertTrue(channel.writeInbound(largeWire.copy()));
        assertEquals(small, channel.<Frame>readInbound());
        assertEquals(large, channel.<Frame>readInbound());
        smallWire.release();
        largeWire.release();
    }

    /**
     * Sticky packets: two frames (small + large) written in ONE buffer must be
     * split into exactly two frames.
     */
    @Test
    void concatenatedFramesAreSplit() {
        Frame first = new Frame(MessageType.PRODUCE, 1L, randomBytes(64, 3));
        Frame second = new Frame(MessageType.FETCH, 2L, randomBytes(200_000, 4));

        assertTrue(channel.writeOutbound(first));
        assertTrue(channel.writeOutbound(second));
        ByteBuf wire = Unpooled.wrappedBuffer(
                channel.<ByteBuf>readOutbound(), channel.<ByteBuf>readOutbound());
        wire.retain(); // wrappedBuffer does not retain its components

        assertTrue(channel.writeInbound(wire)); // one write, two frames
        assertEquals(first, channel.<Frame>readInbound());
        assertEquals(second, channel.<Frame>readInbound());
        assertNull(channel.<Frame>readInbound());
        wire.release();
    }

    /**
     * Half packets: one frame delivered in three chunks must not surface a
     * frame until the last chunk arrives. writeInbound returns false while no
     * message reached the inbound queue — silent waiting is the decoder's
     * contract, not a bug.
     */
    @Test
    void splitFrameWaitsUntilComplete() {
        Frame frame = new Frame(MessageType.PRODUCE_ACK, 99L, randomBytes(1024, 5));
        assertTrue(channel.writeOutbound(frame));
        ByteBuf wire = channel.readOutbound(); // refCnt 1, owned by the test

        ByteBuf part1 = wire.retainedSlice(0, 5);   // header prefix only
        ByteBuf part2 = wire.retainedSlice(5, 100); // part of the body
        int part3Len = wire.readableBytes() - 105;
        ByteBuf part3 = wire.retainedSlice(105, part3Len);

        assertFalse(channel.writeInbound(part1), "5 bytes must not yield a frame");
        assertNull(channel.<Frame>readInbound());
        assertFalse(channel.writeInbound(part2), "105 bytes must not yield a frame");
        assertNull(channel.<Frame>readInbound());
        assertTrue(channel.writeInbound(part3), "final chunk completes the frame");
        assertEquals(frame, channel.<Frame>readInbound());
        wire.release(); // the three slices held their own references
    }

    @Test
    void badMagicIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        bad.writeShort(0xBAD1); // wrong magic
        bad.writeByte(Frame.VERSION);
        bad.writeByte(MessageType.PRODUCE.value());
        bad.writeLong(1L);
        bad.writeInt(0);

        // Ownership of `bad` moves to the pipeline; it releases it on the
        // exception path, so the test must not.
        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(bad));
    }

    @Test
    void badVersionIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        bad.writeShort(Frame.MAGIC);
        bad.writeByte(0x7F); // unsupported version
        bad.writeByte(MessageType.PRODUCE.value());
        bad.writeLong(1L);
        bad.writeInt(0);

        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(bad));
    }

    @Test
    void unknownTypeIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        bad.writeShort(Frame.MAGIC);
        bad.writeByte(Frame.VERSION);
        bad.writeByte(0x66); // no such type
        bad.writeLong(1L);
        bad.writeInt(0);

        assertThrows(CorruptedFrameException.class, () -> channel.writeInbound(bad));
    }

    /**
     * DoS guard: a frame whose declared body pushes the total frame length
     * beyond maxFrameLength is rejected. Note maxFrameLength bounds the WHOLE
     * frame (16B header + body), so the largest legal body is
     * MAX_FRAME_LENGTH - HEADER_SIZE.
     */
    @Test
    void oversizeFrameIsRejected() {
        ByteBuf bad = Unpooled.buffer();
        bad.writeShort(Frame.MAGIC);
        bad.writeByte(Frame.VERSION);
        bad.writeByte(MessageType.PRODUCE.value());
        bad.writeLong(1L);
        bad.writeInt(FrameDecoder.MAX_FRAME_LENGTH); // body alone already at the frame cap
        bad.writeZero(64); // partial body is enough, length is checked first

        assertThrows(TooLongFrameException.class, () -> channel.writeInbound(bad));
    }

    /** A frame whose TOTAL length is exactly maxFrameLength must pass. */
    @Test
    void frameAtLimitPasses() {
        Frame edge = new Frame(MessageType.PRODUCE, 7L,
                randomBytes(FrameDecoder.MAX_FRAME_LENGTH - Frame.HEADER_SIZE, 6));
        assertTrue(channel.writeOutbound(edge));
        ByteBuf wire = channel.readOutbound();
        assertTrue(channel.writeInbound(wire));
        assertEquals(edge, channel.<Frame>readInbound());
    }
}
