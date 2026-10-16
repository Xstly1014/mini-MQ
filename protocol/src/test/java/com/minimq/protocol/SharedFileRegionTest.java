package com.minimq.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.netty.util.IllegalReferenceCountException;

/**
 * SharedFileRegion contract: it borrows a live FileChannel (a CommitLog
 * segment) and must NOT close it on release — that is exactly the M2-2b
 * disconnect regression (DefaultFileRegion.deallocate() closes the channel,
 * the second zero-copy FETCH then dies with ClosedChannelException).
 * transferTo semantics mirror DefaultFileRegion: the position argument is
 * the offset within the region, writes start at region.position + position.
 */
class SharedFileRegionTest {

    @TempDir
    File tempDir;

    private static FileChannel channelWithData(File f, int size) throws Exception {
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(size);
        FileChannel ch = raf.getChannel();
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) i;
        }
        ch.write(ByteBuffer.wrap(data), 0);
        return ch; // caller closes the channel (and the raf with it)
    }

    /** A sink that accepts at most limit bytes per write, like a small socket buffer. */
    private static final class LimitedSink implements WritableByteChannel {
        final ByteBuffer received = ByteBuffer.allocate(64);
        final int limit;
        boolean open = true;

        LimitedSink(int limit) {
            this.limit = limit;
        }

        @Override
        public int write(ByteBuffer src) {
            int n = Math.min(src.remaining(), limit);
            byte[] tmp = new byte[n];
            src.get(tmp);
            received.put(tmp);
            return n;
        }

        @Override public boolean isOpen() { return open; }
        @Override public void close() { open = false; }
    }

    @Test
    void releaseKeepsTheBorrowedChannelOpen() throws Exception {
        File f = new File(tempDir, "seg-open");
        FileChannel ch = channelWithData(f, 64);
        try {
            SharedFileRegion region = new SharedFileRegion(ch, 0, 16);
            assertEquals(1, region.refCnt());
            region.release();
            assertEquals(0, region.refCnt());
            assertTrue(ch.isOpen(), "release must NOT close the shared segment channel");
        } finally {
            ch.close();
        }
    }

    @Test
    void transferToStreamsTheExactSliceAcrossPartialWrites() throws Exception {
        File f = new File(tempDir, "seg-xfer");
        FileChannel ch = channelWithData(f, 64);
        try {
            // slice [8, 24) of the file: bytes 8..23
            SharedFileRegion region = new SharedFileRegion(ch, 8, 16);
            LimitedSink sink = new LimitedSink(6);

            long written = region.transferTo(sink, 0);
            assertEquals(6, written);
            assertEquals(6, region.transferred());

            // Netty resumes with region.transferred() as the position argument
            written = region.transferTo(sink, region.transferred());
            assertEquals(6, written);
            assertEquals(12, region.transferred());

            written = region.transferTo(sink, region.transferred());
            assertEquals(4, written);
            assertEquals(16, region.transferred());

            // fully transferred: further calls are a no-op
            assertEquals(0, region.transferTo(sink, region.transferred()));

            byte[] expected = new byte[16];
            for (int i = 0; i < 16; i++) {
                expected[i] = (byte) (8 + i);
            }
            sink.received.flip();
            byte[] actual = new byte[16];
            sink.received.get(actual);
            assertArrayEquals(expected, actual, "must stream exactly file bytes [8, 24)");
        } finally {
            ch.close();
        }
    }

    @Test
    void transferToAfterReleaseIsRejected() throws Exception {
        File f = new File(tempDir, "seg-refcnt");
        FileChannel ch = channelWithData(f, 64);
        try {
            SharedFileRegion region = new SharedFileRegion(ch, 0, 8);
            region.release();
            assertThrows(IllegalReferenceCountException.class,
                    () -> region.transferTo(new LimitedSink(8), 0));
        } finally {
            ch.close();
        }
    }

    @Test
    void positionOutOfRangeIsRejected() throws Exception {
        File f = new File(tempDir, "seg-range");
        FileChannel ch = channelWithData(f, 64);
        try {
            SharedFileRegion region = new SharedFileRegion(ch, 0, 8);
            assertThrows(IllegalArgumentException.class,
                    () -> region.transferTo(new LimitedSink(8), 9));
            assertThrows(IllegalArgumentException.class,
                    () -> region.transferTo(new LimitedSink(8), -1));
        } finally {
            ch.close();
        }
    }

    @Test
    void worksInsideZeroCopyFrameLifecycle() throws Exception {
        File f = new File(tempDir, "seg-frame");
        FileChannel ch = channelWithData(f, 64);
        try {
            SharedFileRegion a = new SharedFileRegion(ch, 0, 8);
            SharedFileRegion b = new SharedFileRegion(ch, 8, 8);
            ZeroCopyFrame frame = new ZeroCopyFrame(
                    new Frame(MessageType.FETCH_RESP, 1, new byte[4], 16),
                    Arrays.asList(a, b));
            frame.release();
            assertEquals(0, a.refCnt());
            assertEquals(0, b.refCnt());
            assertTrue(ch.isOpen(), "frame release must leave the shared channel open");
        } finally {
            ch.close();
        }
    }
}
