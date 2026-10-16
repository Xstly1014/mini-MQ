package com.minimq.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;

/**
 * Zero-copy frame lifecycle: regions are ReferenceCounted file-handle owners.
 * A frame that is built but never written MUST be releasable by hand, and
 * release() must tolerate regions that somebody else already released.
 */
class ZeroCopyFrameTest {

    @TempDir
    File tempDir;

    private FileRegion region(FileChannel ch, int count) {
        return new DefaultFileRegion(ch, 0, count);
    }

    @Test
    void releaseDropsEveryRegionsRefCount() throws Exception {
        File f = new File(tempDir, "seg");
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(64);
            FileChannel ch = raf.getChannel();
            FileRegion a = region(ch, 16);
            FileRegion b = region(ch, 16);
            ZeroCopyFrame frame = new ZeroCopyFrame(
                    new Frame(MessageType.FETCH_RESP, 1, new byte[8], 32),
                    Arrays.asList(a, b));

            assertEquals(1, a.refCnt());
            assertEquals(1, b.refCnt());
            frame.release();
            assertEquals(0, a.refCnt(), "release must drop every region");
            assertEquals(0, b.refCnt());
            // idempotent: already-released regions must not be double-released
            // (that would throw IllegalReferenceCountException)
            frame.release();
        }
    }

    @Test
    void headCarriesTrailerLengthAndBodyIsUntouched() {
        Frame head = new Frame(MessageType.FETCH_RESP, 7, new byte[]{1, 2, 3}, 4096);
        ZeroCopyFrame frame = new ZeroCopyFrame(head, java.util.Collections.<FileRegion>emptyList());

        assertEquals(head, frame.head());
        assertEquals(4096, frame.head().trailerLength());
        assertEquals(3 + 4096, frame.head().frameBodyLength(),
                "wire BODY_LENGTH must cover body + trailer so the peer reads one frame");
    }
}
