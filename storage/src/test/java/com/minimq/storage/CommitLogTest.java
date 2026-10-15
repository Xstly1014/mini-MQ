package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogTest {

    @TempDir
    File tempDir;

    private static byte[] payload(String s) {
        return s.getBytes();
    }

    @Test
    void freshStoreStartsAtZero() throws IOException {
        CommitLog log = CommitLog.open(tempDir, 1024 * 1024);
        assertEquals(0L, log.maxOffset());
        assertEquals(1, log.segmentCount());
        assertEquals("00000000000000000000", log.segmentNames().get(0));
        assertFalse(log.iterator().hasNext());
        log.close();
    }

    /**
     * Segment-rolling continuity: with a tiny 64 KiB segment and 129 B records
     * the log must roll many times. Global offsets stay monotonic; a gap is
     * allowed only at a segment boundary (the tail hole of a rolled segment).
     * Every record must survive a reopen byte-for-byte.
     */
    @Test
    void segmentRollingKeepsOffsetsContiguous() throws IOException {
        int segmentSize = 64 * 1024;
        int count = 3000; // 3000 x 129B = ~387 KiB => >= 6 segments

        long[] offsets = new long[count];
        long lastEnd = 0;
        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(1_700_000_000_000L + i, i, makePayload(i, 100));
                long offset = log.append(msg);
                if (offset != lastEnd) {
                    assertTrue(offset % segmentSize == 0 && offset > lastEnd,
                            "record " + i + " jumped to " + offset + " after " + lastEnd
                                    + "; gaps are only allowed at segment starts");
                }
                offsets[i] = offset;
                lastEnd = offset + MessageCodec.sizeOf(msg);
            }
            assertTrue(log.segmentCount() >= 6, "expected multiple segments, got " + log.segmentCount());
        }

        // Reopen: recovery must reproduce the exact same stream (offsets included).
        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            Iterator<Message> it = log.iterator();
            int i = 0;
            while (it.hasNext()) {
                Message msg = it.next();
                assertTrue(i < count, "more records than written");
                assertEquals(offsets[i], msg.getOffset(), "offset mismatch at record " + i);
                assertArrayEquals(makePayload(i, 100), msg.getPayload());
                assertEquals(i, msg.getKeyHash());
                i++;
            }
            assertEquals(count, i);
        }
    }

    /**
     * The 100k-record soak test: append 100k fixed-size messages across ~13
     * x 1 MiB segments, reopen, replay the whole log and verify payload and
     * offset sequence end to end.
     */
    @Test
    void hundredThousandRecordReplay() throws IOException {
        int segmentSize = 1024 * 1024;
        int count = 100_000;
        long[] offsets = new long[count];

        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            for (int i = 0; i < count; i++) {
                offsets[i] = log.append(new Message(1L, i, makePayload(i, 100)));
            }
        }

        long probeBefore;
        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            assertTrue(log.segmentCount() >= 10, "expected double-digit segments");
            Iterator<Message> it = log.iterator();
            int i = 0;
            while (it.hasNext()) {
                Message msg = it.next();
                assertTrue(i < count, "more records than written");
                assertEquals(offsets[i], msg.getOffset(), "offset mismatch at record " + i);
                assertArrayEquals(makePayload(i, 100), msg.getPayload(), "payload mismatch at " + i);
                i++;
            }
            assertEquals(count, i, "expected all records back");
            // Core invariant: the next append lands exactly at the current max offset.
            probeBefore = log.maxOffset();
            assertEquals(probeBefore, log.append(new Message(2L, -1, new byte[8])));
        }
    }

    /** Deterministic payload: size fixed, content derived from the index. */
    private static byte[] makePayload(int i, int size) {
        byte[] payload = new byte[size];
        payload[0] = (byte) (i & 0xFF);
        payload[1] = (byte) ((i >> 8) & 0xFF);
        payload[size - 1] = (byte) ~payload[0];
        return payload;
    }

    @Test
    void appendAfterReopenContinuesAtMaxOffset() throws IOException {
        try (CommitLog log = CommitLog.open(tempDir, 64 * 1024)) {
            for (int i = 0; i < 50; i++) {
                log.append(new Message(1L, i, payload("a" + i)));
            }
        }
        try (CommitLog log = CommitLog.open(tempDir, 64 * 1024)) {
            Message msg = new Message(1L, 50, payload("after-reopen"));
            long offset = log.append(msg);
            assertEquals(offset, msg.getOffset());
            // The new record must be readable right after the old ones.
            Iterator<Message> it = log.iterator();
            int seen = 0;
            Message last = null;
            while (it.hasNext()) {
                last = it.next();
                seen++;
            }
            assertEquals(51, seen);
            assertArrayEquals(payload("after-reopen"), last.getPayload());
        }
    }

    @Test
    void recordLargerThanSegmentIsRejected() {
        assertThrows(IOException.class, () -> {
            try (CommitLog log = CommitLog.open(tempDir, 1024)) {
                log.append(new Message(1L, 1, new byte[2048]));
            }
        });
    }
}
