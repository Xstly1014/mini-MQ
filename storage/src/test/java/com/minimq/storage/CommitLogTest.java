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
     * Segment-rolling continuity: with a tiny 64 KiB segment and 135 B records
     * the log must roll many times. Global offsets stay monotonic; a gap is
     * allowed only at a segment boundary (the tail hole of a rolled segment).
     * Every record must survive a reopen byte-for-byte.
     */
    @Test
    void segmentRollingKeepsOffsetsContiguous() throws IOException {
        int segmentSize = 64 * 1024;
        int count = 3000; // 3000 x 135B = ~398 KiB => >= 6 segments

        long[] offsets = new long[count];
        long lastEnd = 0;
        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(1_700_000_000_000L + i, i, "roll", makePayload(i, 100));
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
                assertEquals("roll", msg.getTopic());
                assertArrayEquals(makePayload(i, 100), msg.getPayload());
                assertEquals(i, msg.getKeyHash());
                i++;
            }
            assertEquals(count, i);
        }
    }

    /**
     * The 2 GiB boundary regression (M2 soak bug): readFrom used
     * {@code idx * segmentSize} — an int product — to decide whether the
     * cursor falls inside a segment. At global offset 2^31 (segment 2 of
     * 1 GiB) the product overflows negative, the range check failed and pos
     * silently reset to 0: the ConsumeQueue dispatcher re-read the segment
     * head forever, never advancing (consume starved, CPU spun). Only a
     * store past 2 GiB can hit it, so the segment chain is hand-built here
     * (sparse pre-allocated files) instead of appending 2 GiB of data.
     */
    @Test
    void readFromLocatesOffsetsPastThe2GiBIntOverflowBoundary() throws IOException {
        int segSize = CommitLog.DEFAULT_SEGMENT_SIZE; // 1 GiB: segment 2 spans 2^31
        for (long base : new long[] { 0L, 1L << 30, 1L << 31 }) {
            MappedFile seg = new MappedFile(new File(tempDir, MappedFile.fileName(base)), segSize);
            seg.close(); // keep the sparse pre-allocated file, release the mapping
        }
        try (CommitLog log = CommitLog.open(tempDir, segSize)) {
            assertEquals(3, log.segmentCount());
            Message first = new Message(1L, 1, "big", makePayload(1, 32));
            Message second = new Message(2L, 2, "big", makePayload(2, 32));
            long off1 = log.append(first);
            long off2 = log.append(second);
            assertEquals(1L << 31, off1, "appends must land in the hand-built 3rd segment");

            // read from the segment head and from inside it: both must return
            // records at/after the cursor, not rewound segment-head data
            assertEquals(off1, log.readFrom(off1, 8).get(0).getOffset());
            assertEquals(off2, log.readFrom(off2, 8).get(0).getOffset());
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
                offsets[i] = log.append(new Message(1L, i, "soak", makePayload(i, 100)));
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
                assertEquals("soak", msg.getTopic());
                assertArrayEquals(makePayload(i, 100), msg.getPayload(), "payload mismatch at " + i);
                i++;
            }
            assertEquals(count, i, "expected all records back");
            // Core invariant: the next append lands exactly at the current max offset.
            probeBefore = log.maxOffset();
            assertEquals(probeBefore, log.append(new Message(2L, -1, "probe", new byte[8])));
        }
    }

    @Test
    void appendAfterReopenContinuesAtMaxOffset() throws IOException {
        try (CommitLog log = CommitLog.open(tempDir, 64 * 1024)) {
            for (int i = 0; i < 50; i++) {
                log.append(new Message(1L, i, "t", payload("a" + i)));
            }
        }
        try (CommitLog log = CommitLog.open(tempDir, 64 * 1024)) {
            Message msg = new Message(1L, 50, "t", payload("after-reopen"));
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
                log.append(new Message(1L, 1, "t", new byte[2048]));
            }
        });
    }

    /** Deterministic payload: size fixed, content derived from the index. */
    private static byte[] makePayload(int i, int size) {
        byte[] payload = new byte[size];
        payload[0] = (byte) (i & 0xFF);
        payload[1] = (byte) ((i >> 8) & 0xFF);
        payload[size - 1] = (byte) ~payload[0];
        return payload;
    }
}
