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

    /**
     * Encodes a message exactly the way the leader would ship it: raw record
     * bytes, no offset write-back (the follower keeps the leader's offset
     * verbatim — that is the whole point of replication).
     */
    private static byte[] encoded(Message msg) {
        byte[] buf = CodecPools.borrowEncodeBuffer(0);
        int total = MessageCodec.encodeInto(msg, buf);
        if (total < 0) {
            buf = CodecPools.borrowEncodeBuffer(-total);
            total = MessageCodec.encodeInto(msg, buf);
        }
        return java.util.Arrays.copyOf(buf, total);
    }

    /**
     * M3 replication append: the leader's bytes land at the exact offset the
     * leader named. A mismatch with the local frontier means two diverged
     * histories — the append must be refused BEFORE any byte is written, and
     * the log must stay intact for the epoch-arbitrated truncation that the
     * caller will run next.
     */
    @Test
    void appendReplicatedEnforcesOffsetContinuity() throws IOException {
        try (CommitLog log = CommitLog.open(tempDir, 64 * 1024)) {
            Message m0 = new Message(1L, 0, "sync", makePayload(0, 64));
            long off0 = log.append(m0);
            assertEquals(0L, off0);

            byte[] rec1 = encoded(new Message(1L, 1, "sync", makePayload(1, 64)));
            long end = log.appendReplicated(off0 + MessageCodec.sizeOf(m0), rec1);
            assertEquals(end, log.maxOffset());
            // the replicated record reads back byte-identical
            assertEquals(1, log.readFrom(off0 + MessageCodec.sizeOf(m0), 1).get(0).getKeyHash());

            // divergent offset: refused loudly, log unchanged
            long before = log.maxOffset();
            assertThrows(IOException.class, () -> log.appendReplicated(before + 4096, rec1));
            assertEquals(before, log.maxOffset());
        }
    }

    /**
     * The stale-leader revival scenario, storage level (M3): the node wrote a
     * dirty tail while partitioned, the new leader's epoch checkpoint says
     * rewind to {@code cut}, then the current lineage is replicated over the
     * corpse. After reopen: old head + new tail, and the dirty tail must NOT
     * resurrect (the segment remainder is zeroed, dead segments deleted —
     * recovery's CRC scan must find nothing to accept).
     */
    @Test
    void truncatedTailIsOverwrittenAndNeverResurrects() throws IOException {
        int segmentSize = 1024; // ~7 records per segment: forces multi-segment
        long endOfKeep = 0;
        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            Message[] keep = new Message[10];
            for (int i = 0; i < 10; i++) {
                keep[i] = new Message(1L, i, "raft", makePayload(i, 100));
                log.append(keep[i]);
            }
            endOfKeep = keep[9].getOffset() + MessageCodec.sizeOf(keep[9]);

            // the partitioned old leader keeps writing — the dirty tail
            for (int i = 10; i < 20; i++) {
                log.append(new Message(1L, i, "raft", makePayload(i, 100)));
            }
            assertTrue(log.segmentCount() > 2, "dirty tail must span segments, got "
                    + log.segmentCount());

            // epoch arbitration: rewind to the end of record 9; idempotent at the tip
            log.truncateTo(log.maxOffset());
            log.truncateTo(endOfKeep);
            assertEquals(endOfKeep, log.maxOffset());

            // the current leader's history is replicated over the cut
            byte[] rec = encoded(new Message(2L, 100, "raft", makePayload(100, 100)));
            log.appendReplicated(endOfKeep, rec);
        }

        try (CommitLog log = CommitLog.open(tempDir, segmentSize)) {
            Iterator<Message> it = log.iterator();
            int seen = 0;
            while (it.hasNext()) {
                Message m = it.next();
                if (seen < 10) {
                    assertEquals(seen, m.getKeyHash(), "old-lineage head record " + seen);
                } else {
                    assertEquals(100, m.getKeyHash(),
                            "the record after the cut must be new-lineage, not resurrected dirt");
                }
                seen++;
            }
            assertEquals(11, seen, "10 kept + 1 replicated");
            // and the log continues forward from the new tip
            assertEquals(log.maxOffset(), log.append(new Message(2L, 101, "raft", makePayload(101, 100))));
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
}
