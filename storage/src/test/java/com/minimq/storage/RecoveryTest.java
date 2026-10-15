package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Crash-recovery scenarios: torn writes, CRC-corrupted tails, garbage/empty
 * newest segments. Everything must recover to the last CRC-valid record.
 */
class RecoveryTest {

    @TempDir
    File tempDir;

    private static final int SEGMENT = 64 * 1024;

    private static Message msg(int i, int payloadSize) {
        byte[] payload = new byte[payloadSize];
        payload[0] = (byte) i;
        payload[payloadSize - 1] = (byte) i;
        return new Message(1_700_000_000_000L + i, i, payload);
    }

    /**
     * Power-loss during a record write: the last record's first 10 bytes are on
     * disk, the rest never made it. Recovery must truncate to record #100 and
     * the next append must continue exactly there.
     */
    @Test
    void tornTailIsTruncatedToLastValidRecord() throws IOException {
        Message torn = msg(100, 100);
        long offsetOfTorn;
        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            for (int i = 0; i < 100; i++) {
                log.append(msg(i, 100));
            }
            byte[] encoded = MessageCodec.encode(torn);
            byte[] partial = new byte[10];
            System.arraycopy(encoded, 0, partial, 0, partial.length);
            offsetOfTorn = log.appendRaw(partial); // simulate a torn write
        }

        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            assertEquals(offsetOfTorn, log.maxOffset(),
                    "must truncate back to where record #100 starts");

            int seen = 0;
            Iterator<Message> it = log.iterator();
            while (it.hasNext()) {
                Message m = it.next();
                assertEquals(seen, m.getKeyHash());
                seen++;
            }
            assertEquals(100, seen, "only the 100 complete records survive");

            // Appending continues right at the truncation point, overwriting
            // the torn bytes.
            Message next = msg(100, 100);
            long offset = log.append(next);
            assertEquals(offsetOfTorn, offset);
            assertEquals(offset, next.getOffset());
        }
    }

    /**
     * Bit rot in record #50's payload: CRC catches it, recovery truncates the
     * whole log back to record #50's start, and the store stays appendable.
     */
    @Test
    void crcCorruptedRecordIsCutOff() throws IOException {
        Message corrupted = msg(49, 100); // record #50 (0-based index 49)
        long offsetOfCorrupted = -1;
        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            for (int i = 0; i < 100; i++) {
                Message m = msg(i, 100);
                log.append(m);
                if (i == 49) {
                    offsetOfCorrupted = m.getOffset();
                }
            }
        }
        // Corrupt a payload byte on disk, after the mapping is released.
        flipByteOnDisk(tempDir, offsetOfCorrupted + MessageCodec.OFFSET_PAYLOAD + 7);

        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            assertEquals(offsetOfCorrupted, log.maxOffset(),
                    "must cut off at the first CRC-failing record");
            int seen = 0;
            Iterator<Message> it = log.iterator();
            while (it.hasNext()) {
                it.next();
                seen++;
            }
            assertEquals(49, seen, "records #1..#49 survive, #50..#100 are gone");

            // Overwrite the corrupted region with a fresh valid record.
            Message fresh = msg(49, 100);
            long offset = log.append(fresh);
            assertEquals(offsetOfCorrupted, offset);
            assertEquals(50, countRecords(log));
        }
    }

    /**
     * The newest segment contains only garbage (e.g. crashed right after
     * rolling, then the pre-allocated zero pages were scribbled on): it must
     * be dropped and recovery must fall back to the previous segment.
     */
    @Test
    void garbageNewestSegmentIsDroppedAndPreviousSegmentUsed() throws IOException {
        long offsetAfterFirstSegment;
        int countToFill = 0;
        try (CommitLog log = CommitLog.open(tempDir, 512)) { // 512B: ~3 records of 129B
            long last = -1;
            for (int i = 0; i < 3; i++) {
                last = log.append(msg(i, 100));
                countToFill++;
            }
            offsetAfterFirstSegment = last + MessageCodec.sizeOf(msg(2, 100));
            // Trigger a roll with one more record: it lands in segment #2.
            log.append(msg(3, 100));
            assertEquals(2, log.segmentCount());
        }
        // Scribble 32 bytes of 0xAA over the start of segment #2.
        File seg2 = new File(tempDir, MappedFile.fileName(512));
        assertTrue(seg2.exists());
        try (RandomAccessFile raf = new RandomAccessFile(seg2, "rw")) {
            raf.seek(0);
            raf.write(new byte[32]);
            java.util.Arrays.fill(new byte[0], (byte) 0); // no-op, keep style simple
            byte[] junk = new byte[32];
            java.util.Arrays.fill(junk, (byte) 0xAA);
            raf.seek(0);
            raf.write(junk);
        }

        try (Recovery.Result result = Recovery.recover(tempDir, 512)) {
            assertEquals(1, result.deletedSegments);
            assertEquals(1, result.segments.size());
            assertEquals(countToFill, result.recoveredMessages);
            assertEquals(offsetAfterFirstSegment, result.maxOffset);
        }
        assertFalse(seg2.exists(), "garbage segment must be deleted");

        try (CommitLog log = CommitLog.open(tempDir, 512)) {
            assertEquals(offsetAfterFirstSegment, log.maxOffset());
            assertEquals(1, log.segmentCount());
            // The 129B record no longer fits into the 125B tail of segment #0,
            // so the append rolls into a fresh segment starting at 512.
            Message next = msg(3, 100);
            assertEquals(512L, log.append(next));
        }
    }

    /**
     * The newest segment is intact but empty (crashed right after rolling):
     * it is kept and appends continue inside it. Note maxOffset is the next
     * write position, i.e. the start of the empty segment (512), not the end
     * of the previous segment's last record (387): segment tail holes are
     * part of the design.
     */
    @Test
    void emptyNewestSegmentIsKept() throws IOException {
        try (CommitLog log = CommitLog.open(tempDir, 512)) {
            for (int i = 0; i < 3; i++) {
                log.append(msg(i, 100));
            }
            log.append(msg(3, 100)); // forces a roll into segment #2
            assertEquals(2, log.segmentCount());
        }
        // Simulate: segment #2's single record never hit the disk.
        File seg2 = new File(tempDir, MappedFile.fileName(512));
        try (RandomAccessFile raf = new RandomAccessFile(seg2, "rw")) {
            raf.setLength(0); // re-created as all zeros by MappedFile
        }

        try (CommitLog log = CommitLog.open(tempDir, 512)) {
            assertEquals(512L, log.maxOffset(), "next write lands at the empty segment start");
            assertEquals(2, log.segmentCount());
            Message next = msg(3, 100);
            assertEquals(512L, log.append(next));
            assertEquals(4, countRecords(log));
        }
    }

    @Test
    void corruptOnlySegmentIsKeptButEmptied() throws IOException {
        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            log.append(msg(0, 100));
        }
        flipByteOnDisk(tempDir, MessageCodec.OFFSET_PAYLOAD + 3); // record #1 payload

        try (Recovery.Result result = Recovery.recover(tempDir, SEGMENT)) {
            assertEquals(0, result.recoveredMessages);
            assertEquals(0, result.maxOffset);
            assertEquals(1, result.segments.size(), "a single segment is never deleted");
        }

        try (CommitLog log = CommitLog.open(tempDir, SEGMENT)) {
            assertEquals(0L, log.maxOffset());
            assertEquals(0, countRecords(log), "emptied by truncation");
            Message m = msg(0, 100);
            assertEquals(0L, log.append(m));
            assertEquals(1, countRecords(log));
        }
    }

    private static int countRecords(CommitLog log) {
        int seen = 0;
        Iterator<Message> it = log.iterator();
        while (it.hasNext()) {
            it.next();
            seen++;
        }
        return seen;
    }

    /**
     * Flips one byte at the given global offset on disk (mapping must be closed).
     */
    private static void flipByteOnDisk(File dir, long globalOffset) throws IOException {
        File[] segments = Recovery.listSegments(dir);
        File target = null;
        for (File f : segments) {
            if (globalOffset >= Long.parseLong(f.getName())
                    && globalOffset < Long.parseLong(f.getName()) + f.length()) {
                target = f;
                break;
            }
        }
        assertNotNull(target, "no segment covers offset " + globalOffset);
        try (RandomAccessFile raf = new RandomAccessFile(target, "rw")) {
            raf.seek(globalOffset - Long.parseLong(target.getName()));
            int b = raf.read();
            raf.seek(globalOffset - Long.parseLong(target.getName()));
            raf.write(b ^ 0x5A);
        }
    }
}
