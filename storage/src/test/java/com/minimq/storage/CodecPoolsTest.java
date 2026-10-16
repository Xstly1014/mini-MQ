package com.minimq.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CodecPools contract (M2 DoD: dirty-object and lifecycle tests for the
 * pools). The lease is the stack frame — the next borrow on the same thread
 * invalidates the previous one, so the hazards to pin down are exactly:
 * <ol>
 *   <li><b>Residue</b>: a shorter record encoded into scratch that still
 *       holds a longer previous record must not leak old bytes into the
 *       valid range ([0, total) fully rewritten) NOR let the length-blind
 *       consumer read past total;</li>
 *   <li><b>CRC reset</b>: a CRC32 borrowed after a previous use must be
 *       clean — a missing reset would accumulate and misjudge the next
 *       record CORRUPT;</li>
 *   <li><b>Thread isolation</b>: concurrent appends on one CommitLog share
 *       nothing (each thread has its own scratch);</li>
 *   <li><b>Hit ratio counters</b>: the monitoring must actually move.</li>
 * </ol>
 */
class CodecPoolsTest {

    @TempDir
    File tempDir;

    private static Message msg(String topic, byte[] payload, long keyHash) {
        return new Message(42L, keyHash, topic, payload);
    }

    @Test
    void scratchReuseLeavesNoResidueInTheValidRange() {
        byte[] big = new byte[2048];
        byte[] small = new byte[16];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) 0xAB; // distinctive residue
        }
        for (int i = 0; i < small.length; i++) {
            small[i] = (byte) i;
        }
        Message bigMsg = msg("residue", big, 1L);
        Message smallMsg = msg("residue", small, 2L);

        // Same-thread sequence: big record first, then a SHORT record into
        // the same scratch — bytes past the small total still hold 0xAB.
        byte[] scratch = CodecPools.borrowEncodeBuffer(0);
        int bigTotal = MessageCodec.encodeInto(bigMsg, scratch);
        assertTrue(bigTotal > 0);

        byte[] same = CodecPools.borrowEncodeBuffer(0);
        assertSame(scratch, same, "same-thread borrow must reuse the scratch");
        int smallTotal = MessageCodec.encodeInto(smallMsg, same);
        assertEquals(31 + "residue".length() + small.length, smallTotal);

        byte[] expected = MessageCodec.encode(smallMsg); // fresh-array reference
        assertArrayEquals(expected, java.util.Arrays.copyOfRange(same, 0, smallTotal),
                "valid range must be identical to a fresh encode (no residue leaked)");
        // and the residue beyond total really is still there — which is fine,
        // because every consumer honors (buffer, total), never buffer.length:
        assertEquals((byte) 0xAB, same[smallTotal], "residue past total is expected");
    }

    @Test
    void encodeIntoReportsRequiredCapacityAndWritesNothingOnShortBuffer() {
        byte[] payload = new byte[300];
        Message m = msg("cap", payload, 3L);
        byte[] tiny = new byte[8];
        int required = MessageCodec.encodeInto(m, tiny);
        assertTrue(required < 0, "short buffer must return a negative requirement");
        assertEquals(-required, MessageCodec.sizeOf(m));
        // nothing written on the failure path
        for (byte b : tiny) {
            assertEquals(0, b);
        }
        // and the suggested capacity is sufficient
        byte[] grown = CodecPools.borrowEncodeBuffer(-required);
        int total = MessageCodec.encodeInto(m, grown);
        assertTrue(total > 0, "borrow(-required) must satisfy encodeInto");
        assertEquals(-required, total);
    }

    @Test
    void borrowedCrcIsResetBetweenUses() {
        // Encode one record (CRC32 used), then immediately decode a VALID
        // record: if the borrowed CRC kept stale state, the decode would
        // wrongly report CORRUPT.
        byte[] payload = new byte[64];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        Message m = msg("crc", payload, 4L);
        byte[] encoded = MessageCodec.encode(m);
        // decode() requires a little-endian buffer (see MappedFile#slice)
        ByteBuffer littleEndian = ByteBuffer.wrap(encoded).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        MessageCodec.DecodeResult first = MessageCodec.decode(littleEndian, 0);
        assertTrue(first.isOk());
        // second decode of the same valid record — stale CRC state would fail
        MessageCodec.DecodeResult second = MessageCodec.decode(
                ByteBuffer.wrap(encoded).order(java.nio.ByteOrder.LITTLE_ENDIAN), 0);
        assertTrue(second.isOk(), "borrowed CRC32 must be reset on every borrow");
        assertEquals(m.getTopic(), second.message.getTopic());
        assertArrayEquals(payload, second.message.getPayload());
    }

    @Test
    void concurrentAppendsStayIsolatedPerThread() throws Exception {
        File dir = new File(tempDir, "store");
        final CommitLog log = CommitLog.open(dir, 1024 * 1024);
        final int threads = 4;
        final int perThread = 500;
        final CountDownLatch done = new CountDownLatch(threads);
        final List<List<Long>> offsetsPerThread = new ArrayList<List<Long>>(threads);
        for (int t = 0; t < threads; t++) {
            offsetsPerThread.add(new ArrayList<Long>());
        }
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            Thread worker = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            // distinct size mix per thread to force scratch growth
                            byte[] payload = new byte[100 + tid * 50 + (i % 37)];
                            Message m = msg("iso-" + tid, payload, tid * 1_000_000L + i);
                            long offset = log.append(m);
                            offsetsPerThread.get(tid).add(offset);
                        }
                        done.countDown();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            worker.start();
        }
        assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));

        // read back every thread's records through decode (CRC-validated)
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                long offset = offsetsPerThread.get(t).get(i);
                List<Message> read = log.readFrom(offset, 1);
                assertEquals(1, read.size(), "record missing at " + offset);
                Message m = read.get(0);
                assertEquals("iso-" + t, m.getTopic());
                assertEquals(100 + t * 50 + (i % 37), m.getPayload().length,
                        "payload crossed threads at " + offset);
            }
        }
        log.close();
    }

    @Test
    void hitRatioCountersAdvance() {
        long hitsBefore = CodecPools.encodeHits.sum();
        long borrowsBefore = CodecPools.encodeBorrows.sum();
        long crcHitsBefore = CodecPools.crcHits.sum();

        byte[] b1 = CodecPools.borrowEncodeBuffer(32);
        byte[] b2 = CodecPools.borrowEncodeBuffer(32);
        assertSame(b1, b2, "same-thread borrow returns the thread-local scratch");
        CodecPools.borrowCrc32(); // clean borrow
        CodecPools.borrowCrc32(); // must be a hit

        assertEquals(borrowsBefore + 2, CodecPools.encodeBorrows.sum());
        assertEquals(hitsBefore + 1, CodecPools.encodeHits.sum(),
                "second same-size borrow is a hit");
        assertEquals(crcHitsBefore + 1, CodecPools.crcHits.sum(),
                "second CRC borrow is a hit");
        assertTrue(CodecPools.stats().contains("hit ratio"),
                "stats() must expose ratios: " + CodecPools.stats());
    }
}
