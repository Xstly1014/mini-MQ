package com.minimq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M2 task 1: the group-commit machinery.
 *
 * SYNC semantics: awaitDurable returns only after flushedOffset passed the
 * batch end; aggregation: concurrent appenders are covered by far fewer
 * forces than batches; linger: with a huge batch-count trigger the flusher
 * still forces within a few linger windows; close(): final force covers
 * everything appended.
 */
class FlushServiceTest {

    @TempDir
    File tempDir;

    private static byte[] payload(int i) {
        byte[] b = new byte[64];
        b[0] = (byte) i;
        b[1] = (byte) (i >>> 8);
        return b;
    }

    @Test
    void syncAwaitReturnsOnlyAfterForceCoversBatch() throws Exception {
        try (CommitLog log = CommitLog.open(tempDir)) {
            FlushService flusher = new FlushService(log, 500, 64);
            flusher.start();
            try {
                long end = 0;
                for (int i = 0; i < 10; i++) {
                    Message msg = new Message(1_700_000_000_000L + i, i, "sync", payload(i));
                    end = log.append(msg) + MessageCodec.sizeOf(msg);
                }
                assertTrue(log.awaitDurable(end, 5_000), "awaitDurable timed out");
                assertTrue(log.flushedOffset() >= end,
                        "durable frontier " + log.flushedOffset() + " behind batch end " + end);
            } finally {
                flusher.close();
            }
        }
    }

    /**
     * The aggregation proof: 8 threads x 20 single-record batches, all parked
     * in awaitDurable. Group commit must cover them with far fewer force
     * sweeps than 160 — the whole point is N appends amortized over 1 force.
     */
    @Test
    void concurrentAppendsAggregateIntoFewForces() throws Exception {
        try (CommitLog log = CommitLog.open(tempDir)) {
            FlushService flusher = new FlushService(log, 2_000, 1_000_000);
            flusher.start();
            ExecutorService pool = Executors.newFixedThreadPool(8);
            try {
                int threads = 8;
                int batchesPerThread = 20;
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<Future<?>>();
                for (int t = 0; t < threads; t++) {
                    final int seed = t;
                    futures.add(pool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                start.await();
                                for (int i = 0; i < batchesPerThread; i++) {
                                    Message msg = new Message(1_700_000_000_000L + i, seed,
                                            "agg", payload(seed * 100 + i));
                                    long end = log.append(msg) + MessageCodec.sizeOf(msg);
                                    assertTrue(log.awaitDurable(end, 10_000),
                                            "awaitDurable timed out");
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
                long totalBatches = threads * batchesPerThread;
                // 2ms linger + 8 parked threads: the overwhelming majority of
                // batches share a force. Generous bound (x4) absorbs machine
                // jitter; the M1 world would be one force per batch.
                assertTrue(flusher.forceSweeps() <= totalBatches / 4,
                        "no aggregation: " + flusher.forceSweeps() + " forces for "
                                + totalBatches + " batches");
                assertTrue(log.flushedOffset() >= log.maxOffset());
            } finally {
                pool.shutdownNow();
                flusher.close();
            }
        }
    }

    /** Huge batch-count trigger must not stall: linger alone forces progress. */
    @Test
    void lingerAloneAdvancesDurableOffset() throws Exception {
        try (CommitLog log = CommitLog.open(tempDir)) {
            FlushService flusher = new FlushService(log, 1_000, 1_000_000);
            flusher.start();
            try {
                Message msg = new Message(1L, 1, "linger", payload(1));
                long end = log.append(msg) + MessageCodec.sizeOf(msg);
                assertTrue(log.awaitDurable(end, 5_000),
                        "linger window did not force within 5s");
            } finally {
                flusher.close();
            }
        }
    }

    /** close() runs a final force: nothing appended may stay un-forced. */
    @Test
    void closeForcesEverythingAppended() throws Exception {
        try (CommitLog log = CommitLog.open(tempDir)) {
            FlushService flusher = new FlushService(log, 60_000_000L, 1_000_000);
            flusher.start();
            for (int i = 0; i < 5; i++) {
                log.append(new Message(1_700_000_000_000L + i, i, "close", payload(i)));
            }
            // linger is 60s: without the close-time final force these bytes
            // would stay in page cache. close() must cover them.
            flusher.close();
            assertEquals(log.maxOffset(), log.flushedOffset(),
                    "close() left an un-forced tail behind");
        }
    }

    /**
     * The latent M1 race, regression-fixed by the M2 append lock: 16 threads
     * appending concurrently must produce exactly 16N records, all CRC-valid,
     * with offsets forming a gap-free cover of [0, maxOffset).
     */
    @Test
    void concurrentAppendsStayConsistent() throws Exception {
        int threads = 16;
        int perThread = 200;
        try (CommitLog log = CommitLog.open(tempDir, 1024 * 1024)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                List<Future<?>> futures = new ArrayList<Future<?>>();
                CountDownLatch start = new CountDownLatch(1);
                for (int t = 0; t < threads; t++) {
                    final int seed = t;
                    futures.add(pool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                start.await();
                                for (int i = 0; i < perThread; i++) {
                                    log.append(new Message(1_700_000_000_000L + i, seed * 31 + i,
                                            "race", payload(seed * 1000 + i)));
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            // Every record CRC-valid (iterator throws on corruption) and the
            // total count exact.
            List<Long> starts = new ArrayList<Long>();
            long covered = 0;
            java.util.Iterator<Message> it = log.iterator();
            while (it.hasNext()) {
                Message m = it.next();
                starts.add(m.getOffset());
                covered += MessageCodec.sizeOf(m);
            }
            assertEquals(threads * perThread, starts.size(),
                    "record count wrong — interleaved writes lost data");
            assertEquals(log.maxOffset(), covered,
                    "byte coverage wrong — interleaved writes corrupted the log");
            // Gap-free: sorted starts + sizes tile [0, maxOffset) exactly
            // (single segment, no roll at this size).
            Collections.sort(starts);
            long expect = 0;
            for (int i = 0; i < starts.size(); i++) {
                assertEquals(expect, starts.get(i).longValue(),
                        "gap/overlap at record " + i);
                expect = starts.get(i) + recordSizeAt(log, starts.get(i));
            }
        }
    }

    private static int recordSizeAt(CommitLog log, long offset) {
        List<Message> one = log.readFrom(offset, 1);
        assertEquals(1, one.size(), "record missing at " + offset);
        return MessageCodec.sizeOf(one.get(0));
    }
}
