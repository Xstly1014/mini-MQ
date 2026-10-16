package com.minimq.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dedicated group-commit flush thread (M2 task 1), the simplified counterpart
 * of MySQL's ordered_commit SYNC stage:
 *
 * <pre>
 * MySQL:    FLUSH (batch write into OS cache) -> SYNC (Leader fsync) -> COMMIT (wake Followers)
 * mini-MQ:  FLUSH (mmap append == already in page cache, batched for free)
 *           -> SYNC (this thread forces, covering every waiting "Follower")
 *           -> ACK  (parked producers wake when flushedOffset passes their end offset)
 * </pre>
 *
 * Two triggers, whichever comes first (isomorphic to MySQL's
 * binlog_group_commit_sync_delay + no_delay_count, Kafka's linger.ms +
 * batch.size):
 * <ul>
 *   <li>{@code flushLingerUs} — max microseconds from one force completing to
 *   the next starting; concurrent appends landing inside the window are
 *   aggregated into that single force,</li>
 *   <li>{@code flushBatchCount} — appends since the last force; reaching it
 *   wakes the flusher early so high write volume never waits the full
 *   linger.</li>
 * </ul>
 *
 * The counter-intuitive property worth reproducing in the bench: the busier
 * the writers, the more each force amortizes over — throughput under SYNC
 * mode can rise with concurrency.
 *
 * Concurrency: never holds the append lock while forcing. It snapshots the
 * segment list under the lock, forces outside it (mmap force is safe to run
 * concurrently with appends — it flushes whatever dirty pages existed), and
 * only advances flushedOffset to the offset captured BEFORE the force, never
 * including bytes appended during it.
 */
public final class FlushService implements Closeable {

    private static final Logger LOG = Logger.getLogger(FlushService.class.getName());

    private final CommitLog commitLog;
    private final long lingerUs;
    private final int batchCount;
    private final Thread worker;
    private volatile boolean running = true;
    /** Completed force sweeps (bench/test observability of the aggregation). */
    private volatile long forceSweeps;
    /**
     * Records covered by the frontier this thread last captured. Kept HERE,
     * not on the CommitLog: a shared counter updated in onFlushed could be
     * snapshotted after the force returned and thus include appends that
     * landed during it — those bytes are NOT durable, yet pending would read
     * 0 forever after, and SYNC producers parked in awaitDurable timed out
     * (the M2-4a race, fixed by moving the counter to the only thread that
     * has the ground truth: this one).
     *
     * Read/write only inside the flush thread (flushLock held for the
     * volatile-visible read of appendedCount); plain long suffices.
     */
    private long lastForcedCount;

    public FlushService(CommitLog commitLog, long flushLingerUs, int flushBatchCount) {
        this.commitLog = commitLog;
        this.lingerUs = Math.max(0, flushLingerUs);
        this.batchCount = Math.max(1, flushBatchCount);
        this.worker = new Thread(new Runnable() {
            @Override
            public void run() {
                loop();
            }
        }, "mini-mq-flush");
        this.worker.setDaemon(true);
    }

    public void start() {
        worker.start();
    }

    /** Force sweeps completed since start (group-commit aggregation metric). */
    public long forceSweeps() {
        return forceSweeps;
    }

    private void loop() {
        while (running) {
            try {
                // The offset frontier captured before the force; only bytes
                // below it are guaranteed durable when the force returns.
                final long frontier;
                synchronized (commitLog.flushLock) {
                    long pending = commitLog.appendedCount - lastForcedCount;
                    if (pending >= batchCount) {
                        // Capture order matters: read appendedCount BEFORE
                        // appendFrontier. If an append lands between the two
                        // reads, the frontier covers MORE records than the
                        // count claims (a harmless extra force), never fewer
                        // (which would leave pending=0 with undurable bytes).
                        lastForcedCount = commitLog.appendedCount;
                        frontier = commitLog.maxOffsetUnlocked();
                    } else {
                        long sleepUs = lingerUs;
                        if (sleepUs <= 0) {
                            // linger 0 + nothing pending: still wait for work,
                            // just don't spin — a tiny park keeps the thread
                            // from burning a core when idle.
                            sleepUs = 200;
                        }
                        try {
                            TimeUnit.MICROSECONDS.timedWait(
                                    commitLog.flushLock, sleepUs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (!running) {
                            return;
                        }
                        pending = commitLog.appendedCount - lastForcedCount;
                        if (pending <= 0) {
                            continue; // idle window, nothing new to force
                        }
                        lastForcedCount = commitLog.appendedCount;
                        frontier = commitLog.maxOffsetUnlocked();
                    }
                }
                // Force OUTSIDE the lock: an mmap force concurrent with
                // appends is safe (page granularity); holding the lock here
                // would serialize writers against disk latency.
                commitLog.forceSegmentsUpToCurrent();
                commitLog.onFlushed(frontier);
                forceSweeps++;
            } catch (Throwable t) {
                if (running) {
                    LOG.log(Level.WARNING, "flush loop error", t);
                }
            }
        }
    }

    /**
     * Stop the loop, run one final force covering everything appended so far,
     * and wake all SYNC waiters — shutdown must never leave an acked-but-
     * unforced window behind.
     */
    @Override
    public void close() throws IOException {
        running = false;
        synchronized (commitLog.flushLock) {
            commitLog.flushLock.notifyAll(); // wake the loop for a clean exit
        }
        try {
            worker.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long frontier;
        synchronized (commitLog.flushLock) {
            frontier = commitLog.maxOffsetUnlocked();
        }
        commitLog.forceSegmentsUpToCurrent();
        commitLog.onFlushed(frontier);
    }

    /** Visible for the bench: segments currently mapped (snapshot). */
    List<MappedFile> segmentSnapshot() {
        return commitLog.segmentSnapshot();
    }
}
