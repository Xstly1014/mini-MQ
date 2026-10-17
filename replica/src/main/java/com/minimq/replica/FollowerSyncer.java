package com.minimq.replica;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;

/**
 * Follower-side replication loop (M3): FETCH the leader's raw bytes and
 * append them verbatim at the leader-named offsets, adopting epochs and
 * truncating where the leader's checkpoint arbitrates a stale tail.
 *
 * <h3>The design in one breath</h3>
 * Replication is a consumer whose payload happens to be the log itself:
 * the follower FETCHes (the M2 zero-copy path serves it — the follower is
 * just another consumer), then {@link CommitLog#appendReplicated} writes
 * the raw span WITHOUT decode/re-encode. Byte-identical logs follow for
 * free: same CRCs, same offsets, same physical layout.
 *
 * <h3>Epoch adoption &amp; arbitration</h3>
 * Every response carries (leaderEpoch, epochStartOffset). A NEWER epoch is
 * recorded in the {@link EpochCheckpoint} before its records are trusted —
 * the checkpoint is what a later revival consults. When the leader answers
 * TRUNCATE_TO, the local log, index, and checkpoint all roll back in one
 * shot (log first — see ConsumeQueueStore.truncateLogAndIndex's ordering
 * contract), and the next fetch restarts from the truncated end. A partial
 * append batch is self-healing the same way: the next fetch begins at
 * whatever {@code maxOffset()} reached, and appendReplicated's offset
 * equality check refuses any divergence loudly.
 *
 * <h3>Stopping</h3>
 * A leaderLost response (NOT_LEADER / STALE_EPOCH) stops the loop; the
 * wiring restarts it when the election produces the next leader. Channel
 * I/O errors back off and retry — the leader may just be between heartbeats.
 */
public final class FollowerSyncer {

    private static final Logger LOG = Logger.getLogger(FollowerSyncer.class.getName());

    /**
     * The network seam: one in-flight blocking fetch against the current
     * leader. Implemented by the broker's cluster transport (M3-6); mocked
     * in tests.
     */
    public interface LeaderChannel {
        FetchedBatch fetch(long fromOffset, long followerEpoch, int maxBytes) throws IOException;
    }

    /** One raw record: where it lives and its verbatim encoded bytes. */
    public static final class RawRecord {
        public final long offset;
        public final byte[] raw;

        public RawRecord(long offset, byte[] raw) {
            this.offset = offset;
            this.raw = raw;
        }
    }

    /** Parsed FOLLOWER_FETCH result: data, arbitration, or leader loss. */
    public static final class FetchedBatch {
        public final long leaderEpoch;
        public final long epochStartOffset;
        public final long highWatermark;
        public final List<RawRecord> records;
        /** TRUNCATE_TO: roll the local log back to this offset first. */
        public final long truncateTo;
        public final boolean truncate;
        public final boolean leaderLost;

        private FetchedBatch(long leaderEpoch, long epochStartOffset, long highWatermark,
                List<RawRecord> records, long truncateTo, boolean truncate, boolean leaderLost) {
            this.leaderEpoch = leaderEpoch;
            this.epochStartOffset = epochStartOffset;
            this.highWatermark = highWatermark;
            this.records = records;
            this.truncateTo = truncateTo;
            this.truncate = truncate;
            this.leaderLost = leaderLost;
        }

        public static FetchedBatch ok(long leaderEpoch, long epochStartOffset, long highWatermark,
                List<RawRecord> records) {
            return new FetchedBatch(leaderEpoch, epochStartOffset, highWatermark,
                    records, -1L, false, false);
        }

        public static FetchedBatch truncate(long epoch, long toOffset) {
            return new FetchedBatch(epoch, -1L, -1L,
                    Collections.<RawRecord>emptyList(), toOffset, true, false);
        }

        public static FetchedBatch leaderLost() {
            return new FetchedBatch(-1L, -1L, -1L,
                    Collections.<RawRecord>emptyList(), -1L, false, true);
        }
    }

    private final CommitLog log;
    private final ConsumeQueueStore store;
    private final EpochCheckpoint checkpoint;
    private final LeaderChannel channel;
    private final int maxBytes;
    private final long idleParkMs;
    private final long errorBackoffMs;

    private volatile boolean running;
    private Thread worker;
    /** The epoch we are currently syncing under (-1 until the first response). */
    private volatile long syncingEpoch = -1L;
    private volatile long lastHw = -1L;

    public FollowerSyncer(CommitLog log, ConsumeQueueStore store, EpochCheckpoint checkpoint,
            LeaderChannel channel, int maxBytes) {
        this.log = log;
        this.store = store;
        this.checkpoint = checkpoint;
        this.channel = channel;
        this.maxBytes = maxBytes;
        this.idleParkMs = 5L;
        this.errorBackoffMs = 200L;
    }

    /** Epoch currently being synced (monitoring; -1 before the first response). */
    public long syncingEpoch() {
        return syncingEpoch;
    }

    /** False once the loop exited (leaderLost or shutdown) — restartable. */
    public synchronized boolean isRunning() {
        return running;
    }

    /**
     * Restart after a {@link #isRunning isRunning() == false} exit: the
     * wiring calls this when the election says a leader exists again. A
     * still-running syncer is a no-op, which is what makes heartbeat-driven
     * restarts safe.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(new Runnable() {
            @Override
            public void run() {
                syncLoop();
            }
        }, "mini-mq-follower-sync");
        worker.setDaemon(true);
        worker.start();
    }

    public void shutdown() {
        running = false;
        Thread w = worker;
        if (w != null) {
            w.interrupt();
            try {
                w.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Last leader-advertised HW observed (monitoring). */
    public long lastHighWatermark() {
        return lastHw;
    }

    private void syncLoop() {
        try {
            while (running) {
                try {
                    boolean more = syncOnce();
                    if (!more) {
                        return; // leader lost: wiring restarts us after the next election
                    }
                } catch (InterruptedException e) {
                    return; // shutdown
                } catch (Throwable t) {
                    if (running) {
                        LOG.log(Level.WARNING, "follower sync round failed (retrying)", t);
                        parkQuietly(errorBackoffMs);
                    }
                }
            }
        } finally {
            // leaderLost exits must leave the syncer restartable: a live
            // `running` flag with a dead thread would make start() a no-op
            // and the follower would never sync again.
            running = false;
        }
    }

    /**
     * One fetch-append round. Visible for tests (the loop is just this,
     * forever).
     *
     * @return false when the leader said it is no longer us (stop the loop)
     */
    public boolean syncOnce() throws IOException, InterruptedException {
        long leo = log.maxOffset();
        long epoch = checkpoint.latestEpoch();
        FetchedBatch batch = channel.fetch(leo, epoch, maxBytes);
        if (batch.leaderLost) {
            LOG.info("follower sync: leader lost, stopping (leo=" + leo + ")");
            return false;
        }
        if (batch.truncate) {
            // The arbitration: our tail belongs to a superseded lineage.
            // Log first, then index, then the checkpoint — the store's
            // truncateLock serializes this against live dispatch rounds.
            store.truncateLogAndIndex(batch.truncateTo);
            checkpoint.truncateFrom(batch.truncateTo);
            LOG.info("follower sync: arbitrated truncate to " + batch.truncateTo
                    + " (was " + leo + ")");
            return true;
        }
        lastHw = batch.highWatermark;
        // Adopt a NEWER epoch before trusting its bytes. Same-epoch responses
        // (the steady state) record nothing; an OLDER epoch than we already
        // adopted is a stale response from a deposed leader — its records
        // still match the log (they were in the checkpointed lineage), but
        // the checkpoint is not rewound by gossip.
        if (batch.leaderEpoch > checkpoint.latestEpoch()) {
            checkpoint.recordEpoch(batch.leaderEpoch, batch.epochStartOffset);
        }
        syncingEpoch = batch.leaderEpoch;
        for (RawRecord r : batch.records) {
            log.appendReplicated(r.offset, r.raw); // refuses divergence loudly
        }
        if (batch.records.isEmpty()) {
            parkQuietly(idleParkMs); // caught up: gentle polling
        }
        return true;
    }

    private static void parkQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
