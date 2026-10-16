package com.minimq.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.replica.FollowerSyncer.FetchedBatch;
import com.minimq.replica.FollowerSyncer.LeaderChannel;
import com.minimq.replica.FollowerSyncer.RawRecord;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.Message;
import com.minimq.storage.MessageCodec;

/**
 * FollowerSyncer (M3-5) against real storage and a scripted leader channel:
 * raw-append continuity, epoch adoption, arbitrated truncation (log + index
 * + checkpoint in one shot), leader-loss shutdown, and the loud refusal on
 * divergent offsets.
 */
class FollowerSyncerTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    /** Scripted channel: returns queued batches, then a tail directive. */
    static final class ScriptedChannel implements LeaderChannel {
        final List<FetchedBatch> script = new ArrayList<FetchedBatch>();
        /** The offset the follower last fetched from (what the leader sees). */
        volatile long lastFromOffset = -1;
        volatile long lastFollowerEpoch = Long.MIN_VALUE;

        @Override
        public FetchedBatch fetch(long fromOffset, long followerEpoch, int maxBytes) {
            this.lastFromOffset = fromOffset;
            this.lastFollowerEpoch = followerEpoch;
            if (script.isEmpty()) {
                return FetchedBatch.ok(-1L, -1L, -1L, new ArrayList<RawRecord>());
            }
            return script.remove(0);
        }
    }

    private static byte[] encoded(Message msg) {
        return MessageCodec.encode(msg);
    }

    private static Message msg(String topic, int seq) {
        return new Message(1_700_000_000_000L + seq, seq, topic,
                ("payload-" + seq).getBytes());
    }

    private File dir(String name) {
        return tempDir.resolve(name).toFile();
    }

    @Test
    void appendsRawRecordsAndAdoptsEpoch() throws Exception {
        CommitLog log = CommitLog.open(dir("log"), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(dir("cq"), log);
        EpochCheckpoint checkpoint = EpochCheckpoint.open(
                new File(dir("meta"), "leader-epoch-checkpoint"));
        ScriptedChannel channel = new ScriptedChannel();
        FollowerSyncer syncer = new FollowerSyncer(log, store, checkpoint, channel, 1 << 20);
        try {
            Message m0 = msg("t", 0);
            Message m1 = msg("t", 1);
            byte[] raw0 = encoded(m0);
            byte[] raw1 = encoded(m1);
            long end0 = raw0.length;
            long end1 = end0 + raw1.length;

            channel.script.add(FetchedBatch.ok(3, 0, end1, Arrays.asList(
                    new RawRecord(0, raw0), new RawRecord(end0, raw1))));

            assertTrue(syncer.syncOnce());
            assertEquals(end1, log.maxOffset(), "raw bytes appended verbatim");
            assertEquals(3L, checkpoint.latestEpoch(), "newer epoch adopted");
            assertEquals(0L, checkpoint.startOffsetFor(3), "epoch boundary recorded");
            assertEquals(end1, syncer.lastHighWatermark(), "leader HW observed");
            assertEquals(3L, syncer.syncingEpoch());

            // the batch is readable and CRC-valid through the normal path
            assertEquals(2, log.readFrom(0, 10).size());

            // next round fetches from the new LEO under the same epoch
            channel.script.add(FetchedBatch.ok(3, 0, end1, new ArrayList<RawRecord>()));
            assertTrue(syncer.syncOnce());
            assertEquals(end1, channel.lastFromOffset, "fetch resumes at the new log end");
            assertEquals(3L, channel.lastFollowerEpoch, "fetch reports the adopted epoch");
            // steady-state same-epoch response: no duplicate checkpoint rows
            assertEquals(3L, checkpoint.latestEpoch());
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * The stale-leader revival: the channel hands down a TRUNCATE_TO below
     * the local end — log, index, and checkpoint all roll back, and the
     * next fetch restarts from the truncated end.
     */
    @Test
    void truncateDirectiveRollsBackLogIndexAndCheckpoint() throws Exception {
        CommitLog log = CommitLog.open(dir("log"), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(dir("cq"), log);
        EpochCheckpoint checkpoint = EpochCheckpoint.open(
                new File(dir("meta"), "leader-epoch-checkpoint"));
        ScriptedChannel channel = new ScriptedChannel();
        FollowerSyncer syncer = new FollowerSyncer(log, store, checkpoint, channel, 1 << 20);
        try {
            // local history: epoch 1 from offset 0, two records
            byte[] raw0 = encoded(msg("t", 0));
            byte[] raw1 = encoded(msg("t", 1));
            long end0 = raw0.length;
            long end1 = end0 + raw1.length;
            channel.script.add(FetchedBatch.ok(1, 0, end1, Arrays.asList(
                    new RawRecord(0, raw0), new RawRecord(end0, raw1))));
            syncer.syncOnce();
            assertTrue(store.awaitDispatchedTo(end1, 5_000));
            assertEquals(2, store.readFrom("t", 0, 100).size());

            // a NEW leader (epoch 5) arbitrates: everything past end0 is a
            // superseded lineage — truncate there
            channel.script.add(FetchedBatch.truncate(5, end0));
            assertTrue(syncer.syncOnce());
            assertEquals(end0, log.maxOffset(), "log rolled back to the boundary");
            assertEquals(1, store.readFrom("t", 0, 100).size(),
                    "index rolled back with it");
            assertTrue(store.awaitDispatchedTo(end0, 5_000));

            // and replication resumes from the truncation point
            byte[] raw2 = encoded(msg("t", 2));
            channel.script.add(FetchedBatch.ok(5, end0, end0 + raw2.length, Arrays.asList(
                    new RawRecord(end0, raw2))));
            assertTrue(syncer.syncOnce());
            assertEquals(end0 + raw2.length, log.maxOffset());
            assertEquals(5L, checkpoint.latestEpoch());
            assertTrue(store.awaitDispatchedTo(end0 + raw2.length, 5_000),
                    "the replayed lineage must be indexed");
            assertEquals(2, store.readFrom("t", 0, 100).size());
        } finally {
            store.close();
            log.close();
        }
    }

    @Test
    void leaderLostStopsTheLoop() throws Exception {
        CommitLog log = CommitLog.open(dir("log"), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(dir("cq"), log);
        EpochCheckpoint checkpoint = EpochCheckpoint.open(
                new File(dir("meta"), "leader-epoch-checkpoint"));
        ScriptedChannel channel = new ScriptedChannel();
        FollowerSyncer syncer = new FollowerSyncer(log, store, checkpoint, channel, 1 << 20);
        try {
            channel.script.add(FetchedBatch.leaderLost());
            assertFalse(syncer.syncOnce(), "leaderLost ends the loop");
            assertEquals(-1L, syncer.syncingEpoch());
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * A response from an OLDER epoch than already adopted (a deposed leader
     * still answering from the old connection): its checkpoint row is NOT
     * rewound, but records are still appended — they belong to a lineage
     * the checkpoint already covers.
     */
    @Test
    void staleEpochResponseDoesNotRewindCheckpoint() throws Exception {
        CommitLog log = CommitLog.open(dir("log"), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(dir("cq"), log);
        EpochCheckpoint checkpoint = EpochCheckpoint.open(
                new File(dir("meta"), "leader-epoch-checkpoint"));
        ScriptedChannel channel = new ScriptedChannel();
        FollowerSyncer syncer = new FollowerSyncer(log, store, checkpoint, channel, 1 << 20);
        try {
            byte[] raw0 = encoded(msg("t", 0));
            channel.script.add(FetchedBatch.ok(4, 0, raw0.length,
                    Arrays.asList(new RawRecord(0, raw0))));
            syncer.syncOnce();
            assertEquals(4L, checkpoint.latestEpoch());

            byte[] raw1 = encoded(msg("t", 1));
            channel.script.add(FetchedBatch.ok(2, 0, raw0.length + raw1.length,
                    Arrays.asList(new RawRecord(raw0.length, raw1))));
            assertTrue(syncer.syncOnce());
            assertEquals(4L, checkpoint.latestEpoch(),
                    "checkpoint is not rewound by stale gossip");
            assertEquals(raw0.length + raw1.length, log.maxOffset(),
                    "records still land (same lineage, verifiable offsets)");
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * Divergence refusal: a record whose offset disagrees with the local
     * frontier means two histories — the append throws, the round fails,
     * and the retry fetches from wherever the local log actually ended.
     */
    @Test
    void divergentOffsetIsRefusedLoudly() throws Exception {
        CommitLog log = CommitLog.open(dir("log"), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(dir("cq"), log);
        EpochCheckpoint checkpoint = EpochCheckpoint.open(
                new File(dir("meta"), "leader-epoch-checkpoint"));
        ScriptedChannel channel = new ScriptedChannel();
        FollowerSyncer syncer = new FollowerSyncer(log, store, checkpoint, channel, 1 << 20);
        try {
            byte[] raw0 = encoded(msg("t", 0));
            channel.script.add(FetchedBatch.ok(1, 0, raw0.length,
                    Arrays.asList(new RawRecord(0, raw0))));
            syncer.syncOnce();

            byte[] junk = encoded(msg("t", 99));
            channel.script.add(FetchedBatch.ok(1, 0, raw0.length + junk.length,
                    Arrays.asList(new RawRecord(4096, junk)))); // wrong offset
            assertThrows(IOException.class, syncer::syncOnce,
                    "offset mismatch must be refused, not interleaved");
            assertEquals(raw0.length, log.maxOffset(), "local log untouched by the refusal");
        } finally {
            store.close();
            log.close();
        }
    }
}
