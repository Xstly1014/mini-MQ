package com.minimq.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * ReplicaManager (M3-5) semantics: HW = min(LEO of ISR), time-based ISR
 * eviction/re-admission, the min.insync.replicas gate, and the acks=all
 * park-and-wake durability gate.
 */
class ReplicaManagerTest {

    private static final long LAG_MAX_MS = 80L;

    /** A mutable stand-in for the leader's log end. */
    private final AtomicLong logEnd = new AtomicLong();

    private ReplicaManager threeNodeLeader(long initialLogEnd) {
        logEnd.set(initialLogEnd);
        return ReplicaManager.newLeader(0, Arrays.asList(1, 2), 2, LAG_MAX_MS,
                new java.util.function.LongSupplier() {
                    @Override
                    public long getAsLong() {
                        return logEnd.get();
                    }
                });
    }

    @Test
    void hwIsMinOverIsrIncludingLeader() {
        ReplicaManager leader = threeNodeLeader(1000);
        try {
            leader.onFollowerFetch(1, 600);
            assertEquals(600L, leader.onFollowerFetch(2, 800));
            assertEquals(600L, leader.highWatermark(), "HW = min(600, 800, 1000)");

            leader.onFollowerFetch(1, 1000); // node-1 catches up
            assertEquals(800L, leader.highWatermark(), "HW follows the laggard up");
        } finally {
            leader.becomeFollower();
        }
    }

    @Test
    void newLeaderStartsHwAtOwnLeoUntilFollowersReport() {
        ReplicaManager leader = threeNodeLeader(500);
        try {
            assertEquals(500L, leader.highWatermark());
            // first fetch from a lagging follower pulls it to the truth
            assertEquals(200L, leader.onFollowerFetch(1, 200));
        } finally {
            leader.becomeFollower();
        }
    }

    /**
     * The HW deadlock killer: a follower that stopped catching up must be
     * evicted by the sweep, un-pinning the watermark for everyone else.
     */
    @Test
    void sweepEvictsStaleFollowerAndHwUnsticks() throws Exception {
        ReplicaManager leader = threeNodeLeader(1000);
        try {
            leader.onFollowerFetch(1, 600);
            leader.onFollowerFetch(2, 900);
            assertEquals(600L, leader.highWatermark());

            // node-1 goes silent; node-2 keeps fetching, caught up
            long sleepMs = LAG_MAX_MS + 60;
            long deadline = System.currentTimeMillis() + sleepMs;
            while (System.currentTimeMillis() < deadline) {
                leader.onFollowerFetch(2, 1000);
                Thread.sleep(10);
            }
            leader.sweep();

            assertFalse(leader.isr().contains(1), "stale follower evicted");
            assertEquals(1000L, leader.highWatermark(),
                    "HW unstuck: min over the SHRUNK ISR");
        } finally {
            leader.becomeFollower();
        }
    }

    @Test
    void evictedFollowerIsReadmittedOnCatchUp() throws Exception {
        ReplicaManager leader = threeNodeLeader(1000);
        try {
            leader.onFollowerFetch(1, 600);
            leader.onFollowerFetch(2, 900);

            long sleepMs = LAG_MAX_MS + 60;
            long deadline = System.currentTimeMillis() + sleepMs;
            while (System.currentTimeMillis() < deadline) {
                leader.onFollowerFetch(2, 1000);
                Thread.sleep(10);
            }
            leader.sweep();
            assertFalse(leader.isr().contains(1));

            // node-1 heals and fetches AT the leader end: readmission is
            // immediate (it proved currency with this very fetch)
            leader.onFollowerFetch(1, 1000);
            assertTrue(leader.isr().contains(1), "healed follower back in the ISR");
        } finally {
            leader.becomeFollower();
        }
    }

    /**
     * min.insync.replicas=2: with one follower evicted the gate holds; with
     * both gone, acks=all writes must FAIL (ISR_TOO_SMALL at the call site),
     * never silently degrade.
     */
    @Test
    void acksAllGateFollowsMinIsr() throws Exception {
        ReplicaManager leader = threeNodeLeader(1000);
        try {
            assertTrue(leader.acksAllAdmissible(), "full ISR of 3");
            assertEquals(3, leader.isrSize());

            long sleepMs = LAG_MAX_MS + 60;
            long deadline = System.currentTimeMillis() + sleepMs;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            leader.sweep(); // both followers stale now
            assertEquals(1, leader.isrSize());
            assertFalse(leader.acksAllAdmissible(),
                    "ISR below minIsr must refuse acks=all, not downgrade it");
        } finally {
            leader.becomeFollower();
        }
    }

    /**
     * The replication twin of M2's group-commit park: the producer thread
     * parks on awaitReplicated; the follower fetch that pushes the HW past
     * its batch end wakes it. The park occupies the thread ON PURPOSE.
     */
    @Test
    void awaitReplicatedParksUntilHwPasses() throws Exception {
        final ReplicaManager leader = threeNodeLeader(0);
        try {
            logEnd.set(1000); // local append: offset 1000 not yet replicated
            leader.onLocalAppend();
            assertEquals(0L, leader.highWatermark(), "followers at 0 hold the HW down");

            final boolean[] replicated = new boolean[1];
            Thread producer = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        replicated[0] = leader.awaitReplicated(1000, 5_000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }, "acks-all-producer");
            producer.start();

            Thread.sleep(100); // let it park
            // Timed wait, same shape as M2's awaitDurable: the park must be
            // un-busy (sleep would also show TIMED_WAITING — the join below
            // is what proves the wake was event-driven, not a timeout).
            assertEquals(Thread.State.TIMED_WAITING, producer.getState(),
                    "producer must be parked while the batch is un-replicated");

            leader.onFollowerFetch(1, 1000); // both followers confirm
            leader.onFollowerFetch(2, 1000);
            producer.join(2_000);
            assertTrue(replicated[0], "HW passed the batch: producer released");
        } finally {
            leader.becomeFollower();
        }
    }

    /** Deposed leader releases parked producers with a failure, not a hang. */
    @Test
    void stepDownReleasesParkedProducers() throws Exception {
        final ReplicaManager leader = threeNodeLeader(0);
        logEnd.set(1000);
        leader.onLocalAppend();

        final boolean[] released = new boolean[1];
        Thread producer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    released[0] = leader.awaitReplicated(1000, 60_000);
                } catch (InterruptedException ignored) {
                }
            }
        }, "acks-all-producer");
        producer.start();
        Thread.sleep(100);

        leader.becomeFollower();
        producer.join(2_000);
        assertFalse(released[0], "deposed leader cannot promise replication");
        assertEquals(-1L, leader.highWatermark(), "not leading: no HW to speak of");
    }

    @Test
    void followerIgnoresFetchesWhenNotLeading() {
        ReplicaManager m = ReplicaManager.newLeader(0,
                new ArrayList<Integer>(Arrays.asList(1)), 1, LAG_MAX_MS,
                new java.util.function.LongSupplier() {
                    @Override
                    public long getAsLong() {
                        return 0;
                    }
                });
        m.becomeFollower();
        assertEquals(0L, m.onFollowerFetch(1, 100), "non-leader tracks nothing");
        assertEquals(-1L, m.highWatermark());
    }

    @Test
    void isrSnapshotIsACopy() {
        ReplicaManager leader = threeNodeLeader(100);
        try {
            Set<Integer> snapshot = leader.isr();
            snapshot.clear();
            assertEquals(2, leader.isr().size(), "mutation of the snapshot must not touch the manager");
        } finally {
            leader.becomeFollower();
        }
    }
}
