package com.minimq.replica;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Leader-side replication bookkeeping (M3): the ISR roster, the high
 * watermark, and the acks=all durability gate.
 *
 * <h3>The three numbers that make replication safe</h3>
 * <ul>
 *   <li><b>LEO</b> — each replica's log end. Followers report theirs with
 *   every FOLLOWER_FETCH; the leader's own comes from the {@link LongSupplier}
 *   (the live {@code commitLog::maxOffset}).</li>
 *   <li><b>ISR</b> — the trusted-substitute list. Membership is judged by
 *   TIME, not by lagging byte counts (Kafka's post-0.10 rule): a follower
 *   that has caught up to the leader's LEO at least once within
 *   {@code lagMaxMs} is in. Byte-count judgment would evict healthy
 *   followers during traffic spikes — the exact bug class Kafka moved away
 *   from, kept here as a config knob and a story.</li>
 *   <li><b>HW</b> — min(LEO) over the ISR (leader included). Consumers only
 *   see records below it; acks=all producers only wake when it passes their
 *   batch. Shrink-before-stall is the load-bearing detail: a dead follower
 *   left in the ISR pins min(LEO) forever — the classic HW deadlock — so
 *   the sweep timer is not optional decoration.</li>
 * </ul>
 *
 * <h3>acks=all is "all of the ISR", never "all replicas"</h3>
 * That gap is the whole point of the ISR: a slow replica gets evicted and
 * stops gating writes. Its bookend is {@code minIsr}: when the ISR shrinks
 * below it, acks=all writes FAIL (ISR_TOO_SMALL) rather than silently
 * degrade — prefer unavailability over unacknowledged loss.
 *
 * <h3>Concurrency</h3>
 * One monitor. Holders: FOLLOWER_FETCH handlers (business pool), PRODUCE
 * acks=all waits (business pool, parked — the same park-as-aggregation
 * trick as M2's SYNC_FLUSH), and the sweep timer thread. Volume is one
 * critical section per fetch — coarse is correct here.
 */
public final class ReplicaManager {

    private static final Logger LOG = Logger.getLogger(ReplicaManager.class.getName());

    /** Per-follower tracking: what the leader knows about each one. */
    private static final class FollowerState {
        final int id;
        /** Last LEO the follower reported (== its next fetch offset). */
        long leo;
        /** When this follower last fetched at/behind the then-leader LEO — the catch-up clock. */
        long lastCaughtUpMs;
        /** Last time any fetch arrived (liveness, distinct from catching up). */
        long lastFetchMs;

        FollowerState(int id, long nowMs, long presumedLeo) {
            this.id = id;
            this.leo = presumedLeo; // presumed caught up to where the leader is NOW
            this.lastCaughtUpMs = nowMs; // innocent until the first window passes
            this.lastFetchMs = nowMs;
        }
    }

    private final int selfId;
    private final List<Integer> followerIds;
    private final int minIsr;
    private final long lagMaxMs;
    private final LongSupplier logEnd;
    private final Object lock = new Object();
    private final Map<Integer, FollowerState> followers = new HashMap<Integer, FollowerState>();
    private final Set<Integer> isr = new HashSet<Integer>();

    private boolean leader;
    private long highWatermark;
    /** Observed-on-follower HW mirror (monitoring; followers record what leaders sent). */
    private final AtomicLong followerViewHw = new AtomicLong(-1);

    /** Monotonic HW-change counter (tests/monitoring: "did it move?"). */
    private volatile long hwEpoch;

    private ReplicaManager(int selfId, List<Integer> followerIds, int minIsr,
            long lagMaxMs, LongSupplier logEnd) {
        this.selfId = selfId;
        this.followerIds = followerIds;
        this.minIsr = minIsr;
        this.lagMaxMs = lagMaxMs;
        this.logEnd = logEnd;
    }

    public static ReplicaManager newLeader(int selfId, List<Integer> followerIds,
            int minIsr, long lagMaxMs, LongSupplier logEnd) {
        ReplicaManager m = new ReplicaManager(selfId, followerIds, minIsr, lagMaxMs, logEnd);
        m.becomeLeader();
        return m;
    }

    /**
     * Boot state for a cluster node (M3-6 wiring): neither leading nor
     * tracking anyone — the election promotes it via {@link #becomeLeader}
     * when it wins, so the knobs must be the real ones from day one.
     */
    public static ReplicaManager newStandby(int selfId, List<Integer> followerIds,
            int minIsr, long lagMaxMs, LongSupplier logEnd) {
        return new ReplicaManager(selfId, followerIds, minIsr, lagMaxMs, logEnd);
    }

    // ------------------------------------------------------------------
    // role transitions (wiring calls these from the RaftNode listener)
    // ------------------------------------------------------------------

    /** Election won: everyone is presumed in-sync until the clock says otherwise. */
    public void becomeLeader() {
        synchronized (lock) {
            leader = true;
            long now = System.currentTimeMillis();
            followers.clear();
            isr.clear();
            for (int id : followerIds) {
                followers.put(id, new FollowerState(id, now, logEnd.getAsLong()));
                isr.add(id);
            }
            // Start from the leader's own LEO: the safe upper bound (no
            // un-replicated data is claimed committed). Followers' first
            // fetches pull it down to the true min within one round trip.
            highWatermark = logEnd.getAsLong();
        }
    }

    /** Deposed (or never was): stop tracking, stop gating. */
    public void becomeFollower() {
        synchronized (lock) {
            leader = false;
            followers.clear();
            isr.clear();
            highWatermark = 0L;
            lock.notifyAll(); // release any parked acks=all producers
        }
    }

    // ------------------------------------------------------------------
    // follower-driven updates (business pool, one per FOLLOWER_FETCH)
    // ------------------------------------------------------------------

    /**
     * A follower fetched: record its position and liveness, possibly widen
     * the ISR, recompute the HW, wake acks=all waiters the new HW covers.
     *
     * @return the current high watermark (rides back in the FETCH_RESP)
     */
    public long onFollowerFetch(int followerId, long fetchOffset) {
        synchronized (lock) {
            if (!leader) {
                return 0L;
            }
            long now = System.currentTimeMillis();
            FollowerState f = followers.get(followerId);
            if (f == null) {
                return highWatermark; // unknown peer: ignore, never crash the loop
            }
            f.lastFetchMs = now;
            f.leo = fetchOffset;
            if (fetchOffset >= logEnd.getAsLong()) {
                f.lastCaughtUpMs = now; // caught up to the leader AS IT IS NOW
                if (!isr.contains(followerId)) {
                    isr.add(followerId); // re-admission: it proved currency
                }
            }
            advanceHwLocked();
            return highWatermark;
        }
    }

    /** Local append landed: the leader's own LEO moved, so might the HW. */
    public void onLocalAppend() {
        synchronized (lock) {
            if (leader) {
                advanceHwLocked();
            }
        }
    }

    // ------------------------------------------------------------------
    // the sweep: shrink the stuck, keep the HW moving
    // ------------------------------------------------------------------

    /**
     * Periodic health check (timer thread): evict followers whose catch-up
     * clock expired, then recompute. This is the anti-deadload armature —
     * without it one dead replica freezes the HW and every acks=all
     * producer in the cluster.
     */
    public void sweep() {
        synchronized (lock) {
            if (!leader) {
                return;
            }
            long now = System.currentTimeMillis();
            boolean changed = false;
            for (FollowerState f : followers.values()) {
                if (isr.contains(f.id) && now - f.lastCaughtUpMs > lagMaxMs) {
                    isr.remove(f.id);
                    changed = true;
                    LOG.warning("node-" + f.id + " evicted from ISR (no catch-up in "
                            + lagMaxMs + "ms, leo=" + f.leo + ")");
                }
            }
            if (changed) {
                advanceHwLocked();
            }
        }
    }

    // ------------------------------------------------------------------
    // queries & gates
    // ------------------------------------------------------------------

    /** HW = min(LEO) over ISR (leader included). */
    private void advanceHwLocked() {
        long hw = logEnd.getAsLong();
        for (int id : isr) {
            FollowerState f = followers.get(id);
            if (f != null) {
                hw = Math.min(hw, f.leo);
            }
        }
        // Deliberately NOT monotonic within a reign: becomeLeader seeds the
        // HW with the leader's own LEO as an optimistic upper bound (every
        // follower PRESUMED caught up); the first real fetch reports pull it
        // down to the truth. After that every input ascends — fetch offsets
        // and local appends are monotone, evictions only REMOVE a min — so
        // the dip happens exactly once per follower, right at its first
        // report, and never below any position a follower actually vouched
        // for. This is Kafka's leader-change behavior: the optimistic seed
        // spans one fetch round trip, not a durability claim.
        if (hw != highWatermark) {
            highWatermark = hw;
            hwEpoch++;
            lock.notifyAll(); // acks=all waiters, your batch may be covered
        }
    }

    /** Current high watermark (leader view; -1 when not leading). */
    public long highWatermark() {
        synchronized (lock) {
            return leader ? highWatermark : -1L;
        }
    }

    /** ISR snapshot (monitoring / FETCH_RESP decoration). */
    public Set<Integer> isr() {
        synchronized (lock) {
            return new HashSet<Integer>(isr);
        }
    }

    public int isrSize() {
        synchronized (lock) {
            return leader ? isr.size() + 1 : 0; // leader counts itself
        }
    }

    /** acks=all admission: enough in-sync replicas to promise durability. */
    public boolean acksAllAdmissible() {
        synchronized (lock) {
            return leader && isrSizeLocked() >= minIsr;
        }
    }

    private int isrSizeLocked() {
        return isr.size() + 1;
    }

    // ------------------------------------------------------------------
    // acks=all durability gate
    // ------------------------------------------------------------------

    /**
     * Parks the calling business thread until the HW passes {@code offset}
     * (the end of its just-appended batch) — the replication twin of M2's
     * SYNC_FLUSH park, and like it, the park IS the write aggregation.
     *
     * @return true if replicated (HW >= offset), false on timeout/step-down
     */
    public boolean awaitReplicated(long offset, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (lock) {
            while (leader && highWatermark < offset) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                lock.wait(remaining);
            }
            return leader && highWatermark >= offset;
        }
    }

    // ------------------------------------------------------------------
    // follower-mode mirror (wiring records the HW the leader sends)
    // ------------------------------------------------------------------

    /** A follower records the leader-advertised HW (monitoring/recovery). */
    public void onLeaderHighWatermark(long hw) {
        followerViewHw.set(hw);
    }

    /** Last HW this node saw from its leader (-1 if none). */
    public long followerViewHighWatermark() {
        return followerViewHw.get();
    }

    /** HW advance count (tests assert "it moved"). */
    public long hwAdvanceCount() {
        return hwEpoch;
    }

    public int selfId() {
        return selfId;
    }

    public List<Integer> followerIds() {
        return Collections.unmodifiableList(followerIds);
    }
}
