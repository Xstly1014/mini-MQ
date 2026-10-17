package com.minimq.broker;

import com.minimq.storage.CommitLog;

/**
 * The seam between request handling and replication (M3): everything the
 * PRODUCE/FETCH paths need to know about cluster authority, in one
 * interface, so {@link BrokerRequestHandler} stays a pure request-to-
 * response function testable without a cluster.
 *
 * <ul>
 *   <li><b>LeaderGate</b> — {@link #isLeader()}: a standalone broker is
 *   always the leader (M1/M2 behavior, zero regression); a cluster node
 *   only serves data-plane requests while it holds the election. Failures
 *   carry {@link #leaderHint()} so the client can redirect instead of
 *   guessing.</li>
 *   <li><b>HW boundary</b> — {@link #highWatermark()}: consumers only ever
 *   see records strictly below the watermark (replicated-majority data);
 *   on a standalone broker the watermark IS the local log end, so the
 *   M1/M2 FETCH semantics are unchanged.</li>
 *   <li><b>acks=all gate</b> — admission ({@link #acksAllAdmissible()},
 *   the min.insync.replicas floor) plus the HW-wait park
 *   ({@link #awaitReplicated}, the replication twin of M2's SYNC_FLUSH
 *   park-and-wake).</li>
 * </ul>
 */
public interface ReplicationGate {

    /** Whether this node currently accepts data-plane requests. */
    boolean isLeader();

    /**
     * Replication high watermark; records at/after it are invisible to
     * consumers. Equals the local log end on a standalone broker.
     */
    long highWatermark();

    /**
     * "host:port" of the current leader as this node knows it, or null
     * when unknown (client falls back to its bootstrap list).
     */
    String leaderHint();

    /** acks=all admission: ISR still at/above min.insync.replicas? */
    boolean acksAllAdmissible();

    /** Whether reads may expose data under the current replication state. */
    default boolean readsAdmissible() {
        return acksAllAdmissible();
    }

    /**
     * Parks until the HW passes {@code offset} (the acks=all durability
     * gate). Returns false on timeout or loss of leadership — the caller
     * must answer ERROR, never a lying OK.
     */
    boolean awaitReplicated(long offset, long timeoutMs) throws InterruptedException;

    /** The local LEO moved: the leader's own number in the HW minimum. */
    void onLocalAppend();

    /**
     * The M1/M2 single-broker behavior: every gate open, the watermark is
     * the local log end, nothing ever parks. Clustered brokers get the
     * real thing from ReplicationService.
     */
    static ReplicationGate standalone(final CommitLog log) {
        return new ReplicationGate() {
            @Override
            public boolean isLeader() {
                return true;
            }

            @Override
            public long highWatermark() {
                return log.maxOffset();
            }

            @Override
            public String leaderHint() {
                return null;
            }

            @Override
            public boolean acksAllAdmissible() {
                return true;
            }

            @Override
            public boolean awaitReplicated(long offset, long timeoutMs) {
                return true; // the local append already covers the batch
            }

            @Override
            public void onLocalAppend() {
                // no ISR to inform
            }
        };
    }
}
