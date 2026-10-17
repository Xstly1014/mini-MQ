package com.minimq.broker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partition injection point (M4 chaos matrix): the frame-routing layer drops
 * inbound cluster frames from blacklisted peers — the directional network
 * cut the reliability matrix needs, implemented where the M3-8 fix already
 * routes every inter-node frame (server pipeline AND ClusterClient's own
 * connections, simultaneous-open merged ones included).
 *
 * <h3>Why frames, not connections</h3>
 * A TCP connection is bidirectional: closing it kills both directions, so a
 * one-way cut can never be done at the socket layer. Dropping <i>identified</i>
 * frames at the receiver instead gives exact edges — every cluster frame
 * carries its sender id in the body (VOTE.candidate_id, VoteResponse.
 * responder_id, HeartbeatRequest.leader_id, HeartbeatResponse.responder_id,
 * FetchRequest.follower_id; a FETCH_RESP is attributed to the peer its
 * request was sent to). Reading continues (no TCP backpressure), the sender
 * sees nothing but silence — indistinguishable from a real network partition.
 *
 * <h3>Composing the shapes</h3>
 * <ul>
 * <li><b>Bidirectional A | B</b>: A blocks B <i>and</i> B blocks A — no
 * frame crosses in either direction.</li>
 * <li><b>One-way A → B dead</b>: only B blocks A. B's own frames to A keep
 * flowing (an asymmetric edge, the insidious case: a leader that can
 * shout heartbeats but hears nothing).</li>
 * <li><b>Client traffic is never affected</b>: partitions are between
 * nodes; producer/consumer connections are not cluster frames.</li>
 * </ul>
 *
 * <p>
 * Controlled out-of-band by {@link ChaosControlServer} (loopback only);
 * NOT part of the wire protocol — it is ops tooling the same way an admin
 * port is, and a node without a chaos port has no shield state at all.
 */
public final class PartitionShield {

  private final Set<Integer> inBlack = ConcurrentHashMap.<Integer>newKeySet();

  /** True when an inbound cluster frame from {@code sender} must be dropped. */
  public boolean blocksInbound(int sender) {
    return inBlack.contains(Integer.valueOf(sender));
  }

  public void inAdd(int peerId) {
    inBlack.add(Integer.valueOf(peerId));
  }

  public void inRemove(int peerId) {
    inBlack.remove(Integer.valueOf(peerId));
  }

  public void inClear() {
    inBlack.clear();
  }

  /** Snapshot for the chaos state report (monitoring / tests). */
  public Set<Integer> inBlackSnapshot() {
    return java.util.Collections.unmodifiableSet(
        new java.util.TreeSet<Integer>(inBlack));
  }
}
