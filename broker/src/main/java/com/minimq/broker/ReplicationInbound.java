package com.minimq.broker;

import com.minimq.protocol.Frame;

import io.netty.channel.Channel;

/**
 * EventLoop-side routing target for cluster-internal frames (M3): VOTE /
 * VOTE_RESP / HEARTBEAT / HEARTBEAT_RESP / FOLLOWER_FETCH never enter the
 * business pool — the election messages are O(1) enqueues onto the Raft
 * node's inbox (safe to run on the EventLoop itself), and FOLLOWER_FETCH
 * goes to the single replication thread, which is the entire reason it
 * has its own frame TYPE (route without decoding the body).
 */
public interface ReplicationInbound {

    /**
     * Handle one cluster-internal frame. Called on a Netty EventLoop (or
     * the cluster client's IO thread); implementations must be non-blocking
     * for election messages and enqueue-only for FOLLOWER_FETCH. The
     * {@code channel} is where any response must be written back.
     */
    void onFrame(Channel channel, Frame frame);
}
