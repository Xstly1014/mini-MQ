package com.minimq.protocol;

/**
 * Frame TYPE registry (1 byte on the wire). Values are fixed once released:
 * renumbering breaks compatibility, so append new types only.
 */
public enum MessageType {

    PRODUCE((byte) 1),
    PRODUCE_ACK((byte) 2),
    FETCH((byte) 3),
    FETCH_RESP((byte) 4),
    COMMIT_OFFSET((byte) 5),
    COMMIT_OFFSET_ACK((byte) 6),
    /**
     * Leader -> follower authority heartbeat (M3). Same frame carries the
     * replication lease: a follower resets its election timer on it and
     * answers with {@link #HEARTBEAT_RESP} so the leader can detect a
     * higher-term peer in the reply and step down.
     */
    HEARTBEAT((byte) 7),
    /** Raft vote request; {@code pre_vote = true} marks the PreVote probe. */
    VOTE((byte) 8),
    VOTE_RESP((byte) 9),
    HEARTBEAT_RESP((byte) 10),
    /**
     * Replica sync fetch (M3): a follower pulling the log from the leader.
     * Distinct TYPE from consumer FETCH so the dispatcher can route it to
     * the single replication thread without decoding the body; the BODY is
     * the same FetchRequest/FetchResponse protobuf (Kafka: the follower is
     * "a special consumer"), with follower_id/follower_epoch set.
     */
    FOLLOWER_FETCH((byte) 11);

    private final byte value;

    MessageType(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MessageType of(byte value) {
        for (MessageType t : values()) {
            if (t.value == value) {
                return t;
            }
        }
        return null;
    }
}
