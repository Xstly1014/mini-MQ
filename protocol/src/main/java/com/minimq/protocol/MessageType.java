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
    /** Reserved for M3 lease heartbeats; no handler in M1. */
    HEARTBEAT((byte) 7);

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
