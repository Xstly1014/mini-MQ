package com.minimq.storage;

/**
 * Immutable message POJO. v0 carries only the fields needed by the storage kernel:
 * timestamp, key hash and opaque payload bytes. Topic/queue mapping arrives in M1.
 */
public class Message {

    private final long timestamp;
    private final long keyHash;
    private final byte[] payload;

    /** Global physical offset in the CommitLog, assigned by {@link CommitLog#append}. */
    private long offset = -1L;

    public Message(long timestamp, long keyHash, byte[] payload) {
        this.timestamp = timestamp;
        this.keyHash = keyHash;
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getKeyHash() {
        return keyHash;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getOffset() {
        return offset;
    }

    void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Message{offset=" + offset + ", timestamp=" + timestamp
                + ", keyHash=" + keyHash + ", payloadSize=" + payload.length + '}';
    }
}
