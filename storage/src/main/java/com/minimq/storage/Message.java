package com.minimq.storage;

/**
 * Immutable message POJO (format v1.1): timestamp, key hash, topic and opaque
 * payload bytes. Key hash carries the idempotency key hash used by the
 * producer SDK; topic routes fetch filtering until ConsumeQueue arrives in M2.
 */
public class Message {

    private final long timestamp;
    private final long keyHash;
    private final String topic;
    private final byte[] payload;

    /**
     * Global physical offset in the CommitLog, assigned by
     * {@link CommitLog#append}.
     */
    private long offset = -1L;

    public Message(long timestamp, long keyHash, String topic, byte[] payload) {
        this.timestamp = timestamp;
        this.keyHash = keyHash;
        this.topic = topic;
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getKeyHash() {
        return keyHash;
    }

    public String getTopic() {
        return topic;
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
                + ", keyHash=" + keyHash + ", topic='" + topic
                + "', payloadSize=" + payload.length + '}';
    }
}
