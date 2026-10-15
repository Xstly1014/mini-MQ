package com.minimq.client;

/**
 * A consumed record as returned by {@link MQConsumer#poll()}. The seq-key
 * string is not carried over the wire — storage keeps only its hash.
 */
public final class ClientRecord {

    private final long offset;
    private final long timestamp;
    private final String topic;
    private final byte[] payload;
    private final long keyHash;

    public ClientRecord(long offset, long timestamp, String topic, byte[] payload, long keyHash) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.topic = topic;
        this.payload = payload;
        this.keyHash = keyHash;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getKeyHash() {
        return keyHash;
    }

    @Override
    public String toString() {
        return "ClientRecord{offset=" + offset + ", topic='" + topic
                + "', payloadSize=" + payload.length + '}';
    }
}
