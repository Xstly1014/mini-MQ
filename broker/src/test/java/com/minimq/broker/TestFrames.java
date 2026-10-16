package com.minimq.broker;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/** Shared frame builders for broker tests. */
final class TestFrames {

    private TestFrames() {
    }

    static Frame produce(long requestId, String topic, String... payloads) {
        MqProto.ProduceRequest.Builder b = MqProto.ProduceRequest.newBuilder();
        for (String p : payloads) {
            b.addMessages(MqProto.MessageData.newBuilder()
                    .setTopic(topic)
                    .setPayload(ByteString.copyFromUtf8(p))
                    .setSeqKey("seq-" + p)
                    .setTimestamp(1L));
        }
        return new Frame(MessageType.PRODUCE, requestId, b.build().toByteArray());
    }

    static Frame fetch(long requestId, String topic, long offset, int maxCount) {
        MqProto.FetchRequest req = MqProto.FetchRequest.newBuilder()
                .setTopic(topic)
                .setOffset(offset)
                .setMaxCount(maxCount)
                .build();
        return new Frame(MessageType.FETCH, requestId, req.toByteArray());
    }

    static MqProto.ProduceAck parseAck(Frame f) throws Exception {
        return MqProto.ProduceAck.parseFrom(f.body());
    }

    /**
     * FETCH_RESP body layout v1.2: [4B metaLen][meta][raw tail]; the meta is
     * the protobuf, the raw tail (zero-copy path) is NOT part of the frame
     * body object — it rides in FileRegions.
     */
    static MqProto.FetchResponse parseFetch(Frame f) throws Exception {
        byte[] body = f.body();
        int metaLen = ByteBuffer.wrap(body).getInt(0);
        return MqProto.FetchResponse.parseFrom(ByteBuffer.wrap(body, 4, metaLen));
    }
}
