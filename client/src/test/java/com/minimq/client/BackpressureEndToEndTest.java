package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.protobuf.ByteString;
import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerRequestHandler;
import com.minimq.broker.BrokerServer;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;

/**
 * Full backpressure chain over loopback, the M1 manual acceptance item
 * "business pool saturated -> RETRY_LATER -> producer backs off and retries".
 *
 * Determinism: all three PRODUCEs ride ONE connection, so the broker
 * dispatches them in send order on a single EventLoop — request 1 occupies
 * the only business thread (blocked), request 2 fills the queue, request 3
 * MUST be rejected. (The MQProducer's automatic backoff-retry on the same
 * signal is covered separately in MQProducerTest with a scripted transport.)
 */
class BackpressureEndToEndTest {

    @TempDir
    Path tempDir;

    private static byte[] produceBody(String payload) {
        return MqProto.ProduceRequest.newBuilder()
                .addMessages(MqProto.MessageData.newBuilder()
                        .setTopic("t")
                        .setPayload(ByteString.copyFromUtf8(payload)))
                .build()
                .toByteArray();
    }

    private static MqProto.ProduceAck parseAck(Frame f) throws Exception {
        return MqProto.ProduceAck.parseFrom(f.body());
    }

    @Test
    void backpressureChainIsVisibleAndRecoverable() throws Exception {
        final CountDownLatch firstEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final int[] handled = {0};

        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.resolve("broker-data").toFile())
                .segmentSize(256 * 1024)
                .businessThreads(1)
                .businessQueueCapacity(1)) {
            @Override
            protected BrokerRequestHandler createRequestHandler(CommitLog commitLog,
                    ConsumeQueueStore consumeQueues) {
                return new BrokerRequestHandler(commitLog, consumeQueues,
                        FlushMode.OS_FLUSH, 5_000L, false) {
                    @Override
                    public Frame handle(Frame request) {
                        try {
                            if (++handled[0] == 1) {
                                firstEntered.countDown();
                                // occupy the single business thread
                                assertTrue(release.await(20, TimeUnit.SECONDS));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return (Frame) super.handle(request);
                    }
                };
            }
        };
        server.start();
        ClientChannel channel = null;
        try {
            int port = server.port();
            channel = ClientChannel.connect("127.0.0.1", port, 20_000);

            // request 1: runs (blocked inside handle)
            byte[] body1 = produceBody("m1");
            CompletableFuture<Frame> f1 = channel.request(MessageType.PRODUCE, body1);
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS),
                    "first request must reach the business pool");

            // request 2: same connection, dispatched after #1 -> queued
            byte[] body2 = produceBody("m2");
            CompletableFuture<Frame> f2 = channel.request(MessageType.PRODUCE, body2);

            // request 3: thread busy + queue full -> rejected with RETRY_LATER
            byte[] body3 = produceBody("m3");
            CompletableFuture<Frame> f3 = channel.request(MessageType.PRODUCE, body3);

            Frame rejected = f3.get(5, TimeUnit.SECONDS);
            assertEquals(MessageType.PRODUCE_ACK, rejected.type());
            assertEquals(MqProto.ProduceStatus.RETRY_LATER, parseAck(rejected).getStatus(),
                    "saturated pool must answer RETRY_LATER, not block or drop");

            // EventLoop-zero-blocking monitor: the business thread is still
            // blocked inside handle() right now, yet f3's RETRY_LATER came
            // back — so the worker EventLoop served IO all along and its
            // queue must be empty (dispatch is O(us), storage never runs on
            // IO threads).
            assertEquals(0, server.workerPendingTasks(),
                    "worker EventLoops must have no queued tasks while business is slow");

            // release: #1 and #2 complete normally
            release.countDown();
            assertEquals(MqProto.ProduceStatus.OK, parseAck(f1.get(5, TimeUnit.SECONDS)).getStatus());
            assertEquals(MqProto.ProduceStatus.OK, parseAck(f2.get(5, TimeUnit.SECONDS)).getStatus());

            // "producer" behavior on RETRY_LATER: resend the same batch
            CompletableFuture<Frame> f3Retry =
                    channel.request(MessageType.PRODUCE, body3);
            assertEquals(MqProto.ProduceStatus.OK,
                    parseAck(f3Retry.get(5, TimeUnit.SECONDS)).getStatus(),
                    "the rejected batch must succeed after backoff and resend");

            // everything really landed: consume all three back
            MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", port, "t",
                    "g-bp", tempDir.resolve("offsets").toFile(), 10, 5000);
            try {
                List<String> seen = new ArrayList<String>();
                long deadline = System.currentTimeMillis() + 10_000;
                while (seen.size() < 3 && System.currentTimeMillis() < deadline) {
                    for (ClientRecord r : consumer.poll()) {
                        seen.add(new String(r.getPayload(), "UTF-8"));
                    }
                }
                assertEquals(3, seen.size());
                assertTrue(seen.contains("m1") && seen.contains("m2") && seen.contains("m3"));
            } finally {
                consumer.close();
            }
        } finally {
            if (channel != null) {
                channel.close();
            }
            server.shutdown();
        }
    }
}
