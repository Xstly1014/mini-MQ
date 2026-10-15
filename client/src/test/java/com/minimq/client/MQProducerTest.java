package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * Producer logic against a scripted transport: batching, RETRY_LATER
 * backoff, retry exhaustion and close() drain — no broker involved, so the
 * failure paths are deterministic.
 */
class MQProducerTest {

    /** Scripted transport: the responder decides each request's fate. */
    private static final class FakeTransport implements Transport {
        interface Responder {
            CompletableFuture<Frame> respond(Frame request, int requestIndex);
        }

        final Responder responder;
        final AtomicInteger requests = new AtomicInteger();
        final CopyOnWriteArrayList<byte[]> bodies = new CopyOnWriteArrayList<byte[]>();

        FakeTransport(Responder responder) {
            this.responder = responder;
        }

        @Override
        public CompletableFuture<Frame> request(MessageType type, byte[] body) {
            bodies.add(body);
            return responder.respond(new Frame(type, -1, body), requests.getAndIncrement());
        }

        @Override
        public void close() {
        }

        MqProto.ProduceRequest lastBody() throws Exception {
            return MqProto.ProduceRequest.parseFrom(bodies.get(bodies.size() - 1));
        }
    }

    private static Frame produceAck(MqProto.ProduceStatus status, long... offsets) {
        MqProto.ProduceAck.Builder b = MqProto.ProduceAck.newBuilder().setStatus(status);
        for (long o : offsets) {
            b.addOffsets(o);
        }
        return new Frame(MessageType.PRODUCE_ACK, 0, b.build().toByteArray());
    }

    private static CompletableFuture<Frame> done(Frame f) {
        return CompletableFuture.completedFuture(f);
    }

    @Test
    void budgetFillFlushesImmediatelyWithoutWaitingForLinger() throws Exception {
        // budget = 1 byte: every single send exceeds it, so it must leave as
        // its own batch right away (no 60s linger wait)
        FakeTransport transport = new FakeTransport((req, i) ->
                done(produceAck(MqProto.ProduceStatus.OK, i * 10L)));
        MQProducer producer = new MQProducer(transport, 1, 60_000, 3, 1);

        CompletableFuture<Long> f1 = producer.send("t", new byte[10]);
        assertEquals(0L, f1.get(2, TimeUnit.SECONDS));
        assertEquals(1, transport.bodies.size(), "budget exceeded: flushed at once");

        producer.close();
    }

    @Test
    void manySendsWithAsOneBatchOnClose() throws Exception {
        // huge byte budget + long linger: nothing flushes until close()
        FakeTransport transport = new FakeTransport((req, i) -> {
            try {
                MqProto.ProduceRequest r = MqProto.ProduceRequest.parseFrom(req.body());
                long[] offsets = new long[r.getMessagesCount()];
                for (int j = 0; j < offsets.length; j++) {
                    offsets[j] = j;
                }
                return done(produceAck(MqProto.ProduceStatus.OK, offsets));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        MQProducer producer = new MQProducer(transport, 1024 * 1024, 60_000, 3, 1);

        CompletableFuture<Long> a = producer.send("t", "a".getBytes());
        CompletableFuture<Long> b = producer.send("t", "b".getBytes());
        CompletableFuture<Long> c = producer.send("t", "c".getBytes());

        assertEquals(0, transport.bodies.size(), "linger not expired, budget not filled: nothing sent yet");

        producer.close();

        assertEquals(1, transport.bodies.size(), "close() must flush the buffer as one batch");
        assertEquals(3, transport.lastBody().getMessagesCount());
        assertEquals(0L, a.get(1, TimeUnit.SECONDS));
        assertEquals(1L, b.get(1, TimeUnit.SECONDS));
        assertEquals(2L, c.get(1, TimeUnit.SECONDS));
    }

    @Test
    void retryLaterBacksOffAndEventuallySucceeds() throws Exception {
        FakeTransport transport = new FakeTransport((req, i) -> {
            if (i < 2) {
                return done(produceAck(MqProto.ProduceStatus.RETRY_LATER, -1L));
            }
            return done(produceAck(MqProto.ProduceStatus.OK, 77L));
        });
        MQProducer producer = new MQProducer(transport, 1, 10, 5, 5);

        CompletableFuture<Long> f = producer.send("t", new byte[10]);
        assertEquals(77L, f.get(5, TimeUnit.SECONDS), "third attempt must succeed");
        assertEquals(3, transport.bodies.size(), "two rejections + one success = three requests");
        producer.close();
    }

    @Test
    void exhaustedRetriesFailTheFuture() throws Exception {
        FakeTransport transport = new FakeTransport((req, i) ->
                done(produceAck(MqProto.ProduceStatus.RETRY_LATER, -1L)));
        MQProducer producer = new MQProducer(transport, 1, 10, 2, 1);

        CompletableFuture<Long> f = producer.send("t", new byte[10]);
        try {
            f.get(5, TimeUnit.SECONDS);
            throw new AssertionError("future must fail after retry exhaustion");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(String.valueOf(e.getCause()).contains("retries"));
        }
        assertEquals(3, transport.bodies.size(), "initial attempt + 2 retries");
        producer.close();
    }

    @Test
    void brokerErrorFailsImmediatelyWithoutRetry() throws Exception {
        FakeTransport transport = new FakeTransport((req, i) ->
                done(produceAck(MqProto.ProduceStatus.ERROR, new long[0])));
        // ERROR is semantic: a malformed message stays malformed, retrying is noise
        MQProducer producer = new MQProducer(transport, 1, 10, 5, 1);

        CompletableFuture<Long> f = producer.send("t", new byte[10]);
        try {
            f.get(2, TimeUnit.SECONDS);
            throw new AssertionError("future must fail on broker ERROR");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(String.valueOf(e.getCause()).contains("broker error"));
        }
        assertEquals(1, transport.bodies.size(), "no retries for ERROR status");
        producer.close();
    }

    @Test
    void sendAfterCloseFailsImmediately() throws Exception {
        FakeTransport transport = new FakeTransport((req, i) ->
                done(produceAck(MqProto.ProduceStatus.OK, 0L)));
        MQProducer producer = new MQProducer(transport, 1, 10, 3, 1);
        producer.close();

        CompletableFuture<Long> f = producer.send("t", new byte[10]);
        assertTrue(f.isCompletedExceptionally());
    }

    @Test
    void relayAfterAckSlicesOversizedBacklogIntoBudgetBatches() throws Exception {
        // Regression for the Reconcile 17 MiB-frame crash: while a batch is
        // in flight the buffer keeps growing; the ack-time relay must ship it
        // as budget-sized slices (wire frames cap at 1 MiB), never one huge
        // frame. Batch 1 (oversized single send) blocks on a gate; 6x50B
        // sends pile up behind it; budget 200 fits 2 of them per batch.
        final CompletableFuture<Frame> gate = new CompletableFuture<Frame>();
        FakeTransport transport = new FakeTransport((req, i) -> {
            if (i == 0) {
                return gate; // batch 1 stays in flight
            }
            try {
                MqProto.ProduceRequest r = MqProto.ProduceRequest.parseFrom(req.body());
                long[] offsets = new long[r.getMessagesCount()];
                return done(produceAck(MqProto.ProduceStatus.OK, offsets));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        MQProducer producer = new MQProducer(transport, 200, 60_000, 5, 1);

        CompletableFuture<Long> first = producer.send("t", new byte[500]); // flushes alone
        CompletableFuture<Long>[] backlog = new CompletableFuture[6];
        for (int i = 0; i < backlog.length; i++) {
            backlog[i] = producer.send("t", new byte[50]); // est 99B each, piles up
        }
        assertEquals(1, transport.bodies.size(), "backlog must wait behind the in-flight batch");

        gate.complete(produceAck(MqProto.ProduceStatus.OK, 0L)); // release batch 1
        assertEquals(0L, first.get(2, TimeUnit.SECONDS));

        producer.close();

        // 6 backlog sends x ~99B under a 200B budget => 2 per batch => 3 slices
        assertEquals(4, transport.bodies.size(), "1 initial + 3 budget-sized relay slices");
        int totalMessages = 0;
        for (byte[] body : transport.bodies) {
            MqProto.ProduceRequest r = MqProto.ProduceRequest.parseFrom(body);
            assertTrue(r.getMessagesCount() <= 2,
                    "each batch must respect the byte budget, got " + r.getMessagesCount());
            totalMessages += r.getMessagesCount();
        }
        assertEquals(7, totalMessages, "1 gated + 6 backlog");
        for (CompletableFuture<Long> f : backlog) {
            assertTrue(f.isDone() && !f.isCompletedExceptionally());
        }
    }

    @Test
    void sameTopicOffsetsStayOrderedAcrossBatches() throws Exception {
        // estSize per send ~99B (50 payload + 1 topic + 48 overhead): a 200B
        // budget makes batches of 2; 6 sends = 3 batches, offsets must still
        // come back in send order
        FakeTransport transport = new FakeTransport((req, i) -> {
            try {
                MqProto.ProduceRequest r = MqProto.ProduceRequest.parseFrom(req.body());
                long[] offsets = new long[r.getMessagesCount()];
                for (int j = 0; j < offsets.length; j++) {
                    offsets[j] = i * 3L + j;
                }
                return done(produceAck(MqProto.ProduceStatus.OK, offsets));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        MQProducer producer = new MQProducer(transport, 200, 60_000, 5, 1);

        CompletableFuture<Long>[] futures = new CompletableFuture[6];
        for (int i = 0; i < 6; i++) {
            futures[i] = producer.send("t", new byte[50]);
        }
        producer.close();

        long prev = -1;
        for (CompletableFuture<Long> f : futures) {
            long offset = f.get(1, TimeUnit.SECONDS);
            assertTrue(offset > prev, "same-topic offsets must be strictly increasing");
            prev = offset;
        }
    }
}
