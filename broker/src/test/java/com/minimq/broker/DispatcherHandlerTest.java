package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;

import io.netty.channel.embedded.EmbeddedChannel;

class DispatcherHandlerTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    private CommitLog commitLog;
    private ConsumeQueueStore consumeQueues;
    private ThreadPoolExecutor pool;

    @BeforeEach
    void setUp() throws IOException {
        commitLog = CommitLog.open(tempDir.resolve("log").toFile(), SEGMENT_SIZE);
        consumeQueues = ConsumeQueueStore.open(tempDir.resolve("cq").toFile(), commitLog);
        // 1 thread + queue of 1: the smallest pool that can still saturate,
        // so the rejection path is deterministic to trigger.
        pool = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1),
                new java.util.concurrent.ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "test-biz");
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    @AfterEach
    void tearDown() throws IOException {
        pool.shutdownNow();
        consumeQueues.close();
        commitLog.close();
    }

    /** Default handler over this test's log + index store (OS_FLUSH tier). */
    private BrokerRequestHandler newHandler() {
        return new BrokerRequestHandler(commitLog, consumeQueues, FlushMode.OS_FLUSH, 5_000L, false);
    }

    /** Drains business-pool results from the embedded channel, with timeout. */
    private static Frame awaitOutbound(EmbeddedChannel ch) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            ch.runPendingTasks(); // executes writes scheduled from the biz thread
            Frame f = (Frame) ch.readOutbound();
            if (f != null) {
                return f;
            }
            Thread.sleep(5);
        }
        return null;
    }

    @Test
    void dispatchesProduceToBusinessPoolAndWritesAck() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(
                new DispatcherHandler(pool, newHandler()));

        // writeInbound returns false here: the dispatcher consumed the frame,
        // nothing reaches the pipeline tail — that is the expected path.
        ch.writeInbound(TestFrames.produce(1, "t", "hello"));

        Frame resp = awaitOutbound(ch);
        assertNotNull(resp, "produce ack timed out");
        assertEquals(MessageType.PRODUCE_ACK, resp.type());
        assertEquals(1, resp.requestId());
        assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(resp).getStatus());

        // the message really went through the business pool into the log
        assertEquals(1, commitLog.readFrom(0, 10).size());
        ch.close();
    }

    @Test
    void saturatedPoolAnswersRetryLaterWithoutBlockingTheEventLoop() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        BrokerRequestHandler blocking = new BrokerRequestHandler(
                commitLog, consumeQueues, FlushMode.OS_FLUSH, 5_000L, false) {
            @Override
            public Frame handle(Frame request) {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return (Frame) super.handle(request);
            }
        };
        EmbeddedChannel ch = new EmbeddedChannel(new DispatcherHandler(pool, blocking));

        // task 1: occupies the single business thread (blocked inside handle)
        ch.writeInbound(TestFrames.produce(1, "t", "one"));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "business thread never started");
        // task 2: fills the queue (capacity 1)
        ch.writeInbound(TestFrames.produce(2, "t", "two"));
        // task 3: must be rejected -> RETRY_LATER written synchronously on
        // the EventLoop path (test thread), i.e. readable without waiting
        ch.writeInbound(TestFrames.produce(3, "t", "three"));

        Frame rejected = (Frame) ch.readOutbound();
        assertNotNull(rejected, "rejection must answer immediately, not via the pool");
        assertEquals(MessageType.PRODUCE_ACK, rejected.type());
        assertEquals(3, rejected.requestId());
        assertEquals(MqProto.ProduceStatus.RETRY_LATER, TestFrames.parseAck(rejected).getStatus());

        // release the business thread: requests 1 and 2 complete normally
        release.countDown();
        Frame first = awaitOutbound(ch);
        Frame second = awaitOutbound(ch);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(first).getStatus());
        assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(second).getStatus());
        assertEquals(1, first.requestId());
        assertEquals(2, second.requestId());
        ch.close();
    }

    @Test
    void handlerFailureTurnsIntoErrorResponseNotSilence() throws Exception {
        BrokerRequestHandler failing = new BrokerRequestHandler(
                commitLog, consumeQueues, FlushMode.OS_FLUSH, 5_000L, false) {
            @Override
            public Frame handle(Frame request) {
                throw new IllegalStateException("boom");
            }
        };
        EmbeddedChannel ch = new EmbeddedChannel(new DispatcherHandler(pool, failing));

        ch.writeInbound(TestFrames.produce(1, "t", "x"));

        Frame resp = awaitOutbound(ch);
        assertNotNull(resp);
        assertEquals(MessageType.PRODUCE_ACK, resp.type());
        MqProto.ProduceAck ack = TestFrames.parseAck(resp);
        assertEquals(MqProto.ProduceStatus.ERROR, ack.getStatus());
        assertTrue(ack.getError().contains("boom"));
        ch.close();
    }
}
