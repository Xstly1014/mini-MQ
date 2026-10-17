package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

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

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * M3-6 wiring tests: the ReplicationGate seams in the request handler and
 * the cluster-frame routing in the dispatcher. The gate here is a fake —
 * the REAL gate behaviors (ISR sweep, HW min, election) are covered by the
 * replica module's own tests; what these tests pin down is the CONTRACT
 * between broker and gate:
 *
 * <ul>
 *   <li>non-leaders redirect instead of appending invisible writes,</li>
 *   <li>FETCH serves strictly below the HW,</li>
 *   <li>the three ack tiers wait for exactly what they promise,
 *       nothing more,</li>
 *   <li>cluster frames never touch the business pool.</li>
 * </ul>
 */
class ReplicationWiringTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    private CommitLog commitLog;
    private ConsumeQueueStore consumeQueues;

    /** Scriptable gate: every knob the handler reads, plus await accounting. */
    private static final class FakeGate implements ReplicationGate {
        volatile boolean leader = true;
        volatile long hw = Long.MAX_VALUE;
        volatile String hint = "leader-host:9000";
        volatile boolean admissible = true;
        volatile boolean replicated = true;
        int awaitCalls;
        int localAppends;

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public long highWatermark() {
            return hw;
        }

        @Override
        public String leaderHint() {
            return hint;
        }

        @Override
        public boolean acksAllAdmissible() {
            return admissible;
        }

        @Override
        public boolean awaitReplicated(long offset, long timeoutMs) {
            awaitCalls++;
            return replicated;
        }

        @Override
        public void onLocalAppend() {
            localAppends++;
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        commitLog = CommitLog.open(tempDir.resolve("log").toFile(), SEGMENT_SIZE);
        consumeQueues = ConsumeQueueStore.open(tempDir.resolve("cq").toFile(), commitLog);
    }

    @AfterEach
    void tearDown() throws IOException {
        consumeQueues.close();
        commitLog.close();
    }

    private BrokerRequestHandler handler(FakeGate gate, long awaitTimeoutMs) {
        return new BrokerRequestHandler(commitLog, consumeQueues, FlushMode.OS_FLUSH,
                5_000L, false, gate, awaitTimeoutMs);
    }

    private void awaitIndex() throws InterruptedException {
        assertTrue(consumeQueues.awaitDispatchedTo(commitLog.maxOffset(), 5_000),
                "index dispatch fell behind the log");
    }

    // ------------------------------------------------------------------
    // leader gate
    // ------------------------------------------------------------------

    @Test
    void nonLeaderProduceIsRefusedWithHintAndNothingAppended() throws Exception {
        FakeGate gate = new FakeGate();
        gate.leader = false;
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.produce(7, "t", "a", "b"));

        assertEquals(MessageType.PRODUCE_ACK, resp.type());
        MqProto.ProduceAck ack = TestFrames.parseAck(resp);
        assertEquals(MqProto.ProduceStatus.ERROR, ack.getStatus());
        assertEquals(MqProto.ErrorCode.NOT_LEADER, ack.getErrorCode());
        assertEquals("leader-host:9000", ack.getLeaderHint());
        assertEquals(2, ack.getOffsetsCount(), "one -1 offset per refused message");
        assertEquals(-1L, ack.getOffsets(0));
        assertEquals(-1L, ack.getOffsets(1));
        assertEquals(0, commitLog.readFrom(0, 10).size(), "a non-leader must never append");
        assertEquals(0, gate.localAppends);
    }

    @Test
    void nonLeaderFetchRedirectsWithHintAndEchoesOffset() throws Exception {
        FakeGate gate = new FakeGate();
        gate.leader = false;
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.fetch(9, "t", 123L, 10));

        assertEquals(MessageType.FETCH_RESP, resp.type());
        MqProto.FetchResponse fr = TestFrames.parseFetch(resp);
        assertEquals(MqProto.ErrorCode.NOT_LEADER, fr.getErrorCode());
        assertEquals("leader-host:9000", fr.getLeaderHint());
        assertEquals(123L, fr.getNextOffset(), "the re-bootstrap cursor stays put");
    }

    // ------------------------------------------------------------------
    // HW visibility boundary
    // ------------------------------------------------------------------

    @Test
    void fetchServesOnlyRecordsStrictlyBelowTheHighWatermark() throws Exception {
        FakeGate gate = new FakeGate();
        BrokerRequestHandler h = handler(gate, 5_000L);

        // two records; the HW will sit exactly between them
        Frame first = (Frame) h.handle(TestFrames.produce(1, "t", "one"));
        Frame second = (Frame) h.handle(TestFrames.produce(2, "t", "two"));
        long firstStart = TestFrames.parseAck(first).getOffsets(0);
        long firstEnd = TestFrames.parseAck(second).getOffsets(0); // record 2 starts where 1 ends
        long logEnd = commitLog.maxOffset();
        awaitIndex();
        gate.hw = firstEnd; // record 1 visible, record 2 still above the watermark

        Frame resp = (Frame) h.handle(TestFrames.fetch(3, "t", 0, 10));
        MqProto.FetchResponse fr = TestFrames.parseFetch(resp);
        assertEquals(1, fr.getRecordsCount(), "the unreplicated record must be invisible");
        assertEquals(firstEnd, fr.getNextOffset(), "next_offset stops at the HW");
        assertEquals(firstEnd, fr.getHighWatermark());
        assertEquals(firstStart, fr.getRecords(0).getOffset());

        // the watermark passes the whole log: the second record surfaces
        gate.hw = logEnd;
        Frame resp2 = (Frame) h.handle(TestFrames.fetch(4, "t", firstEnd, 10));
        MqProto.FetchResponse fr2 = TestFrames.parseFetch(resp2);
        assertEquals(1, fr2.getRecordsCount());
        assertEquals(firstEnd, fr2.getRecords(0).getOffset());
    }

    @Test
    void fullyShadowedFetchReturnsNothingAndKeepsNextOffset() throws Exception {
        FakeGate gate = new FakeGate();
        BrokerRequestHandler h = handler(gate, 5_000L);
        h.handle(TestFrames.produce(1, "t", "one"));
        awaitIndex();
        gate.hw = 0; // nothing replicated yet

        Frame resp = (Frame) h.handle(TestFrames.fetch(2, "t", 0, 10));
        MqProto.FetchResponse fr = TestFrames.parseFetch(resp);
        assertEquals(0, fr.getRecordsCount());
        assertEquals(0L, fr.getNextOffset(), "the consumer cursor does not skip past the HW");
    }

    @Test
    void fetchRefusesWhenIsrIsTooSmallThenRespectsHwAfterRecovery() throws Exception {
        FakeGate gate = new FakeGate();
        BrokerRequestHandler h = handler(gate, 5_000L);

        h.handle(TestFrames.produce(1, "t", "one"));
        h.handle(TestFrames.produce(2, "t", "two"));
        awaitIndex();
        long thirdStart = TestFrames.parseAck((Frame) h.handle(
                TestFrames.produce(3, "t", "three"))).getOffsets(0);
        h.handle(TestFrames.produce(4, "t", "four"));
        long logEnd = commitLog.maxOffset();
        awaitIndex();

        gate.admissible = false;
        Frame refused = (Frame) h.handle(TestFrames.fetch(5, "t", thirdStart, 10));
        MqProto.FetchResponse refusedFetch = TestFrames.parseFetch(refused);
        assertEquals(MqProto.ErrorCode.ISR_TOO_SMALL, refusedFetch.getErrorCode());
        assertEquals(thirdStart, refusedFetch.getNextOffset(),
                "an unavailable read must preserve the consumer cursor");
        assertEquals(0, refusedFetch.getRecordsCount());

        gate.admissible = true;
        gate.hw = thirdStart;
        Frame partial = (Frame) h.handle(TestFrames.fetch(6, "t", thirdStart, 10));
        MqProto.FetchResponse partialFetch = TestFrames.parseFetch(partial);
        assertEquals(0, partialFetch.getRecordsCount(),
                "recovered ISR does not bypass the HW gate");
        assertEquals(thirdStart, partialFetch.getNextOffset());

        gate.hw = logEnd;
        Frame visible = (Frame) h.handle(TestFrames.fetch(7, "t", thirdStart, 10));
        MqProto.FetchResponse visibleFetch = TestFrames.parseFetch(visible);
        assertEquals(2, visibleFetch.getRecordsCount());
        assertEquals(logEnd, visibleFetch.getNextOffset());
    }

    // ------------------------------------------------------------------
    // the three ack tiers
    // ------------------------------------------------------------------

    @Test
    void acksZeroNeverWaitsEvenWithTheWatermarkFarBehind() throws Exception {
        FakeGate gate = new FakeGate();
        gate.hw = 0; // replication hopelessly behind — irrelevant for acks=0
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.produce(1, "t", MqProto.AckMode.ACKS_0, "x"));

        assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(resp).getStatus());
        assertEquals(0, gate.awaitCalls, "acks=0 must not park on the HW");
        assertEquals(1, gate.localAppends, "the LEO still moves (it feeds the HW minimum)");
    }

    @Test
    void acksOneKeepsM1SemanticsNoHwPark() throws Exception {
        FakeGate gate = new FakeGate();
        gate.hw = 0;
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.produce(1, "t", "x"));

        assertEquals(MqProto.ProduceStatus.OK, TestFrames.parseAck(resp).getStatus());
        assertEquals(0, gate.awaitCalls, "acks=1 promises local durability only");
    }

    @Test
    void acksAllIsRefusedWhenIsrBelowMinIsr() throws Exception {
        FakeGate gate = new FakeGate();
        gate.admissible = false;
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.produce(1, "t", MqProto.AckMode.ACKS_ALL, "x"));

        MqProto.ProduceAck ack = TestFrames.parseAck(resp);
        assertEquals(MqProto.ProduceStatus.ERROR, ack.getStatus());
        assertEquals(MqProto.ErrorCode.ISR_TOO_SMALL, ack.getErrorCode());
        assertEquals(0, commitLog.readFrom(0, 10).size(),
                "refused before appending: no silent degradation");
        assertEquals(0, gate.awaitCalls);
    }

    @Test
    void acksAllOkWhenTheWatermarkPassesTheBatch() throws Exception {
        FakeGate gate = new FakeGate();
        gate.replicated = true;
        BrokerRequestHandler h = handler(gate, 5_000L);

        Frame resp = (Frame) h.handle(TestFrames.produce(1, "t", MqProto.AckMode.ACKS_ALL, "x"));

        MqProto.ProduceAck ack = TestFrames.parseAck(resp);
        assertEquals(MqProto.ProduceStatus.OK, ack.getStatus());
        assertEquals(1, gate.awaitCalls, "acks=all parks exactly once on the HW");
        assertTrue(ack.getOffsets(0) >= 0);
    }

    @Test
    void acksAllTimeoutIsAnErrorNeverALyingOk() throws Exception {
        FakeGate gate = new FakeGate();
        gate.replicated = false;
        BrokerRequestHandler h = handler(gate, 50L);

        Frame resp = (Frame) h.handle(TestFrames.produce(1, "t", MqProto.AckMode.ACKS_ALL, "x"));

        MqProto.ProduceAck ack = TestFrames.parseAck(resp);
        assertEquals(MqProto.ProduceStatus.ERROR, ack.getStatus());
        assertTrue(ack.getError().contains("replication await timeout"));
        assertEquals(1, commitLog.readFrom(0, 10).size(),
                "the record IS appended — at-least-once until the M4 dedup window");
    }

    // ------------------------------------------------------------------
    // dispatcher: cluster frames split off before the business pool
    // ------------------------------------------------------------------

    /** Records tasks instead of running them: "did the pool get this frame?" */
    private static final class RecordingExecutor implements Executor {
        final List<Runnable> tasks = new ArrayList<Runnable>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }
    }

    private static final class RecordingInbound implements ReplicationInbound {
        final List<Frame> frames = new ArrayList<Frame>();
        final List<Channel> channels = new ArrayList<Channel>();

        @Override
        public void onFrame(Channel channel, Frame frame) {
            frames.add(frame);
            channels.add(channel);
        }
    }

    @Test
    void clusterFramesRouteToReplicationWithoutTouchingTheBusinessPool() {
        RecordingExecutor pool = new RecordingExecutor();
        RecordingInbound inbound = new RecordingInbound();
        EmbeddedChannel ch = new EmbeddedChannel(new DispatcherHandler(pool,
                handler(new FakeGate(), 5_000L), inbound));

        ch.writeInbound(new Frame(MessageType.VOTE, 1, new byte[0]));
        ch.writeInbound(new Frame(MessageType.VOTE_RESP, 2, new byte[0]));
        ch.writeInbound(new Frame(MessageType.HEARTBEAT, 3, new byte[0]));
        ch.writeInbound(new Frame(MessageType.HEARTBEAT_RESP, 4, new byte[0]));
        ch.writeInbound(new Frame(MessageType.FOLLOWER_FETCH, 5, new byte[0]));

        assertEquals(5, inbound.frames.size(), "every cluster frame reaches replication");
        assertEquals(MessageType.FOLLOWER_FETCH, inbound.frames.get(4).type());
        assertEquals(ch, inbound.channels.get(4), "the reply channel travels with the frame");
        assertEquals(0, pool.tasks.size(), "replication traffic must never hit the business pool");
        assertNull(ch.readOutbound(), "nothing is answered on the EventLoop path");
        ch.close();
    }

    @Test
    void produceStillGoesThroughTheBusinessPool() {
        RecordingExecutor pool = new RecordingExecutor();
        EmbeddedChannel ch = new EmbeddedChannel(new DispatcherHandler(pool,
                handler(new FakeGate(), 5_000L), new RecordingInbound()));

        ch.writeInbound(TestFrames.produce(1, "t", "x"));

        assertEquals(1, pool.tasks.size(), "the data plane keeps its M1 shape");
        ch.close();
    }

    @Test
    void standaloneBrokerDropsClusterFramesSilently() {
        RecordingExecutor pool = new RecordingExecutor();
        // null replication = the standalone wiring (DispatcherHandler's
        // two-arg constructor does exactly this)
        EmbeddedChannel ch = new EmbeddedChannel(new DispatcherHandler(pool,
                handler(new FakeGate(), 5_000L), null));

        ch.writeInbound(new Frame(MessageType.FOLLOWER_FETCH, 5, new byte[0]));
        ch.writeInbound(new Frame(MessageType.VOTE, 6, new byte[0]));

        assertEquals(0, pool.tasks.size());
        assertNull(ch.readOutbound());
        assertTrue(ch.isOpen(), "an unknown cluster frame must not kill the connection");
        ch.close();
    }
}
