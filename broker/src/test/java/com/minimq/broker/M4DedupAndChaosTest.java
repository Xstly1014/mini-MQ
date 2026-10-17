package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import com.minimq.storage.Message;

/**
 * M4 unit coverage: the dedup window (component + produce integration),
 * the partition shield, and the chaos control server (command semantics
 * + loopback wire). The reliability matrix drives all three through the
 * real process boundary; these tests pin the pieces it stands on.
 */
class M4DedupAndChaosTest {

  @TempDir
  Path tempDir;

  private static final int SEGMENT_SIZE = 256 * 1024;

  private CommitLog commitLog;
  private ConsumeQueueStore consumeQueues;

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

  private BrokerRequestHandler handlerWithDedup(int entries) {
    return new BrokerRequestHandler(commitLog, consumeQueues, FlushMode.OS_FLUSH,
        5_000L, false, ReplicationGate.standalone(commitLog), 5_000L,
        new DedupWindow(entries));
  }

  // ------------------------------------------------------------------
  // DedupWindow component
  // ------------------------------------------------------------------

  @Test
  void dedupWindowEvictsLeastRecentlyUsedEntry() {
    DedupWindow w = new DedupWindow(2);
    w.record(1L, 10L);
    w.record(2L, 20L);
    w.seen(1L); // refresh key 1: key 2 becomes the eviction victim
    w.record(3L, 30L); // over capacity -> evicts key 2, not key 1

    assertEquals(Long.valueOf(10L), w.seen(1L));
    assertNull(w.seen(2L), "LRU order must decide the eviction");
    assertEquals(Long.valueOf(30L), w.seen(3L));
    assertEquals(2, w.size());
  }

  @Test
  void dedupWindowCountsHitsAndChecks() {
    DedupWindow w = new DedupWindow(8);
    w.record(7L, 70L);
    w.seen(7L);
    w.seen(7L);
    w.seen(8L); // miss

    assertEquals(3, w.checks());
    assertEquals(2, w.hits());
  }

  // ------------------------------------------------------------------
  // produce-path integration
  // ------------------------------------------------------------------

  @Test
  void retriedSeqKeyGetsOriginalOffsetAndNoSecondAppend() throws Exception {
    BrokerRequestHandler handler = handlerWithDedup(1024);
    Frame first = (Frame) handler.handle(TestFrames.produce(1, "orders", "a", "b"));
    MqProto.ProduceAck ack1 = TestFrames.parseAck(first);
    assertEquals(MqProto.ProduceStatus.OK, ack1.getStatus());
    long leoAfterFirst = commitLog.maxOffset();

    // The exact retry a producer issues after a lost ack: same seq keys.
    Frame second = (Frame) handler.handle(TestFrames.produce(2, "orders", "a", "b"));
    MqProto.ProduceAck ack2 = TestFrames.parseAck(second);

    assertEquals(MqProto.ProduceStatus.DUPLICATE, ack2.getStatus());
    assertEquals(ack1.getOffsetsList(), ack2.getOffsetsList(),
        "the retry must converge to the ORIGINAL offsets");
    assertEquals(leoAfterFirst, commitLog.maxOffset(),
        "a converged retry must not append anything");
  }

  @Test
  void mixedBatchAppendsOnlyTheUnseenMessages() throws Exception {
    BrokerRequestHandler handler = handlerWithDedup(1024);
    Frame first = (Frame) handler.handle(TestFrames.produce(1, "orders", "a", "b"));
    MqProto.ProduceAck ack1 = TestFrames.parseAck(first);
    long leoBefore = commitLog.maxOffset();

    // One batch, one old key + two new keys: OK, not DUPLICATE.
    Frame second = (Frame) handler.handle(TestFrames.produce(2, "orders", "a", "c", "d"));
    MqProto.ProduceAck ack2 = TestFrames.parseAck(second);

    assertEquals(MqProto.ProduceStatus.OK, ack2.getStatus());
    assertEquals(ack1.getOffsets(0), ack2.getOffsets(0), "the seen key keeps its offset");
    assertTrue(ack2.getOffsets(1) >= leoBefore, "the new keys append past the old LEO");
    assertTrue(ack2.getOffsets(2) > ack2.getOffsets(1), "offsets stay monotonic");
    // Exact growth: two fresh appends.
    List<Message> tail = commitLog.readFrom(leoBefore, 10);
    assertEquals(2, tail.size(), "exactly the two unseen messages hit the log");
    assertEquals("c", new String(tail.get(0).getPayload(), StandardCharsets.UTF_8));
    assertEquals("d", new String(tail.get(1).getPayload(), StandardCharsets.UTF_8));
  }

  @Test
  void windowEvictionReAdmitsTheKey() throws Exception {
    BrokerRequestHandler handler = handlerWithDedup(2);
    handler.handle(TestFrames.produce(1, "orders", "a", "b")); // fills the window
    handler.handle(TestFrames.produce(2, "orders", "c", "d")); // evicts a, b (capacity 2)

    Frame third = (Frame) handler.handle(TestFrames.produce(3, "orders", "a"));
    MqProto.ProduceAck ack = TestFrames.parseAck(third);

    assertEquals(MqProto.ProduceStatus.OK, ack.getStatus(),
        "an evicted key is unknown again: the retry appends (at-least-once boundary)");
    // a, b, c, d, a-again: five records total — the retry really appended.
    assertEquals(5, commitLog.readFrom(0, 10).size());
  }

  @Test
  void dedupDisabledKeepsM1Semantics() throws Exception {
    BrokerRequestHandler handler = new BrokerRequestHandler(commitLog, consumeQueues,
        FlushMode.OS_FLUSH, 5_000L, false);
    handler.handle(TestFrames.produce(1, "orders", "a"));
    Frame second = (Frame) handler.handle(TestFrames.produce(2, "orders", "a"));
    MqProto.ProduceAck ack = TestFrames.parseAck(second);

    assertEquals(MqProto.ProduceStatus.OK, ack.getStatus(),
        "no window configured: every produce appends (M1-M3 semantics)");
    assertEquals(2, commitLog.readFrom(0, 10).size());
  }

  // ------------------------------------------------------------------
  // PartitionShield + ChaosControlServer
  // ------------------------------------------------------------------

  @Test
  void partitionShieldBlocksAddsAndClears() {
    PartitionShield shield = new PartitionShield();
    assertFalse(shield.blocksInbound(2));
    shield.inAdd(2);
    assertTrue(shield.blocksInbound(2));
    assertFalse(shield.blocksInbound(3), "other peers are unaffected");
    shield.inRemove(2);
    assertFalse(shield.blocksInbound(2));
    shield.inAdd(1);
    shield.inAdd(2);
    assertEquals(java.util.Arrays.asList(1, 2),
        new java.util.ArrayList<Integer>(shield.inBlackSnapshot()),
        "snapshot is sorted regardless of insertion order");
    shield.inClear();
    assertTrue(shield.inBlackSnapshot().isEmpty());
  }

  @Test
  void chaosControlExecutesPartitionOps() {
    PartitionShield shield = new PartitionShield();
    Supplier<String> state = new Supplier<String>() {
      @Override
      public String get() {
        return "{\"node\":0,\"inBlack\":" + ChaosControlServer.ids(shield.inBlackSnapshot()) + "}";
      }
    };

    assertTrue(ChaosControlServer.execute("partition in-add 2", shield, state)
        .contains("\"inBlack\":[2]"));
    assertTrue(ChaosControlServer.execute("partition in-add 5", shield, state)
        .contains("\"inBlack\":[2,5]"));
    assertTrue(ChaosControlServer.execute("partition in-remove 2", shield, state)
        .contains("\"inBlack\":[5]"));
    assertEquals("{\"node\":0,\"inBlack\":[5]}", ChaosControlServer.execute("state", shield, state));
    assertTrue(ChaosControlServer.execute("partition in-clear", shield, state)
        .contains("\"inBlack\":[]"));
    assertTrue(ChaosControlServer.execute("partition nonsense", shield, state)
        .contains("\"ok\":false"));
    assertTrue(ChaosControlServer.execute("frobnicate", shield, state)
        .contains("\"ok\":false"));
  }

  @Test
  void chaosControlServesCommandsOverTheLoopbackWire() throws Exception {
    PartitionShield shield = new PartitionShield();
    ChaosControlServer server = new ChaosControlServer(0, shield,
        new Supplier<String>() {
          @Override
          public String get() {
            return "{\"node\":9,\"role\":\"LEADER\"}";
          }
        });
    server.start();
    try {
      try (Socket sock = new Socket("127.0.0.1", server.port())) {
        sock.setTcpNoDelay(true);
        OutputStream out = sock.getOutputStream();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));

        out.write("partition in-add 3\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
        assertEquals("{\"ok\":true,\"inBlack\":[3]}", in.readLine());

        out.write("state\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
        assertEquals("{\"node\":9,\"role\":\"LEADER\"}", in.readLine());

        out.write("partition in-clear\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
        assertEquals("{\"ok\":true,\"inBlack\":[]}", in.readLine());
        assertTrue(shield.inBlackSnapshot().isEmpty());
      }
    } finally {
      server.close();
    }
  }

  @Test
  void brokerServerExposesChaosPortAndState() throws Exception {
    BrokerConfig config = BrokerConfig.defaults()
        .port(0)
        .storeDir(tempDir.resolve("broker-store").toFile())
        .chaosControlPort(21000 + new java.util.Random().nextInt(2000))
        .dedupWindowEntries(64);
    BrokerServer broker = new BrokerServer(config);
    try {
      broker.start();
      int chaosPort = broker.chaosPort();
      assertTrue(chaosPort > 0, "chaos port must be bound when configured");
      assertNotNull(broker.dedupWindow());
      assertEquals(64, broker.dedupWindow().capacity());

      try (Socket sock = new Socket("127.0.0.1", chaosPort)) {
        sock.setTcpNoDelay(true);
        OutputStream out = sock.getOutputStream();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
        out.write("state\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
        String state = in.readLine();
        // standalone broker: role tag + live storage numbers + shield
        assertTrue(state.contains("\"role\":\"STANDALONE\""), state);
        assertTrue(state.contains("\"leo\":0"), state);
        assertTrue(state.contains("\"flushed\":0"), state);
        assertTrue(state.contains("\"inBlack\":[]"), state);
        assertTrue(state.contains("\"dedup\":"), state);
      }
    } finally {
      broker.shutdown();
    }
    assertEquals(-1, broker.chaosPort(), "chaos port reports closed after shutdown");
  }

  @Test
  void brokerServerWithoutChaosPortHasNoOpsSurface() throws Exception {
    BrokerConfig config = BrokerConfig.defaults()
        .port(0)
        .storeDir(tempDir.resolve("broker-store2").toFile());
    BrokerServer broker = new BrokerServer(config);
    try {
      broker.start();
      assertEquals(-1, broker.chaosPort());
      assertNull(broker.dedupWindow(), "dedup stays opt-in");
    } finally {
      broker.shutdown();
    }
  }
}
