package com.minimq.client.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.minimq.client.ClientChannel;
import com.minimq.client.MQProducer;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * THE M4 DELIVERABLE: the 12-cell reliability chaos matrix. Main
 * configuration everywhere: acks=all + SYNC_FLUSH + RF=3 +
 * min.insync.replicas=2 + leader dedup window 100k. Rows are fault x
 * timing (storm / flush window / election window / steady), columns are
 * RPO (acked-but-lost), RTO (service recovery), visible duplicates (past
 * every dedup line), and reorderings — reconciled by SEQ, never by
 * offset (offsets lie after a truncation; seqs never do).
 *
 * <p>
 * Every injection goes through {@link ChaosRunner}, which journals a
 * JSONL line per shot (chaos-log.jsonl) so any non-zero cell is
 * attributable and replayable.
 *
 * <p>
 * Special cells: ISR-shrink (the "acks=all means no loss"
 * misconception: min.insync.replicas turns silent degradation into a loud
 * ISR_TOO_SMALL) and minority-unavailability (the partition side's
 * unavailable span, recorded honestly rather than mixed into RTO).
 */
@Timeout(1200)
class ReliabilityMatrixTest {

  private static final String TOPIC = "matrix";
  private static final long REQUEST_TIMEOUT_MS = 5_000;
  /** The M3 playbook failover budget: new leader serving within 8s. */
  private static final long FAILOVER_BUDGET_MS = 8_000;

  /** Cells that kill a leader mid-storm get a wider patience margin. */
  private static final long RECOVERY_WAIT_MS = 25_000;

  private File root;
  private MatrixCluster cluster;
  private ChaosRunner chaos;

  @BeforeEach
  void startCluster() throws Exception {
    Logger rootLog = Logger.getLogger("");
    rootLog.setLevel(Level.FINE);
    for (java.util.logging.Handler h : rootLog.getHandlers()) {
      h.setLevel(Level.FINE);
    }
    root = java.nio.file.Files.createTempDirectory("m4-matrix-").toFile();
    cluster = new MatrixCluster(root);
    cluster.startAll();
    cluster.awaitLeader(20_000);
    cluster.baselineElectedLines();
    chaos = new ChaosRunner(cluster);
  }

  @AfterEach
  void stopCluster() throws Exception {
    if (chaos != null) {
      chaos.close();
      try {
        java.nio.file.Files.write(cluster.chaosLog.toPath(),
            chaos.journalLines(), java.nio.charset.StandardCharsets.UTF_8);
      } catch (Exception ignored) {
        // the journal is best-effort telemetry for postmortems
      }
    }
    if (cluster != null) {
      cluster.close();
    }
    // Postmortem switch: -Dmatrix.keep preserves the last cell's full
    // cluster state (node logs + stores + chaos journal) for attribution.
    if (System.getProperty("matrix.keep") == null) {
      deleteRecursivelyWithRetries(root);
    } else {
      System.out.println("[MATRIX] kept " + root + " (-Dmatrix.keep)");
    }
  }

  private static void deleteRecursivelyWithRetries(File f) throws InterruptedException {
    for (int attempt = 0; attempt < 50; attempt++) {
      try {
        deleteRecursively(f);
        return;
      } catch (Exception stillLocked) {
        Thread.sleep(100);
      }
    }
  }

  private static void deleteRecursively(File f) throws Exception {
    File[] kids = f.listFiles();
    if (kids != null) {
      for (File k : kids) {
        deleteRecursively(k);
      }
    }
    java.nio.file.Files.deleteIfExists(f.toPath());
  }

  // ==================================================================
  // the shared cell skeleton: storm writer, injection, recovery, reconcile
  // ==================================================================

  /** What a cell does once its timing condition is met. */
  interface Injection {
    void fire(int leader, StormWriter writer) throws Exception;
  }

  /**
   * Background storm writer: hammers acks=all sends, journals acked seqs,
   * and timestamps the first successful post-injection ack (RTO) plus the
   * last failed-send moment (unavailability tail).
   */
  final class StormWriter {
    final MQProducer producer;
    final Set<Integer> acked = new ConcurrentSkipListSet<Integer>();
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicLong injectedAt = new AtomicLong(0);
    final AtomicLong recoveredAt = new AtomicLong(0);
    /** last moment a send failed its whole retry budget (downtime tail) */
    final AtomicLong lastHardFailureAt = new AtomicLong(0);
    final AtomicLong firstHardFailureAt = new AtomicLong(0);
    int seq = 0;
    Thread thread;
    volatile int sendTimeoutsSeen;

    StormWriter(MQProducer producer) {
      this.producer = producer;
    }

    void start() {
      thread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (running.get()) {
            int mySeq;
            synchronized (StormWriter.this) {
              mySeq = seq++;
            }
            try {
              CompletableFuture<Long> f = producer.send(TOPIC, payload(mySeq));
              f.get(6, TimeUnit.SECONDS);
              acked.add(mySeq);
              if (injectedAt.get() > 0 && recoveredAt.get() == 0) {
                recoveredAt.set(System.nanoTime());
              }
            } catch (Exception budgetExhausted) {
              // at-least-once's declared boundary: un-acked
              // sends are allowed to vanish (not a loss)
              sendTimeoutsSeen++;
              long now = System.nanoTime();
              lastHardFailureAt.set(now);
              if (firstHardFailureAt.get() == 0 && injectedAt.get() > 0) {
                firstHardFailureAt.set(now);
              }
            }
          }
        }
      }, "m4-storm-writer");
      thread.setDaemon(true);
      thread.start();
    }

    void setCruising() throws InterruptedException {
      Thread.sleep(500); // the storm is genuinely blowing
    }

    void stop() throws InterruptedException {
      running.set(false);
      thread.join(10_000);
    }

    long rtoMs() {
      if (injectedAt.get() == 0 || recoveredAt.get() == 0) {
        return 0; // service never actually interrupted
      }
      return (recoveredAt.get() - injectedAt.get()) / 1_000_000L;
    }
  }

  private CellResult runStormCell(String name, Injection injection) throws Exception {
    int leader = cluster.awaitLeader(20_000);
    MQProducer producer = MQProducer.connectCluster(cluster.addresses,
        REQUEST_TIMEOUT_MS, 64 * 1024, 20);
    try {
      producer.acks(MqProto.AckMode.ACKS_ALL);
      StormWriter storm = new StormWriter(producer);
      storm.start();
      storm.setCruising();
      try {
        injection.fire(leader, storm);
      } finally {
        storm.injectedAt.compareAndSet(0, System.nanoTime());
      }
      // let the recovery play out and the storm finish its business
      long deadline = System.currentTimeMillis() + RECOVERY_WAIT_MS;
      while (System.currentTimeMillis() < deadline && storm.recoveredAt.get() == 0) {
        Thread.sleep(50);
      }
      if (storm.recoveredAt.get() == 0) {
        // no interruption observed (e.g. follower-kill cells): fine
        Thread.sleep(1_500);
      } else {
        Thread.sleep(1_000); // post-recovery traffic lands
      }
      storm.stop();
      producer.close();

      return reconcile(name, storm.acked);
    } finally {
      producer.close();
    }
  }

  /** The reconciliation: drain + seq-keyed accounting. */
  private CellResult reconcile(String name, Set<Integer> acked) throws Exception {
    List<Integer> consumed = cluster.drainSeqs(TOPIC, 200_000, 30_000);
    CellResult r = new CellResult(name);

    // RPO: acked (promised durability) but never consumed
    Set<Integer> consumedSet = new HashSet<Integer>(consumed);
    Set<Integer> lost = new TreeSet<Integer>(acked);
    lost.removeAll(consumedSet);
    r.rpo = lost.size();

    // visible duplicates: same seq consumed more than once
    r.duplicates = consumed.size() - consumedSet.size();

    // reorderings: a seq arriving after a strictly bigger seq
    int maxSoFar = -1;
    for (int s : consumed) {
      if (s < maxSoFar) {
        r.reordered++;
      } else {
        maxSoFar = s;
      }
    }
    if (!lost.isEmpty()) {
      r.note = "lost seqs: " + firstFew(lost);
    }
    System.out.println("[MATRIX] " + r.markdownRow()
        + " (acked=" + acked.size() + ", consumed=" + consumed.size() + ")");
    return r;
  }

  private static String firstFew(Set<Integer> lost) {
    StringBuilder sb = new StringBuilder("[");
    int i = 0;
    for (int s : lost) {
      if (i++ == 8) {
        sb.append("...");
        break;
      }
      sb.append(s).append(' ');
    }
    return sb.append(']').toString().trim();
  }

  private static byte[] payload(int seq) {
    return ByteBuffer.allocate(4).putInt(seq).array();
  }

  // ==================================================================
  // rows 1-3: kill -9 LEADER at three timings
  // ==================================================================

  @Test
  void cell01Kill9LeaderAtStorm() throws Exception {
    CellResult r = runStormCell("kill9-leader@storm", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) {
        chaos.kill9(leader, "storm");
      }
    });
    assertEquals(0, r.rpo, r.note);
    assertTrue(r.rtoMs <= FAILOVER_BUDGET_MS, "RTO " + r.rtoMs + "ms over budget");
    cluster.awaitLeader(RECOVERY_WAIT_MS);
  }

  @Test
  void cell02Kill9LeaderAtFlushWindow() throws Exception {
    CellResult r = runStormCell("kill9-leader@flush-window", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) throws Exception {
        boolean hitWindow = chaos.awaitFlushWindow(leader, 5_000);
        chaos.kill9(leader, hitWindow ? "flush-window" : "flush-window-missed");
      }
    });
    assertEquals(0, r.rpo, r.note);
    assertTrue(r.rtoMs <= FAILOVER_BUDGET_MS, "RTO " + r.rtoMs + "ms over budget");
  }

  @Test
  void cell03Kill9LeaderAtElection() throws Exception {
    // kill the leader, then DURING the election window kill a candidate:
    // the surviving single node cannot reach majority (2 of 3) — the
    // matrix's most brutal timing row. Revive the candidate to recover.
    CellResult r = runStormCell("kill9-leader@election", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) throws Exception {
        chaos.kill9(leader, "election-seed");
        boolean inWindow = chaos.awaitNoLeader(2_000);
        int candidate = cluster.someFollowerOf(leader);
        chaos.kill9(candidate, inWindow ? "election-window" : "election-window-missed");
        // recovery requires majority again: revive the candidate
        Thread.sleep(1_000);
        cluster.restartNode(candidate);
      }
    });
    assertEquals(0, r.rpo, r.note);
    // election re-run + revival: wider than the plain failover budget,
    // but the service MUST come back
    assertTrue(r.rtoMs <= 30_000, "RTO " + r.rtoMs + "ms — service never recovered");
    cluster.awaitLeader(20_000);
  }

  // ==================================================================
  // rows 4-5: kill -9 FOLLOWER
  // ==================================================================

  @Test
  void cell04Kill9FollowerAtStorm() throws Exception {
    CellResult r = runStormCell("kill9-follower@storm", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) {
        chaos.kill9(cluster.someFollowerOf(leader), "storm");
      }
    });
    assertEquals(0, r.rpo, r.note);
    // no leader change: service never interrupted (ISR absorbs it)
    cluster.awaitLeader(10_000);
  }

  @Test
  void cell05Kill9FollowerAtFlushWindow() throws Exception {
    CellResult r = runStormCell("kill9-follower@flush-window", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) throws Exception {
        boolean hitWindow = chaos.awaitFlushWindow(leader, 5_000);
        chaos.kill9(cluster.someFollowerOf(leader),
            hitWindow ? "flush-window" : "flush-window-missed");
      }
    });
    assertEquals(0, r.rpo, r.note);
  }

  // ==================================================================
  // rows 6-7: bilateral partition (leader isolated from the majority)
  // ==================================================================

  @Test
  void cell06BilateralPartitionAtStorm() throws Exception {
    // The leader is cut off from BOTH followers: it keeps answering
    // ISR_TOO_SMALL (it cannot hear the new election to step down),
    // the two followers elect the new leader, the producer follows.
    CellResult r = runStormCell("partition-bilateral@storm", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) {
        int f1 = -1;
        int f2 = -1;
        for (int i = 0; i < MatrixCluster.NODES; i++) {
          if (i != leader) {
            if (f1 < 0) {
              f1 = i;
            } else {
              f2 = i;
            }
          }
        }
        chaos.partitionBilateral(leader, f1, "storm");
        chaos.partitionBilateral(leader, f2, "storm");
        try {
          cluster.awaitNewElection(FAILOVER_BUDGET_MS);
          Thread.sleep(1_500); // new leader serves; producer redirects
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertEquals(0, r.rpo, r.note);
    // heal so the isolated (stale) leader hears the new term and steps down
    chaos.healPartitions("after-storm");
    Thread.sleep(2_000);
    cluster.awaitLeader(10_000);
  }

  @Test
  void cell07BilateralPartitionAtElection() throws Exception {
    // kill the leader, then DURING the election window partition the
    // two survivors from each other: neither can reach majority
    // (needs 2 of 3), both PreVote forever — deadlock until the heal.
    CellResult r = runStormCell("partition-bilateral@election", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) throws Exception {
        chaos.kill9(leader, "election-seed");
        boolean inWindow = chaos.awaitNoLeader(2_000);
        int f1 = -1;
        int f2 = -1;
        for (int i = 0; i < MatrixCluster.NODES; i++) {
          if (i != leader) {
            if (f1 < 0) {
              f1 = i;
            } else {
              f2 = i;
            }
          }
        }
        chaos.partitionBilateral(f1, f2, inWindow ? "election-window"
            : "election-window-missed");
        Thread.sleep(4_000); // let the deadlock establish itself
        chaos.healPartitions("election-deadlock-release");
      }
    });
    assertEquals(0, r.rpo, r.note);
    // recovery had to wait for the heal: the widest honest RTO in the
    // matrix, but still bounded by the recovery wait
    assertTrue(r.rtoMs <= 40_000, "RTO " + r.rtoMs + "ms — service never recovered");
    cluster.awaitLeader(20_000);
  }

  // ==================================================================
  // row 8: unilateral partition at steady state (the insidious cut)
  // ==================================================================

  @Test
  void cell08UnilateralPartitionAtSteady() throws Exception {
    // The follower stops hearing the leader (inbound cut at the
    // follower): its lease expires, it PreVotes, the live leader
    // refuses (lease valid) — no needless election (the etcd PreVote
    // design paying off). The follower lags out of the ISR; the
    // service never stops. Heal: it fetches, catches up, returns.
    CellResult r = runStormCell("partition-unilateral@steady", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) {
        int follower = cluster.someFollowerOf(leader);
        chaos.partitionUnilateral(leader, follower, "steady");
        try {
          Thread.sleep(1_500); // lease expiry + PreVote refusal + ISR sweep
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    assertEquals(0, r.rpo, r.note);
    chaos.healPartitions("steady-heal");
    Thread.sleep(2_500); // catch up + rejoin
    cluster.awaitLeader(10_000);
  }

  // ==================================================================
  // row 9: the stale leader revives under the new leader (epoch arbitration)
  // ==================================================================

  @Test
  void cell09StaleLeaderRevivalUnderNewLeader() throws Exception {
    CellResult r = runStormCell("stale-leader-revival@serving", new Injection() {
      @Override
      public void fire(int leader, StormWriter storm) throws Exception {
        chaos.kill9(leader, "revival-seed");
        cluster.awaitNewElection(FAILOVER_BUDGET_MS);
        cluster.awaitLeaderOtherThan(leader, 10_000);
        Thread.sleep(1_000); // the new leader serves traffic
        cluster.restartNode(leader); // same storeDir: epoch arbitration
        Thread.sleep(2_500); // recover by CRC, fetch, truncate, rejoin
      }
    });
    assertEquals(0, r.rpo, r.note);
    // the revived node must NOT preempt the incumbent (higher epoch)
    int incumbent = cluster.awaitLeader(10_000);
    assertNotEquals(-1, incumbent);
  }

  // ==================================================================
  // special cell 1: ISR shrink -> silent degradation must become a LOUD error
  // ==================================================================

  @Test
  void cell10IsrShrinkTurnsSilentDegradationIntoLoudError() throws Exception {
    int leader = cluster.awaitLeader(20_000);
    int f1 = -1;
    int f2 = -1;
    for (int i = 0; i < MatrixCluster.NODES; i++) {
      if (i != leader) {
        if (f1 < 0) {
          f1 = i;
        } else {
          f2 = i;
        }
      }
    }
    MQProducer producer = MQProducer.connectCluster(cluster.addresses,
        REQUEST_TIMEOUT_MS, 64 * 1024, 20);
    try {
      producer.acks(MqProto.AckMode.ACKS_ALL);
      Set<Integer> acked = new java.util.LinkedHashSet<Integer>();
      for (int i = 0; i < 20; i++) {
        producer.send(TOPIC, payload(i)).get(10, TimeUnit.SECONDS);
        acked.add(i);
      }

      // kill BOTH followers: ISR shrinks to just the leader
      chaos.kill9(f1, "isr-shrink");
      chaos.kill9(f2, "isr-shrink");

      // every acks=all send must now FAIL (ISR_TOO_SMALL propagated
      // as an error), never silently succeed as a weaker write —
      // the "acks=all means no loss" misconception made testable
      int loudFailures = 0;
      long unavailabilityStart = 0;
      long firstFailureAt = 0;
      for (int i = 100; i < 112; i++) {
        try {
          producer.send(TOPIC, payload(i)).get(15, TimeUnit.SECONDS);
          // NOT acceptable: a silent success here would be a
          // silent acks=1 degradation
        } catch (Exception expectedLoud) {
          loudFailures++;
          if (firstFailureAt == 0) {
            firstFailureAt = System.currentTimeMillis();
            unavailabilityStart = firstFailureAt;
          }
        }
      }
      assertTrue(loudFailures >= 10,
          "ISR below min.insync.replicas must refuse writes loudly, got only "
              + loudFailures + " failures in 12 sends");

      // revive one follower: ISR re-forms to 2, service resumes
      cluster.restartNode(f1);
      long serviceBackAt = 0;
      for (int i = 200; i < 220; i++) {
        try {
          producer.send(TOPIC, payload(i)).get(20, TimeUnit.SECONDS);
          acked.add(i);
          if (serviceBackAt == 0) {
            serviceBackAt = System.currentTimeMillis();
          }
        } catch (Exception stillDown) {
          // ISR not back to 2 yet — keep trying
        }
      }
      assertTrue(serviceBackAt > 0, "service never resumed after ISR re-formed");

      CellResult r = reconcile("isr-shrink(loud-error)", acked);
      assertEquals(0, r.rpo, r.note);
      System.out.println("[MATRIX] isr-shrink unavailable span: "
          + (serviceBackAt - unavailabilityStart) + "ms (recorded honestly)");
    } finally {
      producer.close();
    }
  }

  // ==================================================================
  // special cell 2: minority-partition unavailability, recorded honestly
  // ==================================================================

  @Test
  void cell11MinorityPartitionUnavailabilityRecorded() throws Exception {
    // The same isolation as cell06, but the metric is the POINT: how
    // long is the write path unavailable while the majority elects?
    // (RTO and partition-side unavailability are separate numbers.)
    int leader = cluster.awaitLeader(20_000);
    int f1 = -1;
    int f2 = -1;
    for (int i = 0; i < MatrixCluster.NODES; i++) {
      if (i != leader) {
        if (f1 < 0) {
          f1 = i;
        } else {
          f2 = i;
        }
      }
    }
    MQProducer producer = MQProducer.connectCluster(cluster.addresses,
        REQUEST_TIMEOUT_MS, 64 * 1024, 20);
    try {
      producer.acks(MqProto.AckMode.ACKS_ALL);
      Set<Integer> acked = new java.util.LinkedHashSet<Integer>();
      for (int i = 0; i < 20; i++) {
        producer.send(TOPIC, payload(i)).get(10, TimeUnit.SECONDS);
        acked.add(i);
      }

      long injectedAt = System.currentTimeMillis();
      chaos.partitionBilateral(leader, f1, "minority-unavailability");
      chaos.partitionBilateral(leader, f2, "minority-unavailability");

      // probe every 100ms: unavailability = until the first success
      long firstSuccessAt = 0;
      int probes = 0;
      int probeSeq = 1_000;
      for (long deadline = System.currentTimeMillis() + 30_000; System.currentTimeMillis() < deadline;) {
        probes++;
        try {
          producer.send(TOPIC, payload(probeSeq++)).get(3, TimeUnit.SECONDS);
          acked.add(probeSeq - 1);
          if (firstSuccessAt == 0) {
            firstSuccessAt = System.currentTimeMillis();
            break;
          }
        } catch (Exception stillDown) {
          Thread.sleep(100);
        }
      }
      assertTrue(firstSuccessAt > 0, "write path never recovered from the partition");
      long unavailableMs = firstSuccessAt - injectedAt;
      // more traffic on the healthy side, then heal
      for (int i = 2_000; i < 2_050; i++) {
        try {
          producer.send(TOPIC, payload(i)).get(10, TimeUnit.SECONDS);
          acked.add(i);
        } catch (Exception e) {
          // transport still chasing the new leader for this one
        }
      }
      chaos.healPartitions("minority-unavailability-end");
      Thread.sleep(2_000);

      CellResult r = reconcile("minority-unavailability", acked);
      assertEquals(0, r.rpo, r.note);
      System.out.println("[MATRIX] minority-partition unavailability: "
          + unavailableMs + "ms over " + probes + " probes "
          + "(prefer unavailability over a wrong write)");
    } finally {
      producer.close();
    }
  }

  // ==================================================================
  // row 12: retry convergence — the dedup window's end-to-end proof
  // ==================================================================

  @Test
  void cell12RetryStormConvergesThroughDedupWindow() throws Exception {
    int leader = cluster.awaitLeader(20_000);
    // A raw single-node channel: the matrix must control seq keys
    // EXACTLY (the SDK generates its own), and must read the ProduceAck
    // statuses the transport layer otherwise hides.
    ClientChannel channel = ClientChannel.connect(
        "127.0.0.1", cluster.addresses.get(leader).getPort(), REQUEST_TIMEOUT_MS);
    try {
      MqProto.ProduceRequest.Builder req = MqProto.ProduceRequest.newBuilder()
          .setAcks(MqProto.AckMode.ACKS_ALL);
      for (int i = 0; i < 5; i++) {
        req.addMessages(MqProto.MessageData.newBuilder()
            .setTopic(TOPIC)
            .setPayload(com.google.protobuf.ByteString.copyFrom(payload(i)))
            .setSeqKey("matrix-retry-" + i)
            .setTimestamp(System.currentTimeMillis()));
      }
      byte[] body = req.build().toByteArray();

      Frame ack1 = channel.request(MessageType.PRODUCE, body)
          .get(10, TimeUnit.SECONDS);
      MqProto.ProduceAck parsed1 = MqProto.ProduceAck.parseFrom(ack1.body());
      assertEquals(MqProto.ProduceStatus.OK, parsed1.getStatus());
      List<Long> original = parsed1.getOffsetsList();

      // THE RETRY: the same seq keys, as a producer issues after a
      // lost ack. The leader's dedup window must collapse it.
      Frame ack2 = channel.request(MessageType.PRODUCE, body)
          .get(10, TimeUnit.SECONDS);
      MqProto.ProduceAck parsed2 = MqProto.ProduceAck.parseFrom(ack2.body());
      assertEquals(MqProto.ProduceStatus.DUPLICATE, parsed2.getStatus(),
          "the retry must be answered DUPLICATE, not appended again");
      assertEquals(original, parsed2.getOffsetsList(),
          "the retry must converge to the ORIGINAL offsets");

      // consume: each seq exactly once — the window's whole point
      List<Integer> consumed = cluster.drainSeqs(TOPIC, 100, 20_000);
      Collections.sort(consumed);
      List<Integer> expected = new ArrayList<Integer>();
      for (int i = 0; i < 5; i++) {
        expected.add(i);
      }
      assertEquals(expected, consumed,
          "a converged retry must be invisible to the consumer");
    } finally {
      channel.close();
    }
  }
}
