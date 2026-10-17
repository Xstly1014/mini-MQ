package com.minimq.client.chaos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The M4 chaos injector: five injection kinds over a live 3-process cluster,
 * every shot journaled as a JSONL line so a failed matrix cell can be
 * replayed and attributed (an injection that cannot be replayed makes any
 * failure un-attributable).
 *
 * <p>
 * The five kinds:
 * <ol>
 * <li><b>kill -9</b> — {@code Process.destroyForcibly()}: TerminateProcess on
 * Windows, no shutdown hooks, no flush (the rawest crash there is).</li>
 * <li><b>graceful</b> — QUIT on stdin: the ordered shutdown path.</li>
 * <li><b>bilateral partition</b> — both sides blacklist each other at the
 * frame-routing layer (see PartitionShield): every cluster frame dies at
 * the receiver, TCP stays up — indistinguishable from a real cut.</li>
 * <li><b>unilateral partition</b> — only one direction is cut; the insidious
 * case (a node that can shout but never hear).</li>
 * <li><b>heal</b> — clear every blacklist: the partition ends, the nodes
 * see each other again.</li>
 * </ol>
 *
 * <p>
 * Timing control ("when" the fault lands) lives in the matrix cells:
 * flush-window injection awaits {@code flushed < leo} from the node's own
 * chaos state report; election-window injection awaits the no-leader
 * condition. The journal records the cluster state AT the injection, so
 * "did the injection actually hit its window" is checkable after the fact.
 */
final class ChaosRunner implements java.io.Closeable {

  private final MatrixCluster cluster;
  private final List<String> journal = new ArrayList<String>();

  ChaosRunner(MatrixCluster cluster) {
    this.cluster = cluster;
  }

  // ------------------------------------------------------------------
  // the five injections (each journals one JSONL line)
  // ------------------------------------------------------------------

  /** Kind 1: REAL kill -9 (TerminateProcess — no hooks, no flush). */
  void kill9(int node, String at) {
    String state = stateOf(node);
    cluster.killNode(node);
    journal("kill-9", node, at, state);
  }

  /** Kind 2: graceful QUIT (ordered shutdown: flush, step-down). */
  void gracefulStop(int node, String at) throws Exception {
    String state = stateOf(node);
    cluster.gracefulStop(node);
    journal("graceful", node, at, state);
  }

  /** Kind 3: bilateral — a and b drop each other's frames, both ways. */
  void partitionBilateral(int a, int b, String at) {
    command(a, "partition in-add " + b);
    command(b, "partition in-add " + a);
    journal("partition-bilateral", a, at, stateOf(a) + " peer=" + b);
  }

  /**
   * Kind 4: unilateral — {@code blockedSender}'s frames die at
   * {@code blocker}; the reverse direction keeps flowing.
   */
  void partitionUnilateral(int blockedSender, int blocker, String at) {
    command(blocker, "partition in-add " + blockedSender);
    journal("partition-unilateral", blocker, at,
        stateOf(blocker) + " blockedInboundFrom=" + blockedSender);
  }

  /** Kind 5: heal — every node's blacklist cleared, the cut ends. */
  void healPartitions(String at) {
    for (int i = 0; i < MatrixCluster.NODES; i++) {
      if (cluster.isAlive(i)) {
        command(i, "partition in-clear");
      }
    }
    journal("heal", -1, at, "");
  }

  // ------------------------------------------------------------------
  // timing gates (the "when" column of the matrix)
  // ------------------------------------------------------------------

  /**
   * Blocks until the node reports a non-empty flush backlog
   * ({@code flushed < leo}) — the SYNC_FLUSH window where a kill -9 can
   * still eat un-forced bytes. Returns false on timeout (the cell treats
   * that as "window missed", not as failure).
   */
  boolean awaitFlushWindow(int node, long timeoutMs) throws InterruptedException {
    return awaitState(node, timeoutMs, new java.util.function.Predicate<String>() {
      @Override
      public boolean test(String state) {
        long leo = longField(state, "leo");
        long flushed = longField(state, "flushed");
        return leo > flushed;
      }
    });
  }

  /**
   * Blocks until NO node reports itself leader — the election window
   * (between a leader's death and the new leader's first heartbeat).
   */
  boolean awaitNoLeader(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      boolean anyLeader = false;
      for (int i = 0; i < MatrixCluster.NODES; i++) {
        if (cluster.isAlive(i) && boolField(stateOf(i), "leader")) {
          anyLeader = true;
          break;
        }
      }
      if (!anyLeader) {
        return true;
      }
      Thread.sleep(25);
    }
    return false;
  }

  // ------------------------------------------------------------------
  // the chaos control line protocol (loopback, see ChaosControlServer)
  // ------------------------------------------------------------------

  /** One command -> one JSON line, fresh connection per command. */
  private String command(int node, String line) {
    try {
      Socket sock = new Socket();
      sock.connect(new InetSocketAddress("127.0.0.1", cluster.chaosPort(node)), 2_000);
      try {
        sock.setTcpNoDelay(true);
        OutputStream out = sock.getOutputStream();
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
        return in.readLine();
      } finally {
        sock.close();
      }
    } catch (IOException e) {
      return "{\"ok\":false,\"error\":\"" + e.getClass().getSimpleName() + "\"}";
    }
  }

  /** The node's state JSON (empty-ish on connection failure). */
  String stateOf(int node) {
    String resp = command(node, "state");
    return resp == null ? "{}" : resp;
  }

  private boolean awaitState(int node, long timeoutMs,
      java.util.function.Predicate<String> condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (condition.test(stateOf(node))) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
  }

  // ------------------------------------------------------------------
  // the journal (chaos-log)
  // ------------------------------------------------------------------

  private synchronized void journal(String type, int target, String at, String state) {
    // one JSONL line: the replayable record of WHO was injected with
    // WHAT, WHEN, and with what cluster state AT that moment.
    String line = "{\"ts\":" + System.currentTimeMillis()
        + ",\"type\":\"" + type + "\""
        + ",\"target\":" + target
        + ",\"at\":\"" + at + "\""
        + ",\"state\":\"" + state.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
    journal.add(line);
  }

  /** The accumulated JSONL journal (the matrix writes it to chaos-log.jsonl). */
  synchronized List<String> journalLines() {
    return new ArrayList<String>(journal);
  }

  @Override
  public void close() {
    // nothing persistent here; the cluster owns the processes
  }

  // ------------------------------------------------------------------
  // minimal JSON field readers (hand-rolled: no JSON dependency in the
  // client SDK, and the chaos state is a fixed tiny shape)
  // ------------------------------------------------------------------

  static long longField(String json, String field) {
    return parseLong(json, "\"" + field + "\":");
  }

  static boolean boolField(String json, String field) {
    int i = json.indexOf("\"" + field + "\":");
    if (i < 0) {
      return false;
    }
    return json.regionMatches(i + field.length() + 3, "true", 0, 4);
  }

  private static long parseLong(String json, String key) {
    int i = json.indexOf(key);
    if (i < 0) {
      return -1L;
    }
    int j = i + key.length();
    int end = j;
    while (end < json.length()
        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
      end++;
    }
    try {
      return Long.parseLong(json.substring(j, end));
    } catch (NumberFormatException e) {
      return -1L;
    }
  }
}
