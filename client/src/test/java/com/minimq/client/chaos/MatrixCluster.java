package com.minimq.client.chaos;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.storage.FlushMode;

/**
 * The matrix's 3-process cluster: same skeleton as MultiProcessClusterTest's
 * ProcCluster, plus the two M4 opt-ins wired through ClusterMain — a chaos
 * control port per node (partition injection + state reads) and the leader
 * dedup window (100k entries, the manual's default). Every node runs the
 * matrix's main configuration: SYNC_FLUSH.
 *
 * <p>
 * The chaos ports make this harness's observation WHITE-box where the
 * matrix needs it (role/LEO/flushed/ISR/dedup stats on demand) while the
 * data plane stays black-box (produce/consume through the real SDK).
 */
final class MatrixCluster implements java.io.Closeable {

  static final int NODES = 3;
  private static final String ELECTED_MARKER = "elected leader";
  /** The manual's default window: 100k entries. */
  static final int DEDUP_ENTRIES = 100_000;

  private final File root;
  final int[] ports = new int[NODES];
  private final int[] chaosPorts = new int[NODES];
  final List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
  private final File[] storeDirs = new File[NODES];
  private final File[] logs = new File[NODES];
  private final Process[] procs = new Process[NODES];
  private final boolean[] alive = new boolean[NODES];
  private final int[] electedBaseline = new int[NODES];
  final File offsetsDir;
  /** The replayable chaos journal (chaos-log.jsonl), written by the test. */
  final File chaosLog;

  MatrixCluster(File root) throws IOException {
    this.root = root;
    for (int i = 0; i < NODES; i++) {
      ports[i] = freePort();
      chaosPorts[i] = freePort();
      addresses.add(new InetSocketAddress("127.0.0.1", ports[i]));
    }
    for (int i = 0; i < NODES; i++) {
      storeDirs[i] = new File(new File(root, "node" + i), "commitlog");
      logs[i] = new File(root, "node" + i + ".log");
    }
    this.offsetsDir = new File(root, "consumer-offsets");
    this.chaosLog = new File(root, "chaos-log.jsonl");
    if (!offsetsDir.mkdirs()) {
      throw new IOException("cannot create " + offsetsDir);
    }
  }

  int chaosPort(int node) {
    return chaosPorts[node];
  }

  boolean isAlive(int node) {
    return alive[node];
  }

  void startAll() throws Exception {
    for (int i = 0; i < NODES; i++) {
      startNode(i);
    }
  }

  private void startNode(int i) throws Exception {
    if (alive[i]) {
      return;
    }
    if (!new File(root, "node" + i).exists() && !new File(root, "node" + i).mkdirs()) {
      throw new IOException("cannot create " + new File(root, "node" + i));
    }
    StringBuilder peers = new StringBuilder();
    for (int j = 0; j < NODES; j++) {
      peers.append(j > 0 ? "," : "").append("127.0.0.1:").append(ports[j]);
    }
    String javaBin = System.getProperty("java.home") + File.separator + "bin"
        + File.separator + "java";
    ProcessBuilder pb = new ProcessBuilder(javaBin,
        "-Xms16m", "-Xmx256m",
        "-cp", System.getProperty("java.class.path"),
        "com.minimq.broker.ClusterMain",
        String.valueOf(i), String.valueOf(ports[i]),
        storeDirs[i].getAbsolutePath(),
        FlushMode.SYNC_FLUSH.name(),
        peers.toString(),
        String.valueOf(chaosPorts[i]),
        String.valueOf(DEDUP_ENTRIES));
    pb.directory(new File(root, "node" + i));
    pb.redirectErrorStream(false);
    pb.redirectError(logs[i]);
    procs[i] = pb.start();
    alive[i] = true;

    final CompletableFuture<String> ready = new CompletableFuture<String>();
    final Process p = procs[i];
    Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          BufferedReader in = new BufferedReader(
              new InputStreamReader(p.getInputStream(), "UTF-8"));
          String line = in.readLine();
          ready.complete(line == null ? "<eof>" : line);
          while (in.readLine() != null) {
            // drain so the child never blocks on a full pipe
          }
        } catch (IOException e) {
          ready.completeExceptionally(e);
        }
      }
    }, "m4-matrix-ready-" + i);
    reader.setDaemon(true);
    reader.start();
    String line = ready.get(30, TimeUnit.SECONDS);
    if (!line.startsWith("READY:")) {
      throw new AssertionError("node-" + i + " never became READY, stdout: "
          + line + ", log tail: " + tail(i));
    }
  }

  /** REAL kill -9: TerminateProcess — no shutdown hooks, no flush. */
  void killNode(int i) {
    if (!alive[i]) {
      return;
    }
    procs[i].destroyForcibly();
    try {
      procs[i].waitFor(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    alive[i] = false;
  }

  /** Graceful: QUIT walks BrokerServer's ordered shutdown. */
  void gracefulStop(int i) throws Exception {
    if (!alive[i]) {
      return;
    }
    procs[i].getOutputStream().write("QUIT\n".getBytes(StandardCharsets.US_ASCII));
    procs[i].getOutputStream().flush();
    if (!procs[i].waitFor(20, TimeUnit.SECONDS)) {
      throw new AssertionError("node-" + i + " ignored QUIT; log tail: " + tail(i));
    }
    alive[i] = false;
  }

  /** Same storeDir, same ports: the revival IS the point. */
  void restartNode(int i) throws Exception {
    killNode(i);
    startNode(i);
  }

  // ------------------------------------------------------------------
  // black-box leader finding (probe produce, like M3's harness)
  // ------------------------------------------------------------------

  int awaitLeader(long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      int leader = probeLeaderOnce();
      if (leader >= 0) {
        return leader;
      }
      Thread.sleep(150);
    }
    throw new AssertionError("no leader elected within " + timeoutMs
        + "ms; logs: " + allRoles());
  }

  /** Waits for a leader OTHER than the excluded node (post-kill failover). */
  int awaitLeaderOtherThan(int exclude, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      int leader = probeLeaderOnce();
      if (leader >= 0 && leader != exclude) {
        return leader;
      }
      Thread.sleep(150);
    }
    throw new AssertionError("no leader other than node " + exclude
        + " within " + timeoutMs + "ms; logs: " + allRoles());
  }

  private static final byte[] PROBE_PAYLOAD = new byte[4];

  private int probeLeaderOnce() {
    for (int i = 0; i < NODES; i++) {
      if (!alive[i]) {
        continue;
      }
      try {
        com.minimq.client.MQProducer probe = com.minimq.client.MQProducer.connect(
            "127.0.0.1", ports[i], 1_500, 2_048, 20);
        try {
          probe.send("probe", PROBE_PAYLOAD).get(2, TimeUnit.SECONDS);
          return i;
        } catch (Exception notLeaderOrDown) {
          // NOT_LEADER or connect refused: try the next node
        } finally {
          probe.close();
        }
      } catch (Exception connectFailed) {
        // node down or election in flight
      }
    }
    return -1;
  }

  // ------------------------------------------------------------------
  // election-line telemetry (same as M3's harness)
  // ------------------------------------------------------------------

  void baselineElectedLines() throws IOException {
    for (int i = 0; i < NODES; i++) {
      electedBaseline[i] = countElectedLines(i);
    }
  }

  long awaitNewElection(long timeoutMs) throws IOException, InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < NODES; i++) {
        if (alive[i] && countElectedLines(i) > electedBaseline[i]) {
          return System.nanoTime();
        }
      }
      Thread.sleep(20);
    }
    throw new AssertionError("no new election within " + timeoutMs
        + "ms; logs: " + allRoles());
  }

  private int countElectedLines(int i) throws IOException {
    if (!logs[i].exists()) {
      return 0;
    }
    int count = 0;
    for (String line : Files.readAllLines(logs[i].toPath(), StandardCharsets.ISO_8859_1)) {
      if (line.contains(ELECTED_MARKER)) {
        count++;
      }
    }
    return count;
  }

  private String tail(int i) {
    try {
      if (!logs[i].exists()) {
        return "<no log>";
      }
      List<String> lines = Files.readAllLines(logs[i].toPath(), StandardCharsets.ISO_8859_1);
      StringBuilder sb = new StringBuilder();
      for (int j = Math.max(0, lines.size() - 15); j < lines.size(); j++) {
        sb.append(lines.get(j)).append('\n');
      }
      return sb.toString();
    } catch (IOException e) {
      return "<unreadable: " + e + ">";
    }
  }

  private String allRoles() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < NODES; i++) {
      sb.append("--- node-").append(i).append(alive[i] ? "" : " (DOWN)")
          .append(" ---\n").append(tail(i));
    }
    return sb.toString();
  }

  int someFollowerOf(int leader) {
    for (int i = 0; i < NODES; i++) {
      if (i != leader) {
        return i;
      }
    }
    throw new IllegalArgumentException("no follower besides node " + leader);
  }

  // ------------------------------------------------------------------
  // reconciliation consume (seq keyed — offset is storage, seq is business)
  // ------------------------------------------------------------------

  /**
   * Drains the topic through the failover consumer. Records carry the
   * producing seq as the first 4 payload bytes (big-endian) — the matrix
   * reconciles by seq, never by offset (the manual's pit rule: after a
   * truncation, offsets lie; seqs do not).
   */
  List<Integer> drainSeqs(String topic, int maxCount, long timeoutMs) throws Exception {
    MQConsumer consumer = MQConsumer.subscribeCluster(addresses, topic,
        "matrix-" + System.nanoTime(), offsetsDir, 100, 5_000L);
    try {
      List<Integer> seqs = new ArrayList<Integer>();
      long deadline = System.currentTimeMillis() + timeoutMs;
      // 150 empty polls (~3.75s) spans the new leader's sweep window: after
      // a partition the survivors' optimistic HW seeds stay frozen until the
      // lagMax sweep evicts the isolated peer — records physically present
      // become fetchable only then. A 750ms patience would declare them lost.
      int emptyPolls = 0;
      while (emptyPolls < 400 && System.currentTimeMillis() < deadline
          && seqs.size() < maxCount) {
        List<ClientRecord> batch = consumer.poll();
        if (batch.isEmpty()) {
          emptyPolls++;
          Thread.sleep(25);
        } else {
          emptyPolls = 0;
          for (ClientRecord r : batch) {
            seqs.add(Integer.valueOf(java.nio.ByteBuffer.wrap(r.getPayload()).getInt(0)));
          }
        }
      }
      return seqs;
    } finally {
      consumer.close();
    }
  }

  private static int freePort() throws IOException {
    try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @Override
  public void close() {
    for (int i = 0; i < NODES; i++) {
      if (alive[i]) {
        procs[i].destroyForcibly();
        alive[i] = false;
      }
    }
    for (int i = 0; i < NODES; i++) {
      try {
        if (procs[i] != null) {
          procs[i].waitFor(5, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
