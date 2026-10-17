package com.minimq.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loopback control port for the M4 chaos matrix (ops tooling, NOT wire
 * protocol): the ChaosRunner drives partition injection and reads node
 * state (role/term/LEO/HW/ISR/flush frontier/dedup stats) over a tiny
 * line protocol, so the matrix's timing control ("kill when the flush
 * backlog is non-zero", "kill while replication lag is large") targets
 * real in-process observations instead of guesses.
 *
 * <pre>
 *   partition in-add 2      {"ok":true,"inBlack":[2]}
 *   partition in-remove 2   {"ok":true,"inBlack":[]}
 *   partition in-clear       {"ok":true,"inBlack":[]}
 *   state                    {"node":0,"role":"LEADER",...}
 * </pre>
 *
 * <p>
 * Security posture: binds 127.0.0.1 only, started only when the broker
 * is configured with a chaos port (ClusterMain test nodes); a production
 * broker never opens it. One command per line, one JSON response per
 * command, connection stays open until the peer hangs up.
 */
public final class ChaosControlServer {

  private static final Logger LOG = Logger.getLogger(ChaosControlServer.class.getName());

  private final PartitionShield shield;
  private final Supplier<String> stateJson;
  private final ServerSocket socket;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private volatile Thread acceptThread;

  ChaosControlServer(int port, PartitionShield shield, Supplier<String> stateJson)
      throws IOException {
    this.shield = shield;
    this.stateJson = stateJson;
    // loopback only: chaos control is a local operator's lever, never a
    // network surface
    this.socket = new ServerSocket(port, 4, java.net.InetAddress.getByName("127.0.0.1"));
  }

  /** Actual bound port (useful when configured with 0). */
  public int port() {
    return socket.getLocalPort();
  }

  public void start() {
    acceptThread = new Thread(new Runnable() {
      @Override
      public void run() {
        acceptLoop();
      }
    }, "mini-mq-chaos-control");
    acceptThread.setDaemon(true);
    acceptThread.start();
  }

  public void close() {
    running.set(false);
    try {
      socket.close();
    } catch (IOException ignored) {
      // closing is the point
    }
  }

  private void acceptLoop() {
    while (running.get()) {
      try {
        final Socket client = socket.accept();
        Thread handler = new Thread(new Runnable() {
          @Override
          public void run() {
            serve(client);
          }
        }, "mini-mq-chaos-conn");
        handler.setDaemon(true);
        handler.start();
      } catch (IOException e) {
        if (running.get()) {
          LOG.log(Level.WARNING, "chaos control accept failed", e);
        }
        return; // socket closed = shutdown
      }
    }
  }

  private void serve(Socket client) {
    try {
      client.setTcpNoDelay(true);
      BufferedReader in = new BufferedReader(
          new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
      OutputStream out = client.getOutputStream();
      String line;
      while (running.get() && (line = in.readLine()) != null) {
        String resp = execute(line.trim());
        out.write((resp + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
      }
    } catch (IOException e) {
      // peer hung up; nothing to log at this severity
    } finally {
      try {
        client.close();
      } catch (IOException ignored) {
      }
    }
  }

  /** One command -> one JSON line. Package-private so tests can call directly. */
  static String execute(String command, PartitionShield shield, Supplier<String> stateJson) {
    if (command.startsWith("partition ")) {
      String op = command.substring("partition ".length());
      if (op.startsWith("in-add ")) {
        shield.inAdd(parseId(op.substring("in-add ".length())));
        return ok(shield);
      }
      if (op.startsWith("in-remove ")) {
        shield.inRemove(parseId(op.substring("in-remove ".length())));
        return ok(shield);
      }
      if (op.equals("in-clear")) {
        shield.inClear();
        return ok(shield);
      }
      return "{\"ok\":false,\"error\":\"unknown partition op\"}";
    }
    if (command.equals("state")) {
      return stateJson.get();
    }
    return "{\"ok\":false,\"error\":\"unknown command\"}";
  }

  private String execute(String command) {
    return execute(command, shield, stateJson);
  }

  private static String ok(PartitionShield shield) {
    return "{\"ok\":true,\"inBlack\":" + ids(shield.inBlackSnapshot()) + "}";
  }

  static String ids(Set<Integer> ids) {
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (Integer id : ids) {
      if (!first) {
        sb.append(',');
      }
      sb.append(id.intValue());
      first = false;
    }
    return sb.append(']').toString();
  }

  private static int parseId(String s) {
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
