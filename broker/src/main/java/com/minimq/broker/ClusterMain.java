package com.minimq.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.minimq.storage.FlushMode;

/**
 * One node of a static multi-process cluster (M3 task 5): each process is a
 * full broker with its own storage, election timers and replication thread —
 * the real process boundary an in-JVM cluster cannot simulate (SIGKILL cannot
 * be delivered inside a shared JVM without taking the harness down with it).
 *
 * <p>Prints {@code READY:<port>} on stdout once listening; broker logging —
 * including the {@code node-N elected leader, epoch N} line the failover
 * timing breakdown watches for — goes to stderr. A {@code QUIT} line on
 * stdin triggers the graceful path (flush, step-down, zero-loss window);
 * {@code Process.destroyForcibly()} is the kill -9 scenario: no shutdown
 * hooks, no flush — durability is handed to the page cache, the ISR
 * majority, and the M2 flush tiers.
 *
 * <p>Election timers keep etcd's 10:1 shape (heartbeat 75ms, timeout
 * randomized in [750, 1500)ms) so the playbook's 8s failover budget maps
 * onto the documented breakdown: detection &le;1.5s + election &le;0.5s +
 * client redirect ~1s + safety margin.
 *
 * <pre>usage: ClusterMain &lt;nodeId&gt; &lt;port&gt; &lt;storeDir&gt; &lt;SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH&gt; &lt;host:port,host:port,...&gt;</pre>
 *
 * <p>The peers list is the FULL static topology, index == nodeId (entry 0 is
 * node-0's address); every node is started with the identical list.
 */
public final class ClusterMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("usage: ClusterMain <nodeId> <port> <storeDir>"
                    + " <SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH> <host:port,host:port,...>");
            System.exit(2);
        }
        int nodeId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        File storeDir = new File(args[2]);
        FlushMode mode = FlushMode.valueOf(args[3]);
        List<InetSocketAddress> peers = new ArrayList<InetSocketAddress>();
        for (String spec : args[4].split(",")) {
            String[] hostPort = spec.trim().split(":");
            peers.add(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        if (!storeDir.exists() && !storeDir.mkdirs()) {
            throw new IOException("cannot create " + storeDir);
        }

        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(port)
                .storeDir(storeDir)
                .segmentSize(64 * 1024 * 1024)
                .flushMode(mode)
                .cluster(nodeId, peers)
                .minIsr(2)
                .replicaLagMaxMs(3_000)
                .heartbeatIntervalMs(75)
                .electionTimeoutBaseMs(750)
                .replicationAwaitTimeoutMs(5_000));
        server.start();
        System.out.println("READY:" + server.port());
        System.out.flush();

        // Serve until a "QUIT" line arrives on stdin (graceful), the process
        // is terminated (kill -9 under test), or stdin EOF (park forever —
        // the same discipline as BrokerMain: a node that dies when its
        // parent stops typing cannot be spawned from a harness).
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line;
            try {
                line = stdin.readLine();
            } catch (IOException e) {
                line = null;
            }
            if (line == null) {
                Thread.currentThread().join();
                break;
            }
            if (line.trim().equalsIgnoreCase("QUIT")) {
                break;
            }
        }
        server.shutdown();
    }

    private ClusterMain() {
    }
}
