package com.minimq.bench;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.storage.FlushMode;

/**
 * Standalone broker process entry for the kill -9 durability experiments
 * (M2 task 4b): the crash must hit a REAL process — an in-process broker
 * cannot be SIGKILLed without taking the harness down with it.
 *
 * <p>Prints {@code READY:<port>} on stdout once listening (the harness
 * parses that line to learn the ephemeral port); broker logging stays on
 * stderr. SIGTERM triggers the graceful path (BrokerServer's shutdown
 * hook); SIGKILL skips it — exactly the scenario under test.
 *
 * <pre>usage: BrokerMain &lt;port&gt; &lt;storeDir&gt; &lt;SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH&gt; &lt;lingerUs&gt; &lt;batchCount&gt;</pre>
 */
public final class BrokerMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println(
                    "usage: BrokerMain <port> <storeDir> <SYNC_FLUSH|ASYNC_FLUSH|OS_FLUSH> <lingerUs> <batchCount>");
            System.exit(2);
        }
        int port = Integer.parseInt(args[0]);
        File storeDir = new File(args[1]);
        FlushMode mode = FlushMode.valueOf(args[2]);
        long lingerUs = Long.parseLong(args[3]);
        int batchCount = Integer.parseInt(args[4]);

        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(port)
                .storeDir(storeDir)
                .segmentSize(256 * 1024 * 1024)
                .flushMode(mode)
                .flushLingerUs(lingerUs)
                .flushBatchCount(batchCount));
        server.start();
        System.out.println("READY:" + server.port());
        System.out.flush();

        // Serve until a "QUIT" line arrives on stdin. Deliberately NOT
        // exiting on stdin EOF: shells that background us hand us an
        // already-EOF stdin, and a broker that dies as soon as its parent
        // stops typing is impossible to spawn from tests — diag.sh proved
        // exactly that (READY printed, then the read returned -1 and the
        // broker vanished before the first connect). On EOF we park forever;
        // the harness stops us via QUIT, SIGTERM (graceful hook) or SIGKILL
        // (the durability scenario under test).
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line;
            try {
                line = stdin.readLine();
            } catch (IOException e) {
                line = null; // stdin broken: fall through to park
            }
            if (line == null) {
                Thread.currentThread().join(); // park until killed
                break;
            }
            if (line.trim().equalsIgnoreCase("QUIT")) {
                break;
            }
        }
        server.shutdown();
    }

    private BrokerMain() {
    }
}
