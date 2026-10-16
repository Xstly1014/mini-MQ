package com.minimq.bench;

import java.util.concurrent.TimeUnit;

import com.minimq.client.MQProducer;

/**
 * Debug helper (M2-4b): connect to an EXTERNAL broker and produce a few
 * messages, printing what happens — narrows down whether a failure is the
 * client stack or something specific to the spawned-broker environment.
 */
public final class ConnectSmoke {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        MQProducer p = MQProducer.connect("127.0.0.1", port, 20_000, 256 * 1024, 1L);
        int ok = 0;
        for (int i = 0; i < 10; i++) {
            try {
                Long off = p.send("smoke", new byte[64]).get(5, TimeUnit.SECONDS);
                ok++;
                if (i == 0) {
                    System.out.println("first ack offset=" + off);
                }
            } catch (Exception e) {
                System.out.println("send " + i + " FAILED: " + e);
                break;
            }
        }
        p.close();
        System.out.println("smoke done, acked=" + ok + "/10");
    }

    private ConnectSmoke() {
    }
}
