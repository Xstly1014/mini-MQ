package com.minimq.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;
import com.minimq.client.ClientRecord;
import com.minimq.client.MQConsumer;
import com.minimq.client.MQProducer;

/**
 * Standalone zero-copy reproduction (M2 task 2b debugging): broker + SDK in
 * one JVM, JUL warnings visible on stderr (Maven exec/surefire swallow
 * them). Args: [count=5000] [fetchMax=256] [zeroCopy=true].
 */
public final class ZeroCopyRepro {

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 5_000;
        int fetchMax = args.length > 1 ? Integer.parseInt(args[1]) : 256;
        boolean zeroCopy = args.length > 2 ? Boolean.parseBoolean(args[2]) : true;

        Path root = Files.createTempDirectory("mini-mq-zc-repro-");
        BrokerServer server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(root.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                .zeroCopyFetch(zeroCopy));
        server.start();
        try {
            MQProducer producer = MQProducer.connect("127.0.0.1", server.port(),
                    20_000, 256 * 1024, 1L);
            try {
                for (int i = 0; i < count; i++) {
                    producer.send("t", ("m-" + i).getBytes("UTF-8")).get(20, TimeUnit.SECONDS);
                }
            } finally {
                producer.close();
            }
            System.out.println("produced " + count);

            MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", server.port(),
                    "t", "g", root.resolve("offsets").toFile(), fetchMax, 20_000);
            try {
                long received = 0;
                while (received < count) {
                    List<ClientRecord> batch = consumer.poll();
                    if (batch.isEmpty()) {
                        Thread.sleep(50);
                        continue;
                    }
                    for (ClientRecord r : batch) {
                        if (!("m-" + (received)).equals(new String(r.getPayload(), "UTF-8"))) {
                            throw new IllegalStateException("out of order at " + received
                                    + ": " + new String(r.getPayload(), "UTF-8"));
                        }
                        received++;
                    }
                    System.out.println("consumed " + received);
                }
                System.out.println("OK: all " + count + " consumed");
            } finally {
                consumer.close();
            }
        } finally {
            server.shutdown();
        }
    }

    private ZeroCopyRepro() {
    }
}
