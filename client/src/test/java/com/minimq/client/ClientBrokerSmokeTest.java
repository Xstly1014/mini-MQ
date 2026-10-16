package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.broker.BrokerConfig;
import com.minimq.broker.BrokerServer;

/**
 * Client SDK against a real broker over loopback: the produce-consume loop,
 * restart resume, and topic filtering. These are the M1-3 acceptance paths;
 * the heavier end-to-end reconciliation runs in M1-4.
 */
class ClientBrokerSmokeTest {

    @TempDir
    Path tempDir;

    private BrokerServer server;

    @BeforeEach
    void startBroker() throws Exception {
        server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.resolve("broker-data").toFile())
                .segmentSize(256 * 1024));
        server.start();
    }

    @AfterEach
    void stopBroker() {
        server.shutdown();
    }

    private int port() {
        return server.port();
    }

    @Test
    void produceConsumeRoundTrip() throws Exception {
        MQProducer producer = MQProducer.connect("127.0.0.1", port(),
                3000, 64 * 1024, 20);
        try {
            for (int i = 0; i < 20; i++) {
                producer.send("orders", ("msg-" + i).getBytes("UTF-8"));
            }
        } finally {
            producer.close(); // drains everything, all futures complete
        }

        MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", port(),
                "orders", "g1", tempDir.resolve("offsets").toFile(), 32, 3000);
        try {
            List<String> payloads = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (payloads.size() < 20 && System.currentTimeMillis() < deadline) {
                for (ClientRecord r : consumer.poll()) {
                    payloads.add(new String(r.getPayload(), "UTF-8"));
                }
            }
            assertEquals(20, payloads.size());
            for (int i = 0; i < 20; i++) {
                assertEquals("msg-" + i, payloads.get(i), "batch order must be send order");
            }
            consumer.commit();
        } finally {
            consumer.close();
        }
    }

    @Test
    void consumerResumesFromPersistedOffsetAfterRestart() throws Exception {
        MQProducer producer = MQProducer.connect("127.0.0.1", port(),
                3000, 64 * 1024, 20);
        try {
            for (int i = 0; i < 10; i++) {
                producer.send("orders", ("msg-" + i).getBytes("UTF-8"));
            }
        } finally {
            producer.close();
        }

        // first session: consume exactly one small batch, commit, "crash"
        java.io.File offsetsDir = tempDir.resolve("offsets").toFile();
        MQConsumer first = MQConsumer.subscribe("127.0.0.1", port(),
                "orders", "g-resume", offsetsDir, 4, 3000);
        List<String> seen = new ArrayList<String>();
        try {
            for (ClientRecord r : first.poll()) {
                seen.add(new String(r.getPayload(), "UTF-8"));
            }
            assertEquals(4, seen.size(), "fetchMaxCount=4: first poll returns 4");
            first.commit(); // persists end of this batch
        } finally {
            first.close();
        }

        // second session, same group file: must resume, not restart from head
        MQConsumer second = MQConsumer.subscribe("127.0.0.1", port(),
                "orders", "g-resume", offsetsDir, 4, 3000);
        try {
            List<String> rest = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (rest.size() < 6 && System.currentTimeMillis() < deadline) {
                for (ClientRecord r : second.poll()) {
                    rest.add(new String(r.getPayload(), "UTF-8"));
                }
            }
            assertEquals(6, rest.size(), "remaining 6 messages, no duplicates from the head");
            assertEquals("msg-4", rest.get(0));
            assertEquals("msg-9", rest.get(5));
        } finally {
            second.close();
        }
    }

    @Test
    void consumerOnlySeesItsTopic() throws Exception {
        MQProducer producer = MQProducer.connect("127.0.0.1", port(),
                3000, 64 * 1024, 20);
        try {
            producer.send("orders", "o1".getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
            producer.send("payments", "p1".getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
            producer.send("orders", "o2".getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
            producer.send("payments", "p2".getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
            producer.send("orders", "o3".getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
        } finally {
            producer.close();
        }

        MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", port(),
                "payments", "g-pay", tempDir.resolve("offsets").toFile(), 32, 3000);
        try {
            List<String> seen = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                List<ClientRecord> batch = consumer.poll();
                for (ClientRecord r : batch) {
                    seen.add(new String(r.getPayload(), "UTF-8"));
                }
                if (seen.size() >= 2 && batch.isEmpty()) {
                    break; // caught up
                }
            }
            assertEquals(2, seen.size(), "broker-side topic filter must hold");
            assertEquals("p1", seen.get(0));
            assertEquals("p2", seen.get(1));
        } finally {
            consumer.close();
        }
    }

    /**
     * M2 zero-copy end-to-end: broker sends FETCH responses as head +
     * FileRegions (sendfile); the raw span contains foreign-topic records,
     * the consumer must still slice exactly its own payloads, in order,
     * with the offset cursor intact.
     */
    @Test
    void zeroCopyFetchRoundTripWithMixedTopics() throws Exception {
        BrokerServer zcServer = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.resolve("zc-broker-data").toFile())
                .segmentSize(256 * 1024)
                .zeroCopyFetch(true));
        zcServer.start();
        try {
            MQProducer producer = MQProducer.connect("127.0.0.1", zcServer.port(),
                    3000, 64 * 1024, 20);
            try {
                for (int i = 0; i < 10; i++) {
                    producer.send("noise", ("noise-" + i).getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
                    producer.send("wanted", ("w-" + i).getBytes("UTF-8")).get(5, TimeUnit.SECONDS);
                }
            } finally {
                producer.close();
            }

            MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", zcServer.port(),
                    "wanted", "g-zc", tempDir.resolve("zc-offsets").toFile(), 32, 3000);
            try {
                List<String> seen = new ArrayList<String>();
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline) {
                    List<ClientRecord> batch = consumer.poll();
                    for (ClientRecord r : batch) {
                        seen.add(new String(r.getPayload(), "UTF-8"));
                    }
                    if (seen.size() >= 10 && batch.isEmpty()) {
                        break; // caught up
                    }
                }
                assertEquals(10, seen.size(), "exactly the wanted records, zero noise");
                for (int i = 0; i < 10; i++) {
                    assertEquals("w-" + i, seen.get(i), "order must survive zero-copy slicing");
                }
                consumer.commit();
            } finally {
                consumer.close();
            }
        } finally {
            zcServer.shutdown();
        }
    }
}
