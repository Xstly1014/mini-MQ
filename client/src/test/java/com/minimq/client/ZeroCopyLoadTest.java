package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * Zero-copy FETCH under load (M2 task 2b/3 debugging aid): the smoke test
 * proves correctness at 10 records; this one ramps the record count over a
 * REAL NIO loopback (EmbeddedChannel never exercises transferTo) to catch
 * stream-misalignment bugs that only show with larger raw tails.
 */
class ZeroCopyLoadTest {

    @TempDir
    Path tempDir;

    private BrokerServer server;

    @BeforeEach
    void startBroker() throws Exception {
        server = new BrokerServer(BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.resolve("store").toFile())
                .segmentSize(256 * 1024 * 1024)
                .zeroCopyFetch(true));
        server.start();
    }

    @AfterEach
    void stopBroker() {
        server.shutdown();
    }

    private void produceAndConsumeAll(int count) throws Exception {
        MQProducer producer = MQProducer.connect("127.0.0.1", server.port(),
                20_000, 256 * 1024, 1L);
        try {
            for (int i = 0; i < count; i++) {
                producer.send("t", ("m-" + i).getBytes("UTF-8")).get(20, TimeUnit.SECONDS);
            }
        } finally {
            producer.close();
        }

        MQConsumer consumer = MQConsumer.subscribe("127.0.0.1", server.port(),
                "t", "g", tempDir.resolve("offsets").toFile(), 256, 20_000);
        try {
            List<String> seen = new ArrayList<String>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (seen.size() < count && System.currentTimeMillis() < deadline) {
                List<ClientRecord> batch = consumer.poll();
                for (ClientRecord r : batch) {
                    seen.add(new String(r.getPayload(), "UTF-8"));
                }
            }
            assertEquals(count, seen.size(), "all records must arrive, in order");
            assertEquals("m-0", seen.get(0));
            assertEquals("m-" + (count - 1), seen.get(count - 1));
        } finally {
            consumer.close();
        }
    }

    @Test
    void zeroCopySurvivesHundredRecords() throws Exception {
        produceAndConsumeAll(100);
    }

    @Test
    void zeroCopySurvivesFiveThousandRecords() throws Exception {
        produceAndConsumeAll(5_000);
    }
}
