package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * M4 dead-letter machinery: a poison record is retried up to the limit
 * (rewind + refetch), its counter survives a consumer restart, and past
 * the limit it lands in the dead-letter log with the alarm fired — the
 * stream itself never wedges.
 */
class DeadLetterTest {

  @TempDir
  Path tempDir;

  /**
   * A toy broker: serves FETCH from an in-memory record list. Offsets are
   * the record index times 100 (fake record size), so "next offset" is
   * arithmetic — exactly the shape the consumer's rewind logic needs to
   * prove.
   */
  private static final class FakeBroker implements Transport {
    final List<String> payloads = new ArrayList<String>();
    volatile long lastFetchFrom = -1;
    volatile int fetchCount;

    void add(String... ps) {
      payloads.addAll(Arrays.asList(ps));
    }

    @Override
    public CompletableFuture<Frame> request(MessageType type, byte[] body) {
      fetchCount++;
      MqProto.FetchRequest req;
      try {
        req = MqProto.FetchRequest.parseFrom(body);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      lastFetchFrom = req.getOffset();
      int from = (int) (req.getOffset() / 100);
      MqProto.FetchResponse.Builder resp = MqProto.FetchResponse.newBuilder()
          .setZeroCopy(false);
      long next = req.getOffset();
      for (int i = from; i < payloads.size()
          && i - from < Math.max(1, req.getMaxCount()); i++) {
        resp.addRecords(MqProto.RecordData.newBuilder()
            .setOffset(i * 100L)
            .setTimestamp(1L)
            .setTopic("t")
            .setKeyHash(i)
            .setPayload(com.google.protobuf.ByteString.copyFromUtf8(payloads.get(i))));
        next = (i + 1) * 100L;
      }
      resp.setNextOffset(next);
      resp.setHighWatermark(payloads.size() * 100L);
      byte[] meta = resp.build().toByteArray();
      byte[] frameBody = new byte[4 + meta.length];
      ByteBuffer.wrap(frameBody).putInt(meta.length).put(meta);
      return CompletableFuture.completedFuture(
          new Frame(MessageType.FETCH_RESP, 1L, frameBody));
    }

    @Override
    public void close() {
    }
  }

  private MQConsumer consumer(Transport transport, int maxRetries,
      DeadLetterListener listener) throws IOException {
    return new MQConsumer(transport, "t",
        OffsetStore.load(new File(tempDir.toFile(), "offsets.properties")),
        10, 5_000L,
        RetryTracker.load(new File(tempDir.toFile(), "retries.properties")),
        DeadLetterStore.open(new File(tempDir.toFile(), "deadletter.log")),
        maxRetries, listener);
  }

  @Test
  void poisonRecordIsRetriedThenDeadLetteredAndTheStreamMovesOn() throws Exception {
    FakeBroker broker = new FakeBroker();
    broker.add("ok-1", "POISON", "ok-2");
    final AtomicReference<String> alarmed = new AtomicReference<String>();
    MQConsumer consumer = consumer(broker, 2, new DeadLetterListener() {
      @Override
      public void onDeadLetter(ClientRecord record, int attempts, Throwable lastError) {
        alarmed.set("alarmed:" + new String(record.getPayload(), StandardCharsets.UTF_8)
            + ":" + attempts);
      }
    });

    try {
      // Poll 1: full batch; the poison record fails -> attempt 1, rewind
      List<ClientRecord> batch1 = consumer.poll();
      assertEquals(3, batch1.size());
      consumer.reportFailure(batch1.get(1), new RuntimeException("boom-1"));

      // Poll 2: re-fetch from the poison record; fails again -> attempt 2
      List<ClientRecord> batch2 = consumer.poll();
      assertEquals(100L, broker.lastFetchFrom, "the rewind must target the poison record");
      assertEquals(2, batch2.size());
      assertEquals("POISON", new String(batch2.get(0).getPayload(), StandardCharsets.UTF_8));
      consumer.reportFailure(batch2.get(0), new RuntimeException("boom-2"));

      // Poll 3: fails a third time -> past the limit: dead letter + alarm
      List<ClientRecord> batch3 = consumer.poll();
      assertEquals(2, batch3.size());
      assertEquals(3, consumer.reportFailure(batch3.get(0),
          new RuntimeException("boom-3")), "third failure is past maxRetries=2");
      assertEquals("alarmed:POISON:3", alarmed.get());
      assertEquals(0, consumer.attemptsOn(batch3.get(0)), "counter cleared after dead-letter");

      // Poll 4: the stream moved on — fetches resume from AFTER the
      // dead-lettered record; the topic is caught up, nothing wedges
      List<ClientRecord> batch4 = consumer.poll();
      assertEquals(0, batch4.size());
      assertEquals(300L, broker.lastFetchFrom,
          "the poison record is never fetched again");
    } finally {
      consumer.close();
    }

    // The dead-letter log holds the payload and the final error, durably.
    List<String> lines = java.nio.file.Files.readAllLines(
        tempDir.resolve("deadletter.log"), StandardCharsets.UTF_8);
    assertEquals(1, lines.size());
    assertTrue(lines.get(0).contains("|t|100|"));
    assertTrue(lines.get(0).contains("|3|"), "attempts recorded");
    assertTrue(lines.get(0).contains("boom-3"), "the FINAL error is captured");
    String b64 = lines.get(0).substring(lines.get(0).lastIndexOf('|') + 1);
    assertEquals("POISON", new String(
        java.util.Base64.getDecoder().decode(b64), StandardCharsets.UTF_8));
  }

  @Test
  void retryCounterSurvivesAConsumerRestart() throws Exception {
    FakeBroker broker = new FakeBroker();
    broker.add("POISON");
    File retryFile = new File(tempDir.toFile(), "retries.properties");

    MQConsumer first = consumer(broker, 5, null);
    try {
      List<ClientRecord> batch = first.poll();
      first.reportFailure(batch.get(0), new RuntimeException("one"));
      first.reportFailure(batch.get(0), new RuntimeException("two"));
    } finally {
      first.close();
    }

    // A restarted consumer reads the SAME sidecar: the poison record's
    // history is not forgotten (it must not get a fresh retry budget).
    RetryTracker reloaded = RetryTracker.load(retryFile);
    assertEquals(2, reloaded.attempts("t", 0L));
  }

  @Test
  void successAfterRetryPrunesTheCounter() throws Exception {
    FakeBroker broker = new FakeBroker();
    broker.add("flaky", "next");
    MQConsumer consumer = consumer(broker, 3, null);
    try {
      List<ClientRecord> batch = consumer.poll();
      consumer.reportFailure(batch.get(0), new RuntimeException("transient"));
      // the retry succeeds (caller just handles it): the next poll
      // re-delivers from the flaky record — both records this time
      List<ClientRecord> batch2 = consumer.poll();
      assertEquals(0L, broker.lastFetchFrom);
      assertEquals("flaky", new String(batch2.get(0).getPayload(), StandardCharsets.UTF_8));
      assertEquals("next", new String(batch2.get(1).getPayload(), StandardCharsets.UTF_8));
      consumer.poll(); // commits the batch end, prunes below it
      assertEquals(0, consumer.attemptsOn(batch.get(0)),
          "a record that made it stops paying the sidecar tax");
    } finally {
      consumer.close();
    }
  }

  @Test
  void m1ShapeRejectsReportFailureLoudly() throws Exception {
    FakeBroker broker = new FakeBroker();
    broker.add("x");
    MQConsumer m1 = new MQConsumer(broker, "t",
        OffsetStore.load(new File(tempDir.toFile(), "offsets.properties")),
        10, 5_000L);
    try {
      List<ClientRecord> batch = m1.poll();
      assertThrows(IllegalStateException.class,
          () -> m1.reportFailure(batch.get(0), new RuntimeException("x")));
    } finally {
      m1.close();
    }
  }
}
