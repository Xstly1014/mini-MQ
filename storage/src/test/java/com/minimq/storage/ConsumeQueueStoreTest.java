package com.minimq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ConsumeQueueStore (M2 task 2a) semantics: asynchronous per-topic indexing,
 * M1-log upgrade replay, crash-window alignment in both directions (index
 * ahead of the log / index behind the log), and dispatch that never stalls
 * on unindexable topics.
 */
class ConsumeQueueStoreTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    private static Message msg(String topic, int seq) {
        return new Message(1_700_000_000_000L + seq, seq, topic,
                ("payload-" + seq).getBytes());
    }

    private File logDir() {
        return tempDir.resolve("log").toFile();
    }

    private File cqDir() {
        return tempDir.resolve("cq").toFile();
    }

    private static void appendAll(CommitLog log, String topic, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            log.append(msg(topic, i));
        }
    }

    @Test
    void dispatchFollowsLogAndIndexesPerTopic() throws Exception {
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        try {
            for (int i = 0; i < 100; i++) {
                log.append(msg(i % 2 == 0 ? "even" : "odd", i));
            }
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));

            List<ConsumeQueue.Entry> evens = store.readFrom("even", 0, 1000);
            List<ConsumeQueue.Entry> odds = store.readFrom("odd", 0, 1000);
            assertEquals(50, evens.size());
            assertEquals(50, odds.size());

            // offsets strictly ascend and interleave across topics
            long prev = -1;
            for (ConsumeQueue.Entry e : evens) {
                assertTrue(e.offset > prev);
                prev = e.offset;
                assertTrue(e.size > 0);
            }

            // maxCount bounds the batch; resuming from the last end continues
            List<ConsumeQueue.Entry> page = store.readFrom("even", 0, 10);
            assertEquals(10, page.size());
            List<ConsumeQueue.Entry> next = store.readFrom("even", page.get(9).end(), 1000);
            assertEquals(40, next.size());

            // unknown topic is simply empty, never an error
            assertTrue(store.readFrom("missing", 0, 10).isEmpty());
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * The M1 -> M2 upgrade path: a log written before indexes existed gets a
     * complete index on first open (dispatch starts at 0, replays everything).
     */
    @Test
    void freshStoreBuildsIndexForPreExistingLog() throws Exception {
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        appendAll(log, "alpha", 300);
        appendAll(log, "beta", 200);
        log.close();

        log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        try {
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 10_000));
            assertEquals(300, store.readFrom("alpha", 0, 100_000).size());
            assertEquals(200, store.readFrom("beta", 0, 100_000).size());
            // indexed offsets point at real records: fetch-and-decode round trip
            List<ConsumeQueue.Entry> entries = store.readFrom("beta", 0, 1);
            List<Message> one = log.readFrom(entries.get(0).offset, 1);
            assertEquals(1, one.size());
            assertEquals("beta", one.get(0).getTopic());
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * Crash window, index BEHIND the log: one queue's file lost half its
     * entries. On reopen the store must rebuild the laggard AND must not
     * double-index the healthy queue (its entries all sit below its cursor).
     */
    @Test
    void laggardQueueIsRebuiltAndHealthyQueueNotDuplicated() throws Exception {
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        appendAll(log, "healthy", 100);
        appendAll(log, "laggard", 100);
        assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));
        store.close();
        log.close(); // release the mapping before reopen (Windows locks files)

        // simulate a torn index: drop the laggard queue's tail (60 entries)
        ConsumeQueue torn = ConsumeQueue.open(new File(cqDir(), "laggard"));
        assertEquals(100, torn.entryCount());
        torn.truncateToEntries(40);
        torn.close();

        log = CommitLog.open(logDir(), SEGMENT_SIZE);
        store = ConsumeQueueStore.open(cqDir(), log);
        try {
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 10_000));
            assertEquals(100, store.readFrom("laggard", 0, 100_000).size(),
                    "torn queue must be rebuilt to full length");
            assertEquals(100, store.readFrom("healthy", 0, 100_000).size(),
                    "healthy queue must not be double-indexed during replay");
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * Crash window, index AHEAD of the log: entries whose backing records
     * were truncated by CommitLog recovery must be dropped on open.
     */
    @Test
    void orphanedIndexTailIsTruncatedOnOpen() throws Exception {
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        appendAll(log, "t1", 50);
        assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));
        store.close();
        log.close(); // release the mapping before reopen (Windows locks files)

        // simulate the crash artifact: an entry pointing past the log end
        ConsumeQueue q = ConsumeQueue.open(new File(cqDir(), "t1"));
        q.append(log.maxOffset() + 12345, 100);
        assertEquals(51, q.entryCount());
        q.close();

        log = CommitLog.open(logDir(), SEGMENT_SIZE);
        store = ConsumeQueueStore.open(cqDir(), log);
        try {
            assertEquals(50, store.readFrom("t1", 0, 100_000).size(),
                    "entry without a backing record must be dropped");
            // and dispatch is already caught up: the cursor starts at the
            // queue's (truncated) tail, no phantom rebuild
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));
            assertEquals(50, store.readFrom("t1", 0, 100_000).size());
        } finally {
            store.close();
            log.close();
        }
    }

    /**
     * A topic that cannot become a directory name must never stall dispatch
     * for the well-behaved topics.
     */
    @Test
    void unindexableTopicDoesNotStallDispatch() throws Exception {
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        try {
            log.append(msg("good/one", 1)); // '/' cannot appear in a dir name
            log.append(msg("fine", 2));
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));
            assertTrue(store.readFrom("good/one", 0, 10).isEmpty(),
                    "unindexable topic has no queue");
            assertEquals(1, store.readFrom("fine", 0, 10).size());
        } finally {
            store.close();
            log.close();
        }
    }

    @Test
    void junkDirectoriesUnderRootAreIgnored() throws Exception {
        assertTrue(new File(cqDir(), "not a topic!").mkdirs(), "junk dir setup");
        CommitLog log = CommitLog.open(logDir(), SEGMENT_SIZE);
        ConsumeQueueStore store = ConsumeQueueStore.open(cqDir(), log);
        try {
            log.append(msg("real", 1));
            assertTrue(store.awaitDispatchedTo(log.maxOffset(), 5_000));
            assertEquals(1, store.readFrom("real", 0, 10).size());
            assertEquals(1, store.queueCount(), "junk dir must not become a queue");
        } finally {
            store.close();
            log.close();
        }
    }
}
