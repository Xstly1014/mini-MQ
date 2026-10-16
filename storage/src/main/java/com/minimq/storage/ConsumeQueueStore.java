package com.minimq.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Owns every topic's {@link ConsumeQueue} and keeps them aligned with the
 * {@link CommitLog} (M2 task 2a) — the RocketMQ "reput" design:
 *
 * <ul>
 *   <li><b>Write path pays nothing.</b> PRODUCE appends the log only; the
 *   index is built asynchronously by one dispatch thread. This is not just
 *   performance: indexing inline after the log append would race across
 *   business-pool threads (thread B could index offset 200 before thread A
 *   indexes 100 into the same topic queue), and {@link ConsumeQueue} entries
 *   must ascend in CommitLog order — the binary search depends on it.</li>
 *   <li><b>One thread = one order.</b> The dispatcher reads the log strictly
 *   forward from {@code dispatchFrom}, so per-queue entries are appended in
 *   perfect order with no coordination.</li>
 *   <li><b>Startup replay is the same loop.</b> On open, queues whose tail
 *   points past the recovered log end are truncated (crash artifact); the
 *   dispatch cursor then starts at the LAGGARD queue's position and the
 *   normal follow loop rebuilds whatever is missing. Derived data is never
 *   trusted further than the log itself.</li>
 * </ul>
 *
 * Durability note: the index is a cache. SYNC_FLUSH durability gates on the
 * CommitLog only; an index page lost to a crash is rebuilt by replay. The
 * dispatcher forces the files it wrote each round so a clean restart usually
 * resumes without replay at all.
 */
public final class ConsumeQueueStore implements Closeable {

    private static final Logger LOG = Logger.getLogger(ConsumeQueueStore.class.getName());

    /**
     * Topic directory names must be filesystem-safe. A topic failing this
     * pattern is skipped by the dispatcher (message stays in the log; the M1
     * full-scan path is gone) and logged once — a bad topic must never stall
     * dispatch for the good ones.
     */
    private static final Pattern TOPIC_DIR_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    /** Records per dispatch round (log read batch, not a fetch batch). */
    private static final int DISPATCH_BATCH = 512;

    private final File rootDir;
    private final CommitLog commitLog;
    private final ConcurrentMap<String, ConsumeQueue> queues =
            new ConcurrentHashMap<String, ConsumeQueue>();
    /**
     * Per-topic indexed-up-to position (end of the queue's last entry).
     * Written by the dispatch thread only; the replay rule is "index a record
     * iff its offset is at/above the topic's cursor", which is what keeps
     * queues ahead of the global laggard from double-indexing during replay.
     */
    private final ConcurrentMap<String, Long> cursors =
            new ConcurrentHashMap<String, Long>();
    private final Set<String> warnedTopics = Collections.newSetFromMap(
            new ConcurrentHashMap<String, Boolean>());

    private final Thread dispatcher;
    private volatile boolean running = true;
    /** Dispatch cursor: everything below it is indexed in every queue. */
    private volatile long dispatchFrom;
    /**
     * Serializes dispatch rounds against replication truncation (M3): both
     * hold this lock, so the dispatcher can never read a log mid-truncation
     * nor index bytes the truncated log no longer backs. A round is a few
     * hundred appends; truncation is a once-per-epoch event — blocking is
     * fine, interleaving is not.
     */
    private final Object truncateLock = new Object();
    /** Queue forced by the dispatcher (bench observability). */
    private volatile long indexForceCount;

    private ConsumeQueueStore(File rootDir, CommitLog commitLog) throws IOException {
        this.rootDir = rootDir;
        this.commitLog = commitLog;
        if (!rootDir.exists() && !rootDir.mkdirs()) {
            throw new IOException("cannot create consumequeue root " + rootDir);
        }
        long laggard = Long.MAX_VALUE;
        File[] topics = rootDir.listFiles();
        if (topics != null) {
            for (File t : topics) {
                if (!t.isDirectory() || !TOPIC_DIR_PATTERN.matcher(t.getName()).matches()) {
                    continue; // junk name: never index through it, replay decides
                }
                ConsumeQueue q = ConsumeQueue.open(t);
                queues.put(t.getName(), q);
                alignToLog(q);
                ConsumeQueue.Entry last = q.lastEntry();
                long cursor = last != null ? last.end() : 0L;
                cursors.put(t.getName(), cursor);
                laggard = Math.min(laggard, cursor);
            }
        }
        // Dispatch starts at the laggard queue's position: queues ahead of
        // it re-index nothing (their entries sit below their own cursor),
        // the laggard and fresh queues rebuild exactly what is missing. A
        // brand-new store starts at 0 like a laggard with nothing indexed.
        this.dispatchFrom = laggard == Long.MAX_VALUE ? 0L : laggard;
        this.dispatcher = new Thread(new Runnable() {
            @Override
            public void run() {
                dispatchLoop();
            }
        }, "mini-mq-reput");
        this.dispatcher.setDaemon(true);
    }

    /**
     * Crash alignment: drops entries the recovered CommitLog cannot back.
     * Recovery guarantees [0, maxOffset) is a CRC-valid contiguous stream, so
     * any entry whose END fits below maxOffset is backed; one that pokes past
     * it (orphaned by a CommitLog tail truncation) is dropped one by one.
     */
    private void alignToLog(ConsumeQueue q) {
        long maxOffset = commitLog.maxOffset();
        ConsumeQueue.Entry last = q.lastEntry();
        while (last != null && last.end() > maxOffset) {
            q.truncateToEntries(q.entryCount() - 1);
            last = q.lastEntry();
        }
    }

    public static ConsumeQueueStore open(File rootDir, CommitLog commitLog) throws IOException {
        ConsumeQueueStore store = new ConsumeQueueStore(rootDir, commitLog);
        store.dispatcher.start();
        return store;
    }

    /**
     * Index entries for {@code topic} at/after {@code fromPhysicalOffset} —
     * the indexed FETCH path (O(log n) locate + exact reads, versus M1's
     * full-log scan-and-filter).
     */
    public List<ConsumeQueue.Entry> readFrom(String topic, long fromPhysicalOffset, int maxCount) {
        ConsumeQueue q = queues.get(topic);
        if (q == null) {
            return Collections.emptyList();
        }
        return q.readFrom(fromPhysicalOffset, maxCount);
    }

    /** Number of queues (monitoring). */
    public int queueCount() {
        return queues.size();
    }

    /** Dispatch cursor (monitoring / tests). */
    public long dispatchedOffset() {
        return dispatchFrom;
    }

    /** Index file forces issued by the dispatcher (bench observability). */
    public long indexForceCount() {
        return indexForceCount;
    }

    /** Blocks until the dispatch cursor reaches {@code offset} or timeout. */
    public boolean awaitDispatchedTo(long offset, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (dispatchFrom < offset) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(1);
        }
        return true;
    }

    private void dispatchLoop() {
        while (running) {
            boolean progressed = false;
            try {
                synchronized (truncateLock) {
                    // Re-check INSIDE the lock: a truncation may have just
                    // moved the cursor backwards past this log end.
                    long logEnd = commitLog.maxOffset();
                    if (dispatchFrom < logEnd) {
                        List<Message> batch = commitLog.readFrom(dispatchFrom, DISPATCH_BATCH);
                        if (!batch.isEmpty()) {
                            progressed = dispatchRound(batch);
                        }
                    }
                }
            } catch (Throwable t) {
                if (running) {
                    // Keep the cursor and retry the round: index appends are
                    // idempotent at worst (a retry after a partial roll
                    // re-runs the same append; the queue is truncated-and-
                    // replayed on next open if anything did tear).
                    LOG.log(Level.WARNING, "dispatch round failed at " + dispatchFrom, t);
                }
            }
            if (!progressed) {
                park();
            }
        }
    }

    /**
     * One dispatch round — caller holds {@code truncateLock}, which is what
     * makes read-from-log + append-to-index atomic against truncation.
     *
     * @return true if the cursor advanced (caller skips parking)
     */
    private boolean dispatchRound(List<Message> batch) throws IOException {
        ConsumeQueue lastTouched = null;
        for (Message msg : batch) {
            if (!isIndexableTopic(msg.getTopic())) {
                skipUnindexable(msg);
                continue;
            }
            Long cursorBoxed = cursors.get(msg.getTopic());
            long cursor = cursorBoxed == null ? 0L : cursorBoxed.longValue();
            if (msg.getOffset() < cursor) {
                continue; // this queue already indexed past it
            }
            ConsumeQueue q = getOrCreate(msg.getTopic());
            int size = MessageCodec.sizeOf(msg);
            q.append(msg.getOffset(), size);
            cursors.put(msg.getTopic(), msg.getOffset() + size);
            lastTouched = q;
        }
        Message last = batch.get(batch.size() - 1);
        dispatchFrom = last.getOffset() + MessageCodec.sizeOf(last);
        if (lastTouched != null) {
            lastTouched.forceAll(); // who writes, forces
            indexForceCount++;
        }
        return true;
    }

    /**
     * Replication-driven truncation (M3): the leader's epoch checkpoint says
     * the local tail belongs to a superseded lineage, so both the log and
     * every derived index must rewind to {@code offset} before the current
     * leader's history is fetched over it.
     *
     * <p>
     * Ordering contract: log first, index second, all under
     * {@code truncateLock} — the dispatcher is blocked for the whole span,
     * so it can neither read a torn tail (between the two truncations) nor
     * resurrect entries for bytes that no longer exist. Afterwards the
     * cursors rewind to the laggard queue and the normal follow loop
     * replays forward — the exact startup rule, applied live.
     */
    public void truncateLogAndIndex(long offset) throws IOException {
        synchronized (truncateLock) {
            commitLog.truncateTo(offset);
            for (ConsumeQueue q : queues.values()) {
                ConsumeQueue.Entry last = q.lastEntry();
                while (last != null && last.end() > offset) {
                    q.truncateToEntries(q.entryCount() - 1);
                    last = q.lastEntry();
                }
            }
            long laggard = Long.MAX_VALUE;
            for (String topic : queues.keySet()) {
                ConsumeQueue q = queues.get(topic);
                ConsumeQueue.Entry last = q.lastEntry();
                long cursor = last != null ? last.end() : 0L;
                cursors.put(topic, cursor);
                laggard = Math.min(laggard, cursor);
            }
            dispatchFrom = laggard == Long.MAX_VALUE ? 0L : laggard;
        }
    }

    private static void park() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void skipUnindexable(Message msg) {
        if (warnedTopics.add(msg.getTopic())) {
            LOG.warning("topic '" + msg.getTopic() + "' is not filesystem-safe; its records "
                    + "stay in the CommitLog but get no index (first offender at offset "
                    + msg.getOffset() + ")");
        }
    }

    static boolean isIndexableTopic(String topic) {
        if (topic == null || topic.isEmpty() || topic.equals(".") || topic.equals("..")) {
            return false;
        }
        return TOPIC_DIR_PATTERN.matcher(topic).matches();
    }

    /** Dispatcher-thread only (single-threaded creation, lock-free reads). */
    private ConsumeQueue getOrCreate(String topic) throws IOException {
        ConsumeQueue q = queues.get(topic);
        if (q == null) {
            q = ConsumeQueue.open(new File(rootDir, topic));
            ConsumeQueue raced = queues.putIfAbsent(topic, q);
            if (raced != null) {
                q.close();
                q = raced;
            }
        }
        return q;
    }

    /** Forces every queue (shutdown path). */
    public void forceAll() {
        for (ConsumeQueue q : queues.values()) {
            q.forceAll();
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            dispatcher.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        forceAll();
        for (ConsumeQueue q : queues.values()) {
            q.close();
        }
        queues.clear();
    }
}
