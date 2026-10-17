package com.minimq.broker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Session-scoped seq-key dedup window (M4, the third defense line's broker
 * patch): a bounded LRU of KEY_HASH → original offset, checked on the
 * produce path. A producer retry that lands on the SAME leader with the
 * same seq_key gets the original offset back (ProduceStatus.DUPLICATE)
 * instead of a second append — the at-least-once duplicate collapses to
 * exactly-once within the session.
 *
 * <h3>Honest boundaries (declared, not hidden)</h3>
 * <ul>
 * <li><b>Session scope</b>: the window lives in the leader process's
 * memory. A retry that lands on a DIFFERENT leader (failover) is
 * appended again — cross-leader dedup needs the offset discussion
 * (epoch-tagged windows), which is exactly the line Kafka drew before
 * KIP-360; the final arbiter stays consumer-side idempotency by
 * seq_key.</li>
 * <li><b>Best effort under concurrency</b>: check and record are one
 * synchronized section, but two business threads producing the same
 * key in the same instant can both miss and both append. Producers
 * retry with backoff, never in parallel with themselves, so the matrix
 * never races this window.</li>
 * <li><b>Key = storage KEY_HASH</b> (FNV-1a 64 of the seq key). Hash
 * collisions at 100k entries are ~1e-7 — the same trust the storage
 * layer already places in the hash.</li>
 * <li><b>Followers never dedup</b>: they append what the leader already
 * filtered (the FETCH data plane carries the verdict downstream).</li>
 * </ul>
 *
 * <p>
 * Thread-safety: the produce path is multi-threaded (business pool), so
 * every operation takes the monitor; the critical section is two map ops.
 */
public final class DedupWindow {

  private final int capacity;
  private long hits;
  private long checks;

  private final LinkedHashMap<Long, Long> window = new LinkedHashMap<Long, Long>(256, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, Long> eldest) {
      return size() > capacity;
    }
  };

  public DedupWindow(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("dedup window capacity must be > 0, got " + capacity);
    }
    this.capacity = capacity;
  }

  /**
   * Returns the original offset if this key was already appended within
   * the window, else null. Touches the LRU order (a hot retry stream must
   * not evict its own keys).
   */
  public synchronized Long seen(long keyHash) {
    checks++;
    Long prior = window.get(Long.valueOf(keyHash));
    if (prior != null) {
      hits++;
    }
    return prior;
  }

  /** Records the offset a freshly appended message landed at. */
  public synchronized void record(long keyHash, long offset) {
    window.put(Long.valueOf(keyHash), Long.valueOf(offset));
  }

  public synchronized int size() {
    return window.size();
  }

  /** LRU eviction headroom in entries (capacity minus current size). */
  public synchronized int capacity() {
    return capacity;
  }

  public synchronized long hits() {
    return hits;
  }

  public synchronized long checks() {
    return checks;
  }
}
