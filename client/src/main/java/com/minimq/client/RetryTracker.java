package com.minimq.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-record failure counter, persisted as a sidecar next to the offsets
 * (M4 dead-letter machinery): the retry count of a poison record must
 * survive a consumer restart, or a crash-looping consumer never exhausts
 * its retries and never dead-letters the record.
 *
 * <p>
 * Keyed by {@code topic@offset} — the offset is the record's stable
 * identity in the log (seq keys are not carried on the wire, only hashes).
 * Entries exist ONLY for records that failed at least once, so the file
 * stays tiny no matter how large the stream is; {@link #pruneBelow}
 * removes counters for records the group has successfully moved past
 * (a record that succeeded on retry 2 stops paying the sidecar tax).
 *
 * <p>
 * Persistence discipline copies {@link OffsetStore}: atomic write via
 * temp file + rename, one file per consumer group.
 */
final class RetryTracker {

  private final File file;
  private final ConcurrentHashMap<String, Integer> attempts = new ConcurrentHashMap<String, Integer>();

  private RetryTracker(File file, ConcurrentHashMap<String, Integer> loaded) {
    this.file = file;
    this.attempts.putAll(loaded);
  }

  /**
   * Loads the sidecar; a missing file starts empty (no record has failed
   * yet). {@code file == null} keeps the tracker purely in-memory — the
   * shape unit tests want.
   */
  static RetryTracker load(File file) {
    ConcurrentHashMap<String, Integer> loaded = new ConcurrentHashMap<String, Integer>();
    if (file != null && file.exists()) {
      Properties props = new Properties();
      InputStream in = null;
      try {
        in = new FileInputStream(file);
        props.load(in);
        for (String name : props.stringPropertyNames()) {
          loaded.put(name, Integer.parseInt(props.getProperty(name).trim()));
        }
      } catch (IOException e) {
        throw new IllegalStateException("cannot read retry tracker " + file, e);
      } catch (NumberFormatException e) {
        throw new IllegalStateException("corrupt retry tracker " + file, e);
      } finally {
        if (in != null) {
          try {
            in.close();
          } catch (IOException ignored) {
            // best effort
          }
        }
      }
    }
    return new RetryTracker(file, loaded);
  }

  /** Failure count so far for this record, 0 = never failed. */
  int attempts(String topic, long offset) {
    Integer v = attempts.get(key(topic, offset));
    return v == null ? 0 : v.intValue();
  }

  /**
   * Registers one more failure and returns the NEW count (1 = first
   * failure). In-memory immediately; {@link #flush()} persists.
   */
  int bump(String topic, long offset) {
    String k = key(topic, offset);
    for (;;) {
      Integer prev = attempts.get(k);
      if (prev == null) {
        if (attempts.putIfAbsent(k, Integer.valueOf(1)) == null) {
          return 1;
        }
      } else {
        Integer next = Integer.valueOf(prev.intValue() + 1);
        if (attempts.replace(k, prev, next)) {
          return next.intValue();
        }
      }
    }
  }

  /** Drops the counter (record finally succeeded, or dead-lettered). */
  void clear(String topic, long offset) {
    attempts.remove(key(topic, offset));
  }

  /**
   * Drops counters for records the group has moved past: a success after
   * retries must not leave stale entries (they would misclassify a
   * legitimate re-failure of a REUSED offset after log truncation — rare,
   * but the prune makes the semantics airtight regardless).
   */
  void pruneBelow(String topic, long offset) {
    String prefix = topic + "@";
    for (Map.Entry<String, Integer> e : attempts.entrySet()) {
      String k = e.getKey();
      if (!k.startsWith(prefix)) {
        continue;
      }
      try {
        if (Long.parseLong(k.substring(prefix.length())) < offset) {
          attempts.remove(k);
        }
      } catch (NumberFormatException ignored) {
        // foreign key shape; not ours to prune
      }
    }
  }

  /** Atomic flush: temp file + rename, the OffsetStore trick. */
  synchronized void flush() throws IOException {
    if (file == null) {
      return; // in-memory flavor
    }
    Properties props = new Properties();
    for (Map.Entry<String, Integer> e : attempts.entrySet()) {
      props.setProperty(e.getKey(), Integer.toString(e.getValue().intValue()));
    }
    File dir = file.getParentFile();
    if (dir != null && !dir.exists() && !dir.mkdirs()) {
      throw new IOException("cannot create retry tracker dir " + dir);
    }
    File tmp = new File(dir, file.getName() + ".tmp");
    OutputStream out = null;
    try {
      out = new FileOutputStream(tmp);
      props.store(out, "mini-MQ consumer retry counts (dead-letter sidecar)");
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException ignored) {
          // best effort
        }
      }
    }
    Files.move(tmp.toPath(), file.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
  }

  private static String key(String topic, long offset) {
    return topic + "@" + offset;
  }
}
