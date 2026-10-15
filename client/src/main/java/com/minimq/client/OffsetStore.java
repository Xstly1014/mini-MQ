package com.minimq.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer offset store: in-memory map flushed to a properties file.
 * Persistence is atomic (write temp + rename) — a crash mid-flush leaves the
 * previous file intact, never a half-written one (same trick as big-file
 * uploads). Broker-side offset management arrives in M3; until then each
 * consumer group owns its local file.
 */
public final class OffsetStore {

    private final File file;
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<String, Long>();

    private OffsetStore(File file, ConcurrentHashMap<String, Long> offsets) {
        this.file = file;
        this.offsets.putAll(offsets);
    }

    /** Loads the store; a missing file starts empty (consume from the head). */
    public static OffsetStore load(File file) {
        ConcurrentHashMap<String, Long> loaded = new ConcurrentHashMap<String, Long>();
        if (file.exists()) {
            Properties props = new Properties();
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                props.load(in);
                for (String name : props.stringPropertyNames()) {
                    loaded.put(name, Long.parseLong(props.getProperty(name).trim()));
                }
            } catch (IOException e) {
                throw new IllegalStateException("cannot read offset store " + file, e);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("corrupt offset store " + file, e);
            } finally {
                closeQuietly(in);
            }
        }
        return new OffsetStore(file, loaded);
    }

    /** Next offset to consume for the topic; 0 = from the head. */
    public long get(String topic) {
        Long v = offsets.get(topic);
        return v == null ? 0L : v;
    }

    public void update(String topic, long offset) {
        offsets.put(topic, offset);
    }

    /** Atomic flush: temp file in the SAME directory + rename. */
    public synchronized void flush() throws IOException {
        Properties props = new Properties();
        for (java.util.Map.Entry<String, Long> e : offsets.entrySet()) {
            props.setProperty(e.getKey(), Long.toString(e.getValue()));
        }
        File dir = file.getParentFile();
        if (dir != null && !dir.exists() && !dir.mkdirs()) {
            throw new IOException("cannot create offset dir " + dir);
        }
        File tmp = new File(dir, file.getName() + ".tmp");
        OutputStream out = null;
        try {
            out = new FileOutputStream(tmp);
            props.store(out, "mini-MQ consumer offsets (one file per consumer group)");
        } finally {
            closeQuietly(out);
        }
        // Atomic on both POSIX and NTFS when source and target share a volume.
        Files.move(tmp.toPath(), file.toPath(),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
