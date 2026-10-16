package com.minimq.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Per-topic consume index (M2): a chain of mmap files of fixed-size entries,
 * RocketMQ ConsumeQueue style — the "metadata small, data huge" separation
 * that both kills the M1 CommitLog scan and makes the fetch path zero-copy
 * friendly.
 *
 * Entry layout (little-endian, 12 B):
 *
 * <pre>
 * | COMMITLOG_OFFSET | 8B | global physical offset of the record |
 * | SIZE             | 4B | encoded record size                   |
 * </pre>
 *
 * File layout: {store}/consumequeue/{topic}/{startIndex}, a 20-digit entry
 * index — each file holds {@link #ENTRIES_PER_FILE} entries (6 MiB). Entries
 * are strictly ascending in COMMITLOG_OFFSET (the CommitLog append order),
 * which is what the binary search in {@link #readFrom} relies on.
 *
 * Recovery on open: entry positions are in-memory state, so each file is
 * re-scanned (valid entry = size &gt; 0, offset ascending; the pre-allocated
 * untouched region reads as zeros). File-chain continuity is enforced —
 * file(i+1) must start exactly where file(i) ends; a broken chain drops the
 * tail, and {@link ConsumeQueueStore} replays the CommitLog to rebuild it.
 * The queue is derived data: a crash may truncate entries, never the log.
 */
public final class ConsumeQueue implements Closeable {

    public static final int ENTRY_SIZE = 12;
    public static final int ENTRIES_PER_FILE = 500_000; // 6 MiB per file
    public static final int FILE_SIZE = ENTRY_SIZE * ENTRIES_PER_FILE;

    /** Immutable index entry. */
    public static final class Entry {
        public final long offset;
        public final int size;

        public Entry(long offset, int size) {
            this.offset = offset;
            this.size = size;
        }

        public long end() {
            return offset + size;
        }
    }

    private final File dir;
    private final List<MappedFile> files = new ArrayList<MappedFile>();
    /** First entry's physical offset per file — the outer search level. */
    private final List<Long> fileFirstOffsets = new ArrayList<Long>();
    private final Object appendLock = new Object();
    private MappedFile current;
    private long entryCount;

    private ConsumeQueue(File dir) throws IOException {
        this.dir = dir;
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("cannot create consumequeue dir " + dir);
        }
        File[] existing = dir.listFiles();
        if (existing != null && existing.length > 0) {
            Arrays.sort(existing, new Comparator<File>() {
                @Override
                public int compare(File a, File b) {
                    return Long.compare(parseIndex(a.getName()), parseIndex(b.getName()));
                }
            });
            long expectedNext = 0L; // chain-continuity cursor
            for (File f : existing) {
                long startIndex = parseIndex(f.getName());
                if (startIndex < 0 || (files.isEmpty() ? startIndex != 0 : startIndex != expectedNext)) {
                    deleteFileAndTail(f, existing);
                    break; // chain broken (or junk name): drop this file and the rest
                }
                MappedFile mf;
                try {
                    mf = new MappedFile(f, FILE_SIZE);
                } catch (IOException e) {
                    deleteFileAndTail(f, existing);
                    break;
                }
                int entries = scanValidEntries(mf);
                mf.setWrotePosition(entries * ENTRY_SIZE);
                files.add(mf);
                fileFirstOffsets.add(entries > 0 ? entryOffsetAt(mf, 0) : -1L);
                expectedNext = startIndex + entries;
            }
            if (!files.isEmpty()) {
                current = files.get(files.size() - 1);
                entryCount = expectedNext;
            }
        }
        if (files.isEmpty()) {
            roll(0L);
            entryCount = 0;
        }
    }

    /** Drops a broken/unlinked file plus everything after it in the chain. */
    private static void deleteFileAndTail(File from, File[] all) {
        for (File f : all) {
            if (parseIndex(f.getName()) >= parseIndex(from.getName())) {
                f.delete(); // best effort; a leftover is re-validated on next open
            }
        }
    }

    /**
     * Counts leading valid entries: size in (0, MAX_MESSAGE_SIZE] and offsets
     * strictly ascending. The untouched pre-allocated region reads size==0.
     */
    private static int scanValidEntries(MappedFile mf) {
        int entries = 0;
        long prevOffset = -1L;
        while (entries * ENTRY_SIZE + ENTRY_SIZE <= mf.fileSize()) {
            ByteBuffer b = mf.slice(entries * ENTRY_SIZE, (entries + 1) * ENTRY_SIZE);
            long off = b.getLong(0);
            int size = b.getInt(8);
            if (size <= 0 || size > MessageCodec.MAX_MESSAGE_SIZE) {
                break; // untouched zeros or garbage size
            }
            if (off <= prevOffset) {
                break; // must ascend with the CommitLog order
            }
            prevOffset = off;
            entries++;
        }
        return entries;
    }

    public static ConsumeQueue open(File dir) throws IOException {
        return new ConsumeQueue(dir);
    }

    private static long parseIndex(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void roll(long startIndex) throws IOException {
        MappedFile next = new MappedFile(
                new File(dir, String.format("%020d", startIndex)), FILE_SIZE);
        files.add(next);
        fileFirstOffsets.add(-1L); // empty sentinel, set on first append
        current = next;
    }

    /**
     * Appends one entry. Synchronized per-queue — different topics index in
     * parallel. Offset order is guaranteed by the caller (business threads
     * index their record right after the log append, under the log's
     * append-lock ordering).
     */
    public void append(long commitLogOffset, int size) throws IOException {
        byte[] entry = new byte[ENTRY_SIZE];
        ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(commitLogOffset).putInt(size);
        synchronized (appendLock) {
            if (current.free() < ENTRY_SIZE) {
                roll(parseIndex(current.getFile().getName()) + ENTRIES_PER_FILE);
            }
            current.append(entry, 0, ENTRY_SIZE);
            // First entry into a file sets its first-offset key (roll adds a
            // -1 sentinel; a file recovered EMPTY from disk still has one).
            int last = files.size() - 1;
            if (fileFirstOffsets.get(last) < 0L) {
                fileFirstOffsets.set(last, commitLogOffset);
            }
            entryCount++;
        }
    }

    /**
     * Reads up to {@code maxCount} entries whose physical offset is &gt;=
     * {@code fromPhysicalOffset}. Two-level search: file by first-offset
     * (ascending), then binary search inside the file. Empty result means
     * "nothing more for this topic at/after the offset".
     */
    public List<Entry> readFrom(long fromPhysicalOffset, int maxCount) {
        List<Entry> out = new ArrayList<Entry>(Math.min(maxCount, 64));
        if (maxCount <= 0) {
            return out;
        }
        synchronized (appendLock) {
            // Outer level: last file whose first offset <= from (files hold
            // 500K entries each, so a linear walk over files is nothing).
            int fileIdx = -1;
            for (int i = 0; i < files.size(); i++) {
                long first = fileFirstOffsets.get(i);
                if (first >= 0 && first <= fromPhysicalOffset) {
                    fileIdx = i;
                } else if (first > fromPhysicalOffset) {
                    break;
                }
            }
            if (fileIdx < 0) {
                fileIdx = firstNonEmptyFile(); // before every entry: start at the oldest
            }
            if (fileIdx < 0) {
                return out; // nothing indexed at all
            }
            for (int fi = fileIdx; fi < files.size() && out.size() < maxCount; fi++) {
                MappedFile mf = files.get(fi);
                int entries = mf.wrotePosition() / ENTRY_SIZE;
                int start = 0;
                if (fi == fileIdx) {
                    // binary search: first entry with offset >= from
                    int lo = 0, hi = entries - 1, found = -1;
                    while (lo <= hi) {
                        int mid = (lo + hi) >>> 1;
                        if (entryOffsetAt(mf, mid) >= fromPhysicalOffset) {
                            found = mid;
                            hi = mid - 1;
                        } else {
                            lo = mid + 1;
                        }
                    }
                    if (found < 0) {
                        continue; // all entries in this file precede `from`
                    }
                    start = found;
                }
                for (int e = start; e < entries && out.size() < maxCount; e++) {
                    ByteBuffer b = mf.slice(e * ENTRY_SIZE, (e + 1) * ENTRY_SIZE);
                    out.add(new Entry(b.getLong(0), b.getInt(8)));
                }
            }
        }
        return out;
    }

    private int firstNonEmptyFile() {
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).wrotePosition() >= ENTRY_SIZE) {
                return i;
            }
        }
        return -1;
    }

    private static long entryOffsetAt(MappedFile mf, int entryIndex) {
        return mf.slice(entryIndex * ENTRY_SIZE, (entryIndex + 1) * ENTRY_SIZE).getLong(0);
    }

    /** Total entries indexed (replay-alignment support). */
    public long entryCount() {
        synchronized (appendLock) {
            return entryCount;
        }
    }

    /** Last indexed entry, or null when empty (tail-alignment support). */
    public Entry lastEntry() {
        synchronized (appendLock) {
            if (entryCount == 0) {
                return null;
            }
            MappedFile mf = files.get(files.size() - 1);
            int last = mf.wrotePosition() / ENTRY_SIZE - 1;
            ByteBuffer b = mf.slice(last * ENTRY_SIZE, (last + 1) * ENTRY_SIZE);
            return new Entry(b.getLong(0), b.getInt(8));
        }
    }

    /**
     * Drops entries past {@code keepEntries}: recovery alignment — entries
     * not validated against the CommitLog are discarded and replayed by the
     * store. Whole files beyond the cut are deleted from disk.
     */
    public void truncateToEntries(long keepEntries) {
        synchronized (appendLock) {
            if (keepEntries >= entryCount) {
                return;
            }
            while (files.size() > 1) {
                MappedFile last = files.get(files.size() - 1);
                if (parseIndex(last.getFile().getName()) >= keepEntries) {
                    last.close();
                    files.remove(files.size() - 1);
                    fileFirstOffsets.remove(files.size());
                    last.getFile().delete();
                    current = files.get(files.size() - 1);
                } else {
                    break;
                }
            }
            long startIndex = parseIndex(current.getFile().getName());
            int posInFile = (int) (keepEntries - startIndex) * ENTRY_SIZE;
            current.setWrotePosition(posInFile);
            entryCount = keepEntries;
            MappedFile tail = files.get(files.size() - 1);
            fileFirstOffsets.set(files.size() - 1,
                    tail.wrotePosition() >= ENTRY_SIZE ? entryOffsetAt(tail, 0) : -1L);
        }
    }

    /** Forces every file (FlushService covers the index together with the log). */
    public void forceAll() {
        synchronized (appendLock) {
            for (MappedFile mf : files) {
                mf.flush();
            }
        }
    }

    public File getDir() {
        return dir;
    }

    @Override
    public void close() {
        synchronized (appendLock) {
            for (MappedFile mf : files) {
                mf.close();
            }
            files.clear();
        }
    }
}
