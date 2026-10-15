package com.minimq.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The single sequential write-ahead log: a chain of mmap segments, each named
 * by its starting global offset (20-digit zero-padded). Appends roll to the
 * next pre-allocated segment when the current one cannot fit the record.
 *
 * Opening an existing store runs {@link Recovery} first: dirty tails (torn
 * writes / CRC failures) are truncated in place, so the log is always a
 * contiguous stream of CRC-valid records after open.
 *
 * Concurrency: v0 is single-writer by design (mmap page-cache writes from one
 * broker thread); M1 adds group-commit + synchronization.
 */
public class CommitLog implements Closeable {

    public static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024; // 1 GiB

    private final File dir;
    private final int segmentSize;
    private final List<MappedFile> segments = new ArrayList<MappedFile>();
    private MappedFile current;
    private long maxOffset;

    private CommitLog(File dir, int segmentSize) throws IOException {
        this.dir = dir;
        this.segmentSize = segmentSize;
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("cannot create commitlog dir " + dir);
        }
        Recovery.Result result = Recovery.recover(dir, segmentSize);
        if (result.segments.isEmpty()) {
            this.current = new MappedFile(new File(dir, MappedFile.fileName(0L)), segmentSize);
            this.segments.add(current);
            this.maxOffset = 0L;
        } else {
            this.segments.addAll(result.segments);
            this.current = result.lastSegment;
            this.maxOffset = result.maxOffset;
        }
    }

    public static CommitLog open(File dir) throws IOException {
        return open(dir, DEFAULT_SEGMENT_SIZE);
    }

    public static CommitLog open(File dir, int segmentSize) throws IOException {
        return new CommitLog(dir, segmentSize);
    }

    /**
     * Appends one message and returns its global physical offset (also written
     * back into the message). A record larger than a whole segment is rejected.
     */
    public long append(Message msg) throws IOException {
        byte[] encoded = MessageCodec.encode(msg);
        return appendInternal(encoded, msg);
    }

    /**
     * Appends pre-encoded bytes without any offset write-back. Test hook for
     * injecting torn/corrupt tails, and used by the bench harness.
     */
    public long appendRaw(byte[] encoded) throws IOException {
        return appendInternal(encoded, null);
    }

    private long appendInternal(byte[] encoded, Message msg) throws IOException {
        if (encoded.length > segmentSize) {
            throw new IOException("record " + encoded.length
                    + "B exceeds segment size " + segmentSize + "B");
        }
        if (current.free() < encoded.length) {
            roll();
        }
        int pos = current.append(encoded, 0, encoded.length);
        if (pos < 0) {
            throw new IOException("append failed unexpectedly in segment " + current.getFile());
        }
        long global = current.fileFromOffset() + pos;
        if (msg != null) {
            msg.setOffset(global);
        }
        return global;
    }

    private void roll() throws IOException {
        current.flush(); // seal the previous segment before moving on
        MappedFile next = new MappedFile(
                new File(dir, MappedFile.fileName(current.fileFromOffset() + segmentSize)),
                segmentSize);
        segments.add(next);
        current = next;
    }

    /** Flushes dirty pages of the active segment (msync semantics). */
    public void flush() {
        current.flush();
    }

    /** Global offset just past the last appended (or recovered) record. */
    public long maxOffset() {
        return current.fileFromOffset() + current.wrotePosition();
    }

    public int segmentCount() {
        return segments.size();
    }

    /** Read-only snapshot of the segment file names, oldest first. */
    public List<String> segmentNames() {
        List<String> names = new ArrayList<String>(segments.size());
        for (MappedFile mf : segments) {
            names.add(mf.getFile().getName());
        }
        return names;
    }

    MappedFile currentSegment() {
        return current;
    }

    /**
     * Iterates all valid records from the beginning of the log. A CORRUPT or
     * INCOMPLETE record after a successful open indicates a bug: it fails fast.
     */
    public Iterator<Message> iterator() {
        final List<MappedFile> snapshot = new ArrayList<MappedFile>(segments);
        return new Iterator<Message>() {
            private int segIdx;
            private int pos;
            private Message next = advance();

            private Message advance() {
                while (segIdx < snapshot.size()) {
                    MappedFile seg = snapshot.get(segIdx);
                    if (pos >= seg.wrotePosition()) {
                        segIdx++;
                        pos = 0;
                        continue;
                    }
                    MessageCodec.DecodeResult r = MessageCodec.decode(
                            seg.slice(pos, seg.wrotePosition()), 0);
                    switch (r.status) {
                        case OK:
                            Message msg = r.message;
                            msg.setOffset(seg.fileFromOffset() + pos);
                            pos += r.size;
                            return msg;
                        case EMPTY:
                            // Zero hole at the end of a rolled segment: normal.
                            segIdx++;
                            pos = 0;
                            continue;
                        case INCOMPLETE:
                            // Tail hole shorter than one record at a rolled
                            // segment's end: the segment was sealed with a few
                            // unfilled trailing bytes. Skip to the next segment.
                            segIdx++;
                            pos = 0;
                            continue;
                        case CORRUPT:
                        default:
                            throw new IllegalStateException("corrupt record at global offset "
                                    + (seg.fileFromOffset() + pos) + " in " + seg.getFile());
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Message next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Message result = next;
                next = advance();
                return result;
            }
        };
    }

    @Override
    public void close() throws IOException {
        for (MappedFile mf : segments) {
            mf.close();
        }
        segments.clear();
    }
}
