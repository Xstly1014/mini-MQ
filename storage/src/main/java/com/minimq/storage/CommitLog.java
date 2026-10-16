package com.minimq.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
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
 * Concurrency (M2): appends from multiple business-pool threads serialize on
 * {@code appendLock} (the mmap position/pointer dance is not thread safe —
 * this also fixed a latent M1 race that single-connection tests never hit).
 * Durability coordination lives on {@code flushLock}: {@link FlushService}
 * advances {@code flushedOffset} after each force and wakes SYNC-mode
 * producers parked in {@link #awaitDurable}. Lock order is strictly
 * appendLock -> flushLock; the flush thread never takes appendLock.
 */
public class CommitLog implements Closeable {

    public static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024; // 1 GiB

    private final File dir;
    private final int segmentSize;
    private final List<MappedFile> segments = new ArrayList<MappedFile>();

    /** Guards segments/current/appendFrontier/appendedCount (the write path). */
    final Object appendLock = new Object();
    /** Guards flushedOffset/flushedSegmentCount + SYNC waiters. */
    final Object flushLock = new Object();

    private MappedFile current;

    /**
     * Global offset just past the last appended byte; volatile for lock-free reads.
     */
    private volatile long appendFrontier;
    /**
     * Total records ever appended (aggregation counter for the flush batch
     * trigger). Package-private: {@link FlushService} polls it.
     */
    volatile long appendedCount;

    /**
     * Durable frontier: every byte below it survived a completed force. Unlike
     * a "flushed record count" this is monotonic in BYTES, so a force can
     * never claim records that were appended during it (the M2-4a race: a
     * count snapshot taken after the force mis-marked in-flight appends as
     * durable, the next sweep saw pending=0 and SYNC producers timed out).
     */
    private volatile long flushedOffset;

    /** Number of leading segments already forced (skip list for force sweeps). */
    private volatile int flushedSegmentCount;

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
            this.appendFrontier = 0L;
        } else {
            this.segments.addAll(result.segments);
            this.current = result.lastSegment;
            this.appendFrontier = result.maxOffset;
        }
        // Whatever recovery accepted came from the file on disk, i.e. it
        // survived the last crash: the durable frontier starts at the log end.
        this.flushedOffset = this.appendFrontier;
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
     *
     * <p>
     * Encoding goes through the thread-local scratch buffer
     * ({@link CodecPools}): the lease is this stack frame, so no explicit
     * recycle exists to get wrong.
     */
    public long append(Message msg) throws IOException {
        byte[] buf = CodecPools.borrowEncodeBuffer(0);
        int total = MessageCodec.encodeInto(msg, buf);
        if (total < 0) {
            buf = CodecPools.borrowEncodeBuffer(-total);
            total = MessageCodec.encodeInto(msg, buf);
            if (total < 0) {
                throw new IllegalStateException("scratch buffer too small after grow");
            }
        }
        return appendInternal(buf, total, msg);
    }

    /**
     * Appends pre-encoded bytes without any offset write-back. Test hook for
     * injecting torn/corrupt tails, and used by the bench harness.
     */
    public long appendRaw(byte[] encoded) throws IOException {
        return appendInternal(encoded, encoded.length, null);
    }

    /** length is authoritative: buf may be a larger pooled scratch array. */
    private long appendInternal(byte[] encoded, int length, Message msg) throws IOException {
        if (length > segmentSize) {
            throw new IOException("record " + length
                    + "B exceeds segment size " + segmentSize + "B");
        }
        synchronized (appendLock) {
            if (current.free() < length) {
                roll();
            }
            int pos = current.append(encoded, 0, length);
            if (pos < 0) {
                throw new IOException("append failed unexpectedly in segment " + current.getFile());
            }
            long global = current.fileFromOffset() + pos;
            appendFrontier = global + length;
            appendedCount++;
            if (msg != null) {
                msg.setOffset(global);
            }
            return global;
        }
    }

    private void roll() throws IOException {
        current.flush(); // seal the previous segment before moving on
        MappedFile next = new MappedFile(
                new File(dir, MappedFile.fileName(current.fileFromOffset() + segmentSize)),
                segmentSize);
        segments.add(next);
        current = next;
        int sealedIdx = segments.size() - 1;
        // The seal-force above made the old segment durable: let force sweeps
        // skip it (flushLock is safe to take here — lock order append->flush).
        synchronized (flushLock) {
            if (flushedSegmentCount < sealedIdx) {
                flushedSegmentCount = sealedIdx;
            }
        }
    }

    /**
     * Parks the caller until bytes below {@code endOffset} are on disk, i.e.
     * the SYNC_FLUSH ack gate (MySQL Follower semantics). Returns false on
     * timeout — the caller must answer ERROR, never a lying OK.
     */
    public boolean awaitDurable(long endOffset, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        synchronized (flushLock) {
            while (flushedOffset < endOffset) {
                long remainingNs = deadline - System.nanoTime();
                if (remainingNs <= 0) {
                    return false;
                }
                long remainingMs = remainingNs / 1_000_000L;
                flushLock.wait(Math.max(remainingMs, 1L), (int) (remainingNs % 1_000_000L));
            }
            return true;
        }
    }

    /** Current durable frontier (monotonic; advances only after a real force). */
    public long flushedOffset() {
        return flushedOffset;
    }

    /**
     * Called by {@link FlushService} (and close paths) after a force sweep.
     * Advances only the offset frontier — never a record counter: the flusher
     * tracks its own count, so a count written here could include appends
     * that landed during the force (the M2-4a race).
     */
    void onFlushed(long frontier) {
        synchronized (flushLock) {
            if (frontier > flushedOffset) {
                flushedOffset = frontier;
            }
            flushLock.notifyAll();
        }
    }

    /**
     * Forces every mapped segment up to and including the active one. Safe to
     * run concurrently with appends (page-cache force is page-granular); the
     * caller advances flushedOffset separately so bytes appended DURING the
     * force are never claimed durable.
     */
    public void forceSegmentsUpToCurrent() {
        List<MappedFile> snapshot;
        int from;
        synchronized (appendLock) {
            snapshot = new ArrayList<MappedFile>(segments);
        }
        from = flushedSegmentCount;
        for (int i = from; i < snapshot.size(); i++) {
            snapshot.get(i).flush();
        }
        flushedSegmentCount = snapshot.size();
    }

    /** Read-only segment snapshot (bench/monitoring). */
    public List<MappedFile> segmentSnapshot() {
        synchronized (appendLock) {
            return new ArrayList<MappedFile>(segments);
        }
    }

    /**
     * Reads up to {@code maxCount} valid records starting at global offset
     * {@code fromOffset} (the FETCH consume path). Segments are contiguous
     * (each starts at a multiple of segmentSize), so the starting segment is
     * located arithmetically. A tail hole (EMPTY / INCOMPLETE at a sealed
     * segment's end) skips to the next segment, mirroring {@link #iterator}.
     *
     * @throws IllegalStateException on a CORRUPT record — recovery truncated
     *                               the tail at open, so reaching one means on-disk
     *                               damage after
     *                               open or a bug in the write path.
     */
    public List<Message> readFrom(long fromOffset, int maxCount) {
        if (maxCount <= 0 || fromOffset < 0) {
            return new ArrayList<Message>();
        }
        List<Message> out = new ArrayList<Message>(Math.min(maxCount, 256));
        int idx = (int) (fromOffset / segmentSize);
        if (idx >= segments.size()) {
            return out;
        }
        while (idx < segments.size() && out.size() < maxCount) {
            MappedFile seg = segments.get(idx);
            // Range check, NOT idx*segmentSize: that int product overflows at
            // global offset 2^31 (segment 8 x 256MB), silently reset pos to 0
            // and the dispatch cursor spun on the segment head forever (the
            // soak 2GB bug — only a >2GB store can ever hit it).
            int pos = (seg.fileFromOffset() <= fromOffset
                    && fromOffset < seg.fileFromOffset() + (long) seg.fileSize())
                            ? (int) (fromOffset - seg.fileFromOffset())
                            : 0;
            pos = (int) Math.min(pos, seg.fileSize());
            boolean skipToNext = false;
            while (out.size() < maxCount && pos <= seg.wrotePosition() - MessageCodec.HEADER_SIZE) {
                MessageCodec.DecodeResult r = MessageCodec.decode(
                        seg.slice(pos, seg.wrotePosition()), 0);
                switch (r.status) {
                    case OK:
                        Message msg = r.message;
                        msg.setOffset(seg.fileFromOffset() + pos);
                        out.add(msg);
                        pos += r.size;
                        break;
                    case EMPTY:
                    case INCOMPLETE:
                        // tail hole of a sealed segment: continue in the next one
                        skipToNext = true;
                        break;
                    case CORRUPT:
                    default:
                        throw new IllegalStateException("corrupt record at global offset "
                                + (seg.fileFromOffset() + pos) + " in " + seg.getFile());
                }
                if (skipToNext) {
                    break;
                }
            }
            idx++;
        }
        return out;
    }

    /**
     * Header-only decode at a record boundary (the zero-copy FETCH path
     * builds its response meta from this; the payload never enters user
     * space on the broker). Statuses mirror {@link MessageCodec#decode}:
     * anything but OK tells the caller to skip the entry.
     */
    public MessageCodec.HeaderResult decodeHeaderAt(long offset) {
        List<MappedFile> snapshot = segmentSnapshot();
        int idx = (int) (offset / segmentSize);
        if (offset < 0 || idx < 0 || idx >= snapshot.size()) {
            return new MessageCodec.HeaderResult(
                    MessageCodec.DecodeStatus.CORRUPT, 0, 0, 0, null);
        }
        MappedFile seg = snapshot.get(idx);
        if (seg.fileFromOffset() > offset) {
            return new MessageCodec.HeaderResult(
                    MessageCodec.DecodeStatus.CORRUPT, 0, 0, 0, null);
        }
        int pos = (int) (offset - seg.fileFromOffset());
        if (pos + MessageCodec.HEADER_SIZE > seg.wrotePosition()) {
            return new MessageCodec.HeaderResult(
                    MessageCodec.DecodeStatus.INCOMPLETE, 0, 0, 0, null);
        }
        return MessageCodec.decodeHeader(seg.slice(pos, seg.wrotePosition()), 0);
    }

    /**
     * Slices {@code [startOffset, endOffset)} into per-segment
     * {@link FileSlice}s for the zero-copy FETCH path — the bytes go from
     * the page cache straight to the socket, no user-space copy. The span
     * may include foreign-topic records and segment tail holes between the
     * requested records; the response meta carries the exact offsets so the
     * consumer decodes only its own records.
     *
     * The slices borrow this log's live file channels: consume them (or the
     * response built on them) before closing the log.
     */
    public List<FileSlice> fileSlicesFor(long startOffset, long endOffset) {
        List<MappedFile> snapshot = segmentSnapshot();
        List<FileSlice> slices = new ArrayList<FileSlice>(2);
        for (MappedFile seg : snapshot) {
            long segEnd = seg.fileFromOffset() + seg.fileSize();
            long from = Math.max(startOffset, seg.fileFromOffset());
            long to = Math.min(endOffset, segEnd);
            if (to > from) {
                slices.add(new FileSlice(seg.channel(),
                        from - seg.fileFromOffset(), to - from));
            }
            if (segEnd >= endOffset) {
                break;
            }
        }
        return slices;
    }

    /** A contiguous byte range of one segment file (zero-copy transfer unit). */
    public static final class FileSlice {
        public final FileChannel channel;
        public final long position;
        public final long count;

        public FileSlice(FileChannel channel, long position, long count) {
            this.channel = channel;
            this.position = position;
            this.count = count;
        }
    }

    /**
     * Forces all dirty pages up to the current write frontier and marks them
     * durable (shutdown path; the flush thread does the same periodically).
     */
    public void flush() {
        forceSegmentsUpToCurrent();
        onFlushed(appendFrontier);
    }

    /** Global offset just past the last appended (or recovered) record. */
    public long maxOffset() {
        return appendFrontier;
    }

    /** Frontier read while already holding flushLock (FlushService only). */
    long maxOffsetUnlocked() {
        return appendFrontier;
    }

    public int segmentCount() {
        synchronized (appendLock) {
            return segments.size();
        }
    }

    /** Read-only snapshot of the segment file names, oldest first. */
    public List<String> segmentNames() {
        synchronized (appendLock) {
            List<String> names = new ArrayList<String>(segments.size());
            for (MappedFile mf : segments) {
                names.add(mf.getFile().getName());
            }
            return names;
        }
    }

    MappedFile currentSegment() {
        synchronized (appendLock) {
            return current;
        }
    }

    /**
     * Iterates all valid records from the beginning of the log. A CORRUPT or
     * INCOMPLETE record after a successful open indicates a bug: it fails fast.
     */
    public Iterator<Message> iterator() {
        final List<MappedFile> snapshot = segmentSnapshot();
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
        synchronized (appendLock) {
            for (MappedFile mf : segments) {
                mf.close();
            }
            segments.clear();
        }
    }
}
