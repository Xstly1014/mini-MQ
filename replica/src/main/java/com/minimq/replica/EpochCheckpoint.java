package com.minimq.replica;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * The leader-epoch checkpoint (M3): the table of {@code (epoch, startOffset)}
 * reign boundaries — the arbitration data that makes recovery safe where a
 * bare high watermark is not (KIP-101).
 *
 * <p>
 * The hole in HW-only recovery: a stale leader, revived after a partition,
 * carries records beyond the HW that the current leader's history does not
 * have. Both logs "look fine"; whoever overwrites whom silently picks a
 * lineage. The epoch table closes it with authority instead of negotiation:
 * every epoch OWNS a byte range, and a range's owner is the only writer that
 * ever counted. A follower that synced past the current leader's boundary is
 * holding a dead king's records — they get truncated, not merged.
 *
 * <p>
 * Two roles in the replication dance:
 * <ul>
 *   <li><b>Leader side</b>: on election win, {@link #recordEpoch} appends the
 *   new reign (idempotent — a re-elected same-epoch leader rewrites its own
 *   row). On a follower's arbitration query, {@link #endOffsetFor} answers
 *   "epoch E ends at X", where X is the next reign's start or the leader's
 *   live LEO for the reigning epoch.</li>
 *   <li><b>Follower side</b>: after adopting a leader (or truncating to its
 *   boundary), the table must stay consistent with the log — entries whose
 *   startOffset sits at/after a truncation point describe reigns over bytes
 *   that no longer exist, and {@link #truncateFrom} rolls them back.</li>
 * </ul>
 *
 * <p>
 * File format (ASCII, ascending epochs — human-debuggable):
 *
 * <pre>
 * v 1
 * 0 0
 * 1 5200
 * 2 9100
 * </pre>
 *
 * Writes use the M1 discipline: temp file, fsync, atomic rename. The
 * in-memory list is the truth between writes; the file is the truth after a
 * crash.
 */
public final class EpochCheckpoint implements Closeable {

    /** One reign: `epoch` started leading at `startOffset`. */
    public static final class Epoch {
        public final long epoch;
        public final long startOffset;

        public Epoch(long epoch, long startOffset) {
            this.epoch = epoch;
            this.startOffset = startOffset;
        }
    }

    private final File file;
    private final Object lock = new Object();
    private final List<Epoch> epochs = new ArrayList<Epoch>(); // ascending epoch

    private EpochCheckpoint(File file) {
        this.file = file;
    }

    /** Loads the table; a missing file is an empty history (fresh node). */
    public static EpochCheckpoint open(File file) throws IOException {
        EpochCheckpoint cp = new EpochCheckpoint(file);
        if (file.getParentFile() != null && !file.getParentFile().exists()
                && !file.getParentFile().mkdirs()) {
            throw new IOException("cannot create checkpoint dir " + file.getParentFile());
        }
        if (!file.exists()) {
            return cp;
        }
        int lineNo = 0;
        for (String line : Files.readAllLines(file.toPath(), StandardCharsets.US_ASCII)) {
            lineNo++;
            line = line.trim();
            if (line.isEmpty() || (lineNo == 1 && line.startsWith("v "))) {
                continue; // header
            }
            String[] parts = line.split("\\s+");
            if (parts.length != 2) {
                throw new IOException("corrupt epoch checkpoint at " + file
                        + " line " + lineNo + ": '" + line + "'");
            }
            long epoch = Long.parseLong(parts[0]);
            long start = Long.parseLong(parts[1]);
            if (!cp.epochs.isEmpty()) {
                Epoch prev = cp.epochs.get(cp.epochs.size() - 1);
                if (epoch <= prev.epoch) {
                    throw new IOException("epoch checkpoint not ascending at " + file
                            + " line " + lineNo + ": " + epoch + " after " + prev.epoch);
                }
            }
            cp.epochs.add(new Epoch(epoch, start));
        }
        return cp;
    }

    /** Highest epoch recorded, or -1 for an empty history. */
    public long latestEpoch() {
        synchronized (lock) {
            return epochs.isEmpty() ? -1L : epochs.get(epochs.size() - 1).epoch;
        }
    }

    /** The reigning epoch's start offset, or -1 if the table is empty. */
    public long latestStartOffset() {
        synchronized (lock) {
            return epochs.isEmpty() ? -1L : epochs.get(epochs.size() - 1).startOffset;
        }
    }

    /** Start offset of a specific epoch, or -1 if unknown. */
    public long startOffsetFor(long epoch) {
        synchronized (lock) {
            for (Epoch e : epochs) {
                if (e.epoch == epoch) {
                    return e.startOffset;
                }
            }
            return -1L;
        }
    }

    /**
     * Where does {@code epoch} end? The NEXT reign's startOffset; for the
     * reigning (latest) epoch it is the live log end passed by the caller —
     * the leader is still writing that range. Returns -1 for an unknown
     * epoch (the follower must then ask with the epoch it knows; -1 also
     * means "you are behind by whole reigns, resync from scratch").
     */
    public long endOffsetFor(long epoch, long currentLogEnd) {
        synchronized (lock) {
            for (int i = 0; i < epochs.size(); i++) {
                if (epochs.get(i).epoch == epoch) {
                    if (i + 1 < epochs.size()) {
                        return epochs.get(i + 1).startOffset;
                    }
                    return currentLogEnd;
                }
            }
            return -1L;
        }
    }

    /**
     * A new reign begins (election winner). Idempotent: an existing row for
     * the same epoch is replaced, and any rows for LATER epochs (a revived
     * node that somehow remembered future reigns) are dropped — the table
     * must never claim authority over a range the log does not back.
     */
    public void recordEpoch(long epoch, long startOffset) throws IOException {
        synchronized (lock) {
            int i = epochs.size();
            while (i > 0 && epochs.get(i - 1).epoch >= epoch) {
                epochs.remove(i - 1);
                i--;
            }
            if (i > 0 && epochs.get(i - 1).startOffset > startOffset) {
                throw new IOException("epoch " + epoch + " starts at " + startOffset
                        + " before previous reign's start " + epochs.get(i - 1).startOffset);
            }
            epochs.add(new Epoch(epoch, startOffset));
            persistLocked();
        }
    }

    /**
     * Roll the table back with the log (after an arbitrated truncation):
     * every entry starting at/after {@code offset} described a reign over
     * bytes that no longer exist. The entry that STRADDLES the offset stays
     * — its early bytes survived; the new lineage will extend its range.
     */
    public void truncateFrom(long offset) throws IOException {
        synchronized (lock) {
            boolean changed = false;
            while (!epochs.isEmpty()
                    && epochs.get(epochs.size() - 1).startOffset >= offset) {
                epochs.remove(epochs.size() - 1);
                changed = true;
            }
            if (changed) {
                persistLocked();
            }
        }
    }

    /** Immutable snapshot (monitoring / arbitration replies). */
    public List<Epoch> snapshot() {
        synchronized (lock) {
            return new ArrayList<Epoch>(epochs);
        }
    }

    private void persistLocked() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("v 1\n");
        for (Epoch e : epochs) {
            sb.append(e.epoch).append(' ').append(e.startOffset).append('\n');
        }
        File tmp = new File(file.getParentFile(), file.getName() + ".tmp");
        FileOutputStream out = new FileOutputStream(tmp);
        try {
            out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
            out.getFD().sync();
        } finally {
            out.close();
        }
        try {
            Files.move(tmp.toPath(), file.toPath(),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void close() {
        // Nothing held open: the file persists on every mutation.
    }
}
