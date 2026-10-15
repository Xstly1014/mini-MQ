package com.minimq.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Startup recovery. Scans segments from the newest backwards until one yields a
 * valid tail, then truncates dirty bytes (torn writes / CRC failures) in place.
 *
 * v0 contract: only the candidate tail segment is scanned; older segments are
 * trusted. If the newest segment contains no valid record at all and it is not
 * the only segment, it is deleted and the previous segment becomes the
 * candidate. This handles both "rolled, then crashed before first append" and
 * "the whole tail is garbage".
 */
public final class Recovery {

    private Recovery() {
    }

    public static final class Result implements AutoCloseable {
        /** All retained segments in offset order; empty for a fresh store. */
        public final ArrayList<MappedFile> segments;
        /** Validated tail segment; null for a fresh store. */
        public final MappedFile lastSegment;
        /** Global offset just past the last valid record. */
        public final long maxOffset;
        public final long recoveredMessages;
        /** Dirty bytes cleared from the tail segment (0 for a clean shutdown). */
        public final int truncatedBytes;
        /** Number of invalid tail records (torn or CRC-failed). */
        public final int invalidTailRecords;
        /** How many empty tail segments were deleted while walking backwards. */
        public final int deletedSegments;

        Result(ArrayList<MappedFile> segments, MappedFile lastSegment, long maxOffset,
                long recoveredMessages, int truncatedBytes, int invalidTailRecords,
                int deletedSegments) {
            this.segments = segments;
            this.lastSegment = lastSegment;
            this.maxOffset = maxOffset;
            this.recoveredMessages = recoveredMessages;
            this.truncatedBytes = truncatedBytes;
            this.invalidTailRecords = invalidTailRecords;
            this.deletedSegments = deletedSegments;
        }

        /**
         * Releases the mapped segments. Only for callers that use
         * {@link Recovery#recover} standalone (tests); {@link CommitLog}
         * takes ownership of the segments and closes them itself.
         */
        public void close() throws IOException {
            for (MappedFile mf : segments) {
                mf.close();
            }
        }
    }

    private static final class ScanOutcome {
        int validPos;
        long messages;
        boolean dirtyTail;
    }

    public static Result recover(File dir, int segmentSize) throws IOException {
        File[] files = listSegments(dir);
        if (files.length == 0) {
            return new Result(new ArrayList<MappedFile>(), null, 0L, 0L, 0, 0, 0);
        }

        int deleted = 0;
        int idx = files.length - 1;
        // Walk from the newest segment backwards until one is retainable.
        while (true) {
            MappedFile candidate = new MappedFile(files[idx], segmentSize);
            ScanOutcome outcome = scanSegment(candidate);

            boolean dropCandidate = outcome.dirtyTail && outcome.validPos == 0 && idx > 0;
            if (dropCandidate) {
                // Newest candidate has nothing valid: drop it, try the previous one.
                candidate.close();
                if (!files[idx].delete()) {
                    throw new IOException("failed to delete invalid tail segment " + files[idx]);
                }
                deleted++;
                idx--;
                continue;
            }

            if (outcome.dirtyTail) {
                // Keep the segment, truncate the dirty tail in place.
                int dirtyEnd = Math.min(candidate.fileSize(),
                        outcome.validPos + MessageCodec.MAX_MESSAGE_SIZE);
                candidate.truncate(outcome.validPos, dirtyEnd);
                candidate.setWrotePosition(outcome.validPos);
                return finish(dir, segmentSize, files, idx, candidate, outcome,
                        dirtyEnd - outcome.validPos, 1, deleted);
            }

            // Clean tail (pre-allocated zero region reached): keep everything.
            candidate.setWrotePosition(outcome.validPos);
            return finish(dir, segmentSize, files, idx, candidate, outcome, 0, 0, deleted);
        }
    }

    private static Result finish(File dir, int segmentSize, File[] files, int idx,
            MappedFile candidate, ScanOutcome outcome,
            int truncatedBytes, int invalidTails, int deleted)
            throws IOException {
        ArrayList<MappedFile> segments = new ArrayList<MappedFile>(idx + 1);
        for (int i = 0; i < idx; i++) {
            MappedFile older = new MappedFile(files[i], segmentSize);
            // Older segments were sealed by rolling, so their data runs to the
            // end (possibly with a zero hole after the last record, which the
            // iterator skips over). Restoring wrotePosition=fileSize keeps
            // them readable after a reopen.
            older.setWrotePosition(segmentSize);
            segments.add(older);
        }
        segments.add(candidate);
        long maxOffset = candidate.fileFromOffset() + outcome.validPos;
        return new Result(segments, candidate, maxOffset, outcome.messages,
                truncatedBytes, invalidTails, deleted);
    }

    /**
     * Scans one segment from byte 0. Stops at the first EMPTY region (clean),
     * or at the first INCOMPLETE/CORRUPT record (dirty). Returns the position
     * just past the last valid record.
     */
    private static ScanOutcome scanSegment(MappedFile segment) {
        ScanOutcome outcome = new ScanOutcome();
        ByteBuffer buf = segment.slice(0, segment.fileSize());
        int pos = 0;
        while (pos <= segment.fileSize() - MessageCodec.HEADER_SIZE) {
            MessageCodec.DecodeResult r = MessageCodec.decode(buf, pos);
            if (r.status == MessageCodec.DecodeStatus.OK) {
                pos += r.size;
                outcome.messages++;
            } else if (r.status == MessageCodec.DecodeStatus.EMPTY) {
                outcome.validPos = pos;
                outcome.dirtyTail = false;
                return outcome;
            } else {
                outcome.validPos = pos;
                outcome.dirtyTail = true;
                return outcome;
            }
        }
        // Segment filled to the brim without any zero hole: tail is at the edge.
        outcome.validPos = segment.fileSize();
        outcome.dirtyTail = false;
        return outcome;
    }

    static File[] listSegments(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            return new File[0];
        }
        File[] segments = Arrays.stream(files)
                .filter(f -> f.isFile() && f.getName().matches("\\d{20}"))
                .toArray(File[]::new);
        Arrays.sort(segments, Comparator.comparingLong(f -> Long.parseLong(f.getName())));
        return segments;
    }
}
