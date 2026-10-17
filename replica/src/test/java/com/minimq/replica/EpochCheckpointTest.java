package com.minimq.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * EpochCheckpoint (M3-3) semantics: reign boundaries, the endOffset
 * arbitration query that drives stale-tail truncation, and the rollback that
 * keeps the table consistent with a truncated log. Crash = drop the object,
 * reopen from disk.
 */
class EpochCheckpointTest {

    @TempDir
    Path tempDir;

    private File cpFile() {
        return tempDir.resolve("leader-epoch-checkpoint").toFile();
    }

    @Test
    void emptyHistoryReportsNoEpoch() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        try {
            assertEquals(-1L, cp.latestEpoch());
            assertEquals(-1L, cp.latestStartOffset());
            assertEquals(-1L, cp.startOffsetFor(0));
        } finally {
            cp.close();
        }
    }

    @Test
    void endOffsetArbitration_RangesOwnByEpoch() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        try {
            cp.recordEpoch(0, 0);
            cp.recordEpoch(1, 5200);
            cp.recordEpoch(2, 9100);

            // a middle epoch ends where the next reign began
            assertEquals(5200L, cp.endOffsetFor(0, 99_999));
            assertEquals(9100L, cp.endOffsetFor(1, 99_999));
            // the reigning epoch ends at the LIVE log end the leader reports
            assertEquals(9350L, cp.endOffsetFor(2, 9350));
            assertEquals(9351L, cp.endOffsetFor(2, 9351), "moves with the leader's LEO");
            // unknown epoch: no authority to speak of
            assertEquals(-1L, cp.endOffsetFor(7, 9350));
        } finally {
            cp.close();
        }
    }

    @Test
    void tableSurvivesRestart() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        cp.recordEpoch(0, 0);
        cp.recordEpoch(1, 5200);
        cp.close();

        EpochCheckpoint after = EpochCheckpoint.open(cpFile());
        try {
            assertEquals(1L, after.latestEpoch());
            assertEquals(5200L, after.latestStartOffset());
            assertEquals(0L, after.startOffsetFor(0));
        } finally {
            after.close();
        }
    }

    /** Re-election of the same epoch rewrites its own row, nothing else. */
    @Test
    void recordEpochIsIdempotentForSameEpoch() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        try {
            cp.recordEpoch(0, 0);
            cp.recordEpoch(1, 5200);
            cp.recordEpoch(1, 5200); // retry after a lost ack
            List<EpochCheckpoint.Epoch> snapshot = cp.snapshot();
            assertEquals(2, snapshot.size());
            assertEquals(5200L, cp.startOffsetFor(1));
        } finally {
            cp.close();
        }
    }

    /**
     * The stale-leader revival path: the log is arbitrated back to 5200, the
     * table follows via truncateFrom, and only THEN may a new reign be
     * recorded — the new lineage extends the surviving range.
     */
    @Test
    void revivedNodeRecordsNewEpochAfterRollback() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        cp.recordEpoch(0, 0);
        cp.recordEpoch(1, 5200);
        cp.recordEpoch(2, 9100);
        cp.truncateFrom(5200); // log rewound: epoch 2's reign is dead
        cp.recordEpoch(5, 5200); // ...and the revived node leads epoch 5 from there
        cp.close();

        EpochCheckpoint after = EpochCheckpoint.open(cpFile());
        try {
            assertEquals(5L, after.latestEpoch());
            assertEquals(5200L, after.latestStartOffset());
            assertEquals(-1L, after.startOffsetFor(2), "dead reign must be gone");
            assertEquals(-1L, after.startOffsetFor(1), "same start point, superseded");
            assertEquals(0L, after.startOffsetFor(0));
        } finally {
            after.close();
        }
    }

    /**
     * Defense-in-depth: a table claiming reigns over bytes the log no longer
     * has (rollback happened, table didn't follow) must fail the next
     * recordEpoch loudly — a silent merge would resurrect the divergence the
     * arbitration was supposed to end.
     */
    @Test
    void tableClaimingBytesTheLogDoesNotHaveIsRejected() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        try {
            cp.recordEpoch(0, 0);
            cp.recordEpoch(2, 9100);
            assertThrows(IOException.class, () -> cp.recordEpoch(5, 6000),
                    "log ends at 6000 but the table claims a reign from 9100");
        } finally {
            cp.close();
        }
    }

    @Test
    void nonMonotonicStartOffsetIsRejected() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        try {
            cp.recordEpoch(0, 1000);
            assertThrows(IOException.class, () -> cp.recordEpoch(1, 500),
                    "a reign cannot begin before the previous one");
        } finally {
            cp.close();
        }
    }

    /**
     * The truncation companion: after the log rewinds to X, reigns that
     * STARTED at/after X are gone; the reign straddling X keeps its row (its
     * early bytes survived and the new lineage extends the range).
     */
    @Test
    void truncateFromRollsBackWithTheLog_AndSurvivesRestart() throws IOException {
        EpochCheckpoint cp = EpochCheckpoint.open(cpFile());
        cp.recordEpoch(0, 0);
        cp.recordEpoch(1, 5200);
        cp.recordEpoch(2, 9100);
        // the log is truncated back to 6000: epoch 2 is dead, epoch 1 straddles
        cp.truncateFrom(6000);
        cp.close();

        EpochCheckpoint after = EpochCheckpoint.open(cpFile());
        try {
            assertEquals(1L, after.latestEpoch());
            assertEquals(5200L, after.latestStartOffset());
            assertEquals(-1L, after.startOffsetFor(2), "dead reign must not survive the truncation");
            // truncating exactly at a boundary kills that reign's row too
            after.truncateFrom(5200);
            assertEquals(0L, after.latestEpoch());
        } finally {
            after.close();
        }
    }

    @Test
    void corruptFileIsRejectedLoudly() throws IOException {
        java.nio.file.Files.write(cpFile().toPath(),
                "v 1\nnot a number\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        assertThrows(IOException.class, () -> EpochCheckpoint.open(cpFile()));
    }

    @Test
    void nonAscendingFileIsRejectedLoudly() throws IOException {
        java.nio.file.Files.write(cpFile().toPath(),
                "v 1\n1 100\n1 200\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        assertThrows(IOException.class, () -> EpochCheckpoint.open(cpFile()));
        assertTrue(cpFile().exists());
    }
}
