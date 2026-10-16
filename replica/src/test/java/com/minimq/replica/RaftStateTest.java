package com.minimq.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RaftState (M3-3) semantics: the vote-arbitration state machine and — the
 * part Raft actually cares about — its durability. The crash tests simulate
 * kill -9 by dropping the object and reopening from disk.
 */
class RaftStateTest {

    @TempDir
    Path tempDir;

    private File stateFile() {
        return tempDir.resolve("raft-state").toFile();
    }

    @Test
    void freshNodeStartsAtTermZeroWithNoVote() throws IOException {
        RaftState s = RaftState.open(stateFile());
        try {
            assertEquals(0L, s.currentTerm());
            assertEquals(-1, s.votedFor());
        } finally {
            s.close();
        }
        assertTrue(stateFile().exists(), "initial state must already be on disk");
    }

    @Test
    void oneVotePerTerm_DoubleVoteIsRefused() throws IOException {
        RaftState s = RaftState.open(stateFile());
        try {
            assertTrue(s.tryVote(5, 1), "first vote in term 5");
            assertFalse(s.tryVote(5, 2), "second vote in the same term must be refused");
            assertEquals(1, s.votedFor());

            // idempotent retry of a lost response is still granted
            assertTrue(s.tryVote(5, 1));
            assertEquals(1, s.votedFor());
        } finally {
            s.close();
        }
    }

    @Test
    void staleTermIsRefusedAndNotAdopted() throws IOException {
        RaftState s = RaftState.open(stateFile());
        try {
            s.observeTerm(9);
            assertFalse(s.tryVote(7, 1), "campaign from a stale term");
            assertEquals(9L, s.currentTerm());
            assertEquals(-1, s.votedFor());
        } finally {
            s.close();
        }
    }

    @Test
    void biggerTermIsAdoptedAndVoteGranted() throws IOException {
        RaftState s = RaftState.open(stateFile());
        try {
            s.observeTerm(3);
            assertTrue(s.tryVote(4, 2), "a newer campaign earns the vote AND the term");
            assertEquals(4L, s.currentTerm());
            assertEquals(2, s.votedFor());
        } finally {
            s.close();
        }
    }

    @Test
    void observingBiggerTermResetsVote() throws IOException {
        RaftState s = RaftState.open(stateFile());
        try {
            s.tryVote(5, 1);
            s.observeTerm(6);
            assertEquals(6L, s.currentTerm());
            assertEquals(-1, s.votedFor(), "a new term starts unvoted");
            assertTrue(s.tryVote(6, 3), "and the term-6 vote is available again");
            // a SMALLER term on the wire changes nothing
            s.observeTerm(2);
            assertEquals(6L, s.currentTerm());
        } finally {
            s.close();
        }
    }

    /**
     * Iron rule #2, the kill -9 form: the node voted in term 5, crashed, and
     * woke up with no memory. It must STILL refuse a second vote in term 5 —
     * this is exactly the two-leaders-by-double-voting scenario, and the
     * persisted file is the only thing standing in its way.
     */
    @Test
    void voteSurvivesKill9Restart_NoSecondVoteInSameTerm() throws IOException {
        RaftState before = RaftState.open(stateFile());
        before.tryVote(5, 1);
        before.close(); // no flush-on-close tricks: the vote was durable at grant time

        RaftState after = RaftState.open(stateFile());
        try {
            assertEquals(5L, after.currentTerm(), "term survives restart");
            assertEquals(1, after.votedFor(), "vote survives restart");
            assertFalse(after.tryVote(5, 2),
                    "restarted node must not double-vote in the term it already spent");
            assertTrue(after.tryVote(6, 2), "the next term's vote is, of course, free");
        } finally {
            after.close();
        }
    }

    @Test
    void campaignAdvancesTermAndVotesForSelf_Durably() throws IOException {
        RaftState s = RaftState.open(stateFile());
        assertEquals(1L, s.advanceTermAndVoteSelf(3));
        assertEquals(2L, s.advanceTermAndVoteSelf(3));
        s.close();

        RaftState after = RaftState.open(stateFile());
        try {
            assertEquals(2L, after.currentTerm());
            assertEquals(3, after.votedFor(), "candidate remembers it voted for itself");
        } finally {
            after.close();
        }
    }
}
