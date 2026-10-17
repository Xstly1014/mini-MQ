package com.minimq.replica;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * The durable arbitration state of the election kernel (M3):
 * {@code currentTerm} + {@code votedFor}.
 *
 * <p>
 * Raft's iron rule #2 — a node that forgets its vote after a crash can cast a
 * SECOND vote in the same term, and with it two candidates both reach a
 * majority. Persistence is the one and only defense, which shapes this API:
 * every mutation ({@link #advanceTermAndVoteSelf}, {@link #tryVote},
 * {@link #observeTerm}) fsyncs BEFORE the call returns. A vote that was
 * acknowledged but not yet durable is a correctness bug, not a performance
 * problem.
 *
 * <p>
 * Durability discipline is the M1 offset-file one: write a temp file, force
 * it, atomically rename over the old state — a crash mid-write leaves either
 * the complete old state or the complete new one, never a torn hybrid.
 *
 * <p>
 * File format (ASCII, human-debuggable):
 *
 * <pre>
 * term=7
 * votedFor=2
 * </pre>
 *
 * {@code votedFor=-1} means "no vote cast this term". Term is a pure logical
 * counter (iron rule #1): it never reads the wall clock, it only ever
 * increments locally or adopts a bigger value seen on the wire.
 */
public final class RaftState implements Closeable {

    private final File file;
    private final Object lock = new Object();
    private long currentTerm;
    private int votedFor;

    private RaftState(File file, long currentTerm, int votedFor) {
        this.file = file;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    /**
     * Loads (or initializes) the state. A missing file is a brand-new node:
     * term 0, no vote — written to disk immediately so the very first
     * election already goes through the durable path.
     */
    public static RaftState open(File file) throws IOException {
        if (file.getParentFile() != null && !file.getParentFile().exists()
                && !file.getParentFile().mkdirs()) {
            throw new IOException("cannot create raft state dir " + file.getParentFile());
        }
        if (file.exists()) {
            long term = 0L;
            int votedFor = -1;
            for (String line : Files.readAllLines(file.toPath(), StandardCharsets.US_ASCII)) {
                if (line.startsWith("term=")) {
                    term = Long.parseLong(line.substring(5).trim());
                } else if (line.startsWith("votedFor=")) {
                    votedFor = Integer.parseInt(line.substring(9).trim());
                }
            }
            return new RaftState(file, term, votedFor);
        }
        RaftState fresh = new RaftState(file, 0L, -1);
        fresh.persistLocked();
        return fresh;
    }

    public long currentTerm() {
        synchronized (lock) {
            return currentTerm;
        }
    }

    /** The node this term's vote went to, or -1 if none. */
    public int votedFor() {
        synchronized (lock) {
            return votedFor;
        }
    }

    /**
     * Iron rule #5, passive side: a bigger term seen anywhere on the wire
     * (vote request, heartbeat, response) is adopted on the spot and the
     * vote is reset — the node demotes itself before processing anything
     * else. Smaller terms are ignored: stale senders learn OUR term from the
     * response instead.
     */
    public void observeTerm(long term) throws IOException {
        synchronized (lock) {
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = -1;
                persistLocked();
            }
        }
    }

    /**
     * Campaign transition: term+1 and vote for self, persisted BEFORE any
     * vote request leaves the node (a candidate that crashes mid-campaign
     * must wake up remembering it already voted for itself).
     *
     * @return the new campaign term
     */
    public long advanceTermAndVoteSelf(int selfId) throws IOException {
        synchronized (lock) {
            currentTerm++;
            votedFor = selfId;
            persistLocked();
            return currentTerm;
        }
    }

    /**
     * Grants or refuses a vote (iron rules #2 + #4 storage side — the
     * up-to-date log check lives in the election state machine, which calls
     * this only after the candidate's log passed).
     *
     * <ul>
     *   <li>term &lt; currentTerm: refuse (stale campaign).</li>
     *   <li>term &gt; currentTerm: grant, adopting the term (an election this
     *   far along has a candidate worth following).</li>
     *   <li>term == currentTerm: grant only if not yet voted, or already
     *   voted for the same candidate (idempotent retry of a lost
     *   response).</li>
     * </ul>
     */
    public boolean tryVote(long term, int candidateId) throws IOException {
        synchronized (lock) {
            if (term < currentTerm) {
                return false;
            }
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = candidateId;
                persistLocked();
                return true;
            }
            if (votedFor == -1 || votedFor == candidateId) {
                votedFor = candidateId;
                persistLocked();
                return true;
            }
            return false; // one vote per term, already spent
        }
    }

    /** tmp + fsync + atomic rename: never a torn state on disk. */
    private void persistLocked() throws IOException {
        File tmp = new File(file.getParentFile(), file.getName() + ".tmp");
        byte[] body = ("term=" + currentTerm + "\nvotedFor=" + votedFor + "\n")
                .getBytes(StandardCharsets.US_ASCII);
        FileOutputStream out = new FileOutputStream(tmp);
        try {
            out.write(body);
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
        // Nothing held open: state persists on every mutation, not on close.
    }
}
