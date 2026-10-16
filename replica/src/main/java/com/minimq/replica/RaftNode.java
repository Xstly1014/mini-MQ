package com.minimq.replica;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.gen.MqProto.HeartbeatRequest;
import com.minimq.protocol.gen.MqProto.HeartbeatResponse;
import com.minimq.protocol.gen.MqProto.VoteRequest;
import com.minimq.protocol.gen.MqProto.VoteResponse;

/**
 * The election kernel (M3): a Raft-style three-role state machine —
 * Follower / (Pre-)Candidate / Leader — plus the transport seam that keeps
 * it testable in memory.
 *
 * <h3>Single-threaded by construction</h3>
 * The first engineering trap in Raft is concurrent state mutation (heartbeat
 * timer, election timer, RPC handlers all poking {@code role} and
 * {@code term}). The cure here is the Netty-EventLoop discipline: every
 * inbound message and every timer expiry becomes an event on one queue,
 * consumed by one thread. The network threads only enqueue. All state below
 * (except the volatile publish fields) is owned by that thread — no locks,
 * no races, deterministic ordering.
 *
 * <h3>The five iron rules, and where each lives</h3>
 * <ol>
 *   <li><b>Term is a logical counter, never the wall clock</b> — only
 *   {@link RaftState#advanceTermAndVoteSelf} (local ++) and
 *   {@link RaftState#observeTerm} (adopt bigger) ever move it.</li>
 *   <li><b>votedFor is durable before the vote is answered</b> —
 *   {@link RaftState#tryVote} fsyncs before returning true; a kill -9
 *   between the grant and the reply still cannot produce a double vote.</li>
 *   <li><b>Election timeout is randomized</b> — re-rolled on every reset in
 *   {@code [base, 2*base)}, so simultaneous candidates self-separate
 *   instead of deadlocking in a perpetual split vote.</li>
 *   <li><b>Up-to-date check</b> — a vote is granted only to a candidate
 *   whose (lastLogEpoch, lastLogOffset) is at least the voter's: a lagging
 *   leader would bury committed data under stale history.</li>
 *   <li><b>A bigger term seen anywhere demotes on the spot</b> —
 *   {@code adoptTerm} in every handler; split-brain ends at the first
 *   post-heal contact.</li>
 * </ol>
 *
 * <h3>PreVote (etcd default-on)</h3>
 * A node isolated from the quorum keeps timing out; without protection it
 * would keep bumping its term and, on healing, dethrone a perfectly healthy
 * leader by rule #5. So a campaign starts with a term-less probe ("COULD I
 * win?"); only a probe majority earns the real
 * {@link RaftState#advanceTermAndVoteSelf}. A node with an unspent vote
 * (i.e. any node that recognized a live term) refuses probes, which is
 * exactly why the healed node cannot disturb the incumbent.
 *
 * <h3>Timing</h3>
 * Heartbeat : election-timeout keeps the etcd 10:1 shape (100ms : 1000ms
 * there; the defaults here are scaled for a local 3-node cluster). The
 * scheduler runs on {@link System#nanoTime} — monotonic, immune to NTP
 * steps.
 */
public final class RaftNode {

    private static final Logger LOG = Logger.getLogger(RaftNode.class.getName());

    public enum Role { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    /** Network seam: fire-and-forget; replies return via the handle* methods. */
    public interface Transport {
        void sendVoteRequest(int destId, VoteRequest request);

        void sendVoteResponse(int destId, VoteResponse response);

        void sendHeartbeatRequest(int destId, HeartbeatRequest request);

        void sendHeartbeatResponse(int destId, HeartbeatResponse response);
    }

    /** The node's own log currency — the up-to-date comparison key. */
    public interface LogView {
        long logEndOffset();

        long lastLogEpoch();
    }

    /** Role transition hook (wiring records epochs, exposes leadership). */
    public interface Listener {
        void onRoleChange(int nodeId, Role newRole, long term);

        /**
         * A live leader's authoritative heartbeat arrived while this node
         * was ALREADY a follower — no role change happened, so
         * {@link #onRoleChange} stayed silent, but the wiring still needs to
         * (re)start follower sync against that leader. Fired on every
         * authoritative heartbeat; implementations must be idempotent.
         */
        default void onLeaderSeen(int nodeId, int leaderId, long term) {
        }
    }

    private final int selfId;
    private final int nodeCount;
    private final int majority;
    private final RaftState state;
    private final LogView log;
    private final Transport transport;
    private final Listener listener;
    private final long heartbeatIntervalMs;
    private final long electionTimeoutBaseMs;
    private final boolean preVoteEnabled;
    private final long randomSeed;
    /** Election-loop-owned jitter source (re-seeded per node, not per JVM). */
    private final java.util.Random rng;
    private final BlockingQueue<Runnable> inbox = new LinkedBlockingQueue<Runnable>();
    private final Thread loop;

    // ---- event-loop-owned state (no locks: single consumer) ----
    private long electionDeadlineNanos;
    private long heartbeatDeadlineNanos;
    private final Set<Integer> probeVoters = new HashSet<Integer>();
    private final Set<Integer> voters = new HashSet<Integer>();
    /**
     * In-memory mirror of this term's vote (etcd's r.Vote): PRE_CANDIDATE
     * resets it, real votes set it, the durable state never hears about
     * probes. Without the reset, two followers whose leader died would
     * refuse each other's probes forever — both still holding the departed
     * leader's term-1 vote — and no election would ever converge (the
     * observed kill-leader deadlock).
     */
    private int memVoteFor = -1;

    // ---- published for monitoring/tests ----
    private volatile boolean running;
    private volatile Role role = Role.FOLLOWER;
    private volatile long visibleTerm;

    private RaftNode(Builder b) {
        this.selfId = b.selfId;
        this.nodeCount = b.nodeCount;
        this.majority = nodeCount / 2 + 1;
        this.state = b.state;
        this.log = b.log;
        this.transport = b.transport;
        this.listener = b.listener;
        this.heartbeatIntervalMs = b.heartbeatIntervalMs;
        this.electionTimeoutBaseMs = b.electionTimeoutBaseMs;
        this.preVoteEnabled = b.preVoteEnabled;
        this.randomSeed = b.randomSeed;
        this.rng = new java.util.Random(b.randomSeed ^ System.nanoTime());
        this.loop = new Thread(new Runnable() {
            @Override
            public void run() {
                runLoop();
            }
        }, "mini-mq-raft-" + selfId);
        this.visibleTerm = b.state.currentTerm();
        this.memVoteFor = b.state.votedFor(); // restart: last durable vote still counts
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    public void start() {
        if (running) {
            return;
        }
        running = true;
        resetElectionDeadline();
        loop.start();
    }

    public void shutdown() {
        running = false;
        loop.interrupt();
        try {
            loop.join(2_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // inbound API (network threads: enqueue only)
    // ------------------------------------------------------------------

    public void handleVoteRequest(final VoteRequest request) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                processVoteRequest(request);
            }
        });
    }

    public void handleVoteResponse(final VoteResponse response) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                processVoteResponse(response);
            }
        });
    }

    public void handleHeartbeatRequest(final HeartbeatRequest request) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                processHeartbeatRequest(request);
            }
        });
    }

    public void handleHeartbeatResponse(final HeartbeatResponse response) {
        enqueue(new Runnable() {
            @Override
            public void run() {
                processHeartbeatResponse(response);
            }
        });
    }

    // ------------------------------------------------------------------
    // monitoring (any thread)
    // ------------------------------------------------------------------

    public Role role() {
        return role;
    }

    public long term() {
        return visibleTerm;
    }

    public int selfId() {
        return selfId;
    }

    // ------------------------------------------------------------------
    // event loop
    // ------------------------------------------------------------------

    private void runLoop() {
        while (running) {
            try {
                long deadline = role == Role.LEADER ? heartbeatDeadlineNanos : electionDeadlineNanos;
                long waitMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                Runnable event = waitMs <= 0 ? inbox.poll() : inbox.poll(waitMs, TimeUnit.MILLISECONDS);
                if (event != null) {
                    event.run();
                } else if (System.nanoTime() >= deadline) {
                    onTimer();
                } // else: spurious wake — loop
            } catch (InterruptedException e) {
                return; // shutdown
            } catch (Throwable t) {
                if (running) {
                    LOG.log(Level.WARNING, "raft-" + selfId + " event failed", t);
                }
            }
        }
    }

    private void onTimer() {
        if (role == Role.LEADER) {
            heartbeatDeadlineNanos = System.nanoTime() + ms(heartbeatIntervalMs);
            broadcastHeartbeat();
        } else {
            beginElection();
        }
    }

    // ------------------------------------------------------------------
    // election
    // ------------------------------------------------------------------

    /**
     * The randomized iron rule #3: re-rolled on EVERY reset, because the
     * second round of a split vote needs different dice than the first.
     */
    private void resetElectionDeadline() {
        long timeout = electionTimeoutBaseMs + (rng.nextLong() & Long.MAX_VALUE) % electionTimeoutBaseMs;
        electionDeadlineNanos = System.nanoTime() + ms(timeout);
    }

    private void beginElection() {
        if (preVoteEnabled) {
            role = Role.PRE_CANDIDATE;
            // etcd's becomePreCandidate resets r.Vote: timing out IS the
            // admission that this term has no leader I recognize, so my old
            // vote stops vetoing other leaderless nodes' probes. The durable
            // vote is untouched — a probe must never have persistence
            // side effects.
            memVoteFor = -1;
            probeVoters.clear();
            probeVoters.add(selfId);
            resetElectionDeadline();
            VoteRequest probe = VoteRequest.newBuilder()
                    .setCandidateId(selfId)
                    .setTerm(state.currentTerm() + 1) // the term I WOULD take
                    .setPreVote(true)
                    .setLastLogEpoch(log.lastLogEpoch())
                    .setLastLogOffset(log.logEndOffset())
                    .build();
            broadcast(probe);
            if (probeVoters.size() >= majority) { // single-node cluster
                startCampaign();
            }
        } else {
            startCampaign();
        }
    }

    /** The real thing: term++ and self-vote hit the disk BEFORE any request. */
    private void startCampaign() {
        try {
            state.advanceTermAndVoteSelf(selfId);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "raft-" + selfId + " cannot persist campaign term", e);
            resetElectionDeadline();
            return; // no durable vote, no campaign — try again next timeout
        }
        memVoteFor = selfId;
        visibleTerm = state.currentTerm();
        role = Role.CANDIDATE;
        voters.clear();
        voters.add(selfId);
        resetElectionDeadline();
        VoteRequest request = VoteRequest.newBuilder()
                .setCandidateId(selfId)
                .setTerm(state.currentTerm())
                .setPreVote(false)
                .setLastLogEpoch(log.lastLogEpoch())
                .setLastLogOffset(log.logEndOffset())
                .build();
        broadcast(request);
        if (voters.size() >= majority) { // single-node cluster
            becomeLeader();
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        heartbeatDeadlineNanos = System.nanoTime(); // heartbeat immediately:
        // authority must be claimed before the first produce is accepted
        if (listener != null) {
            listener.onRoleChange(selfId, Role.LEADER, state.currentTerm());
        }
    }

    private void broadcastHeartbeat() {
        HeartbeatRequest heartbeat = HeartbeatRequest.newBuilder()
                .setLeaderId(selfId)
                .setTerm(state.currentTerm())
                .setEpoch(state.currentTerm()) // leader epoch == election term
                .build();
        for (int id = 0; id < nodeCount; id++) {
            if (id != selfId) {
                transport.sendHeartbeatRequest(id, heartbeat);
            }
        }
    }

    private void broadcast(VoteRequest request) {
        for (int id = 0; id < nodeCount; id++) {
            if (id != selfId) {
                transport.sendVoteRequest(id, request);
            }
        }
    }

    // ------------------------------------------------------------------
    // handlers (event thread only)
    // ------------------------------------------------------------------

    private void processVoteRequest(VoteRequest request) {
        // Iron rule #5 applies to REAL campaigns only: a PreVote probe carries
        // the term the candidate WOULD take — adopting it here would let two
        // concurrent probes ratchet each other's terms skyward with no
        // leader ever elected (the observed live-lock: everyone follower,
        // term in the dozens, forever).
        if (!request.getPreVote() && request.getTerm() > state.currentTerm()) {
            adoptTerm(request.getTerm());
        }
        boolean logOk = candidateIsUpToDate(request.getLastLogEpoch(), request.getLastLogOffset());
        VoteResponse.Builder resp = VoteResponse.newBuilder()
                .setTerm(state.currentTerm())
                .setResponderId(selfId);
        if (request.getPreVote()) {
            // Term-less probe: no persistence, no state change. The in-memory
            // vote (reset when I myself timed out) is the whole isolation
            // defense — a node that voted this term, or that still hears a
            // leader, refuses; two leaderless nodes grant each other.
            boolean canVote = memVoteFor == -1 || memVoteFor == request.getCandidateId();
            resp.setGranted(request.getTerm() > state.currentTerm() && canVote && logOk);
        } else {
            boolean granted = false;
            try {
                granted = logOk && state.tryVote(request.getTerm(), request.getCandidateId());
            } catch (IOException e) {
                LOG.log(Level.WARNING, "raft-" + selfId + " vote persistence failed", e);
            }
            if (granted) {
                memVoteFor = request.getCandidateId();
            }
            resp.setGranted(granted);
        }
        transport.sendVoteResponse(request.getCandidateId(), resp.build());
    }

    private void processVoteResponse(VoteResponse response) {
        if (response.getTerm() > state.currentTerm()) {
            adoptTerm(response.getTerm());
            return;
        }
        if (response.getTerm() != state.currentTerm()) {
            return; // from an older campaign of mine, already moot
        }
        if (role == Role.PRE_CANDIDATE && response.getGranted()) {
            probeVoters.add(response.getResponderId());
            if (probeVoters.size() >= majority) {
                startCampaign();
            }
        } else if (role == Role.CANDIDATE && response.getGranted()) {
            voters.add(response.getResponderId());
            if (voters.size() >= majority) {
                becomeLeader();
            }
        }
    }

    private void processHeartbeatRequest(HeartbeatRequest request) {
        if (request.getTerm() > state.currentTerm()) {
            adoptTerm(request.getTerm());
        }
        boolean authority = request.getTerm() == state.currentTerm();
        if (authority && role != Role.FOLLOWER) {
            // a same-term leader exists: my campaign (probe or real) is over
            becomeFollower();
        }
        if (authority) {
            resetElectionDeadline();
            // The sync restart signal: a follower whose syncer stopped on a
            // leaderLost never sees a role CHANGE again — this heartbeat is
            // the only "there is a leader now" event it will ever get.
            if (role == Role.FOLLOWER && listener != null) {
                listener.onLeaderSeen(selfId, request.getLeaderId(), state.currentTerm());
            }
        }
        transport.sendHeartbeatResponse(request.getLeaderId(), HeartbeatResponse.newBuilder()
                .setTerm(state.currentTerm())
                .setResponderId(selfId)
                .build());
    }

    private void processHeartbeatResponse(HeartbeatResponse response) {
        if (response.getTerm() > state.currentTerm()) {
            adoptTerm(response.getTerm());
        }
        // else: territory confirmed; ISR tracking is the replication
        // layer's job (FOLLOWER_FETCH timestamps), not the election's.
    }

    // ------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------

    /**
     * Iron rule #4: lastLogEpoch first, then lastLogOffset — a candidate
     * must be at least as current as the voter's own log.
     */
    private boolean candidateIsUpToDate(long candidateEpoch, long candidateOffset) {
        long myEpoch = log.lastLogEpoch();
        if (candidateEpoch != myEpoch) {
            return candidateEpoch > myEpoch;
        }
        return candidateOffset >= log.logEndOffset();
    }

    private void adoptTerm(long term) {
        try {
            state.observeTerm(term);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "raft-" + selfId + " cannot persist adopted term " + term, e);
        }
        memVoteFor = state.votedFor(); // observeTerm reset it to -1 on adoption
        visibleTerm = state.currentTerm();
        becomeFollower();
        // A fresher term deserves a fresh lease: without this reset the node
        // would time out moments after adopting and probe again, pointless.
        resetElectionDeadline();
    }

    private void becomeFollower() {
        if (role == Role.FOLLOWER) {
            return;
        }
        role = Role.FOLLOWER;
        if (listener != null) {
            listener.onRoleChange(selfId, Role.FOLLOWER, state.currentTerm());
        }
    }

    private void enqueue(Runnable event) {
        if (running) {
            inbox.add(event);
        }
    }

    private static long ms(long millis) {
        return millis * 1_000_000L;
    }

    // ------------------------------------------------------------------
    // builder
    // ------------------------------------------------------------------

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Wiring/test surface: every knob the election kernel owns. */
    public static final class Builder {
        private int selfId;
        private int nodeCount = 1;
        private RaftState state;
        private LogView log;
        private Transport transport;
        private Listener listener;
        private long heartbeatIntervalMs = 50L;
        private long electionTimeoutBaseMs = 300L;
        private boolean preVoteEnabled = true;
        private long randomSeed = 0L;

        public Builder selfId(int selfId) {
            this.selfId = selfId;
            return this;
        }

        public Builder nodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder state(RaftState state) {
            this.state = state;
            return this;
        }

        public Builder log(LogView log) {
            this.log = log;
            return this;
        }

        public Builder transport(Transport transport) {
            this.transport = transport;
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = listener;
            return this;
        }

        public Builder heartbeatIntervalMs(long ms) {
            this.heartbeatIntervalMs = ms;
            return this;
        }

        public Builder electionTimeoutBaseMs(long ms) {
            this.electionTimeoutBaseMs = ms;
            return this;
        }

        public Builder preVoteEnabled(boolean enabled) {
            this.preVoteEnabled = enabled;
            return this;
        }

        public Builder randomSeed(long seed) {
            this.randomSeed = seed;
            return this;
        }

        public RaftNode build() {
            if (state == null || log == null || transport == null) {
                throw new IllegalStateException("state/log/transport are required");
            }
            if (selfId < 0 || selfId >= nodeCount) {
                throw new IllegalStateException("selfId must be in [0, nodeCount)");
            }
            if (heartbeatIntervalMs >= electionTimeoutBaseMs) {
                throw new IllegalStateException(
                        "heartbeat interval must be well below the election timeout (10:1 shape)");
            }
            return new RaftNode(this);
        }
    }
}
