package com.minimq.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.gen.MqProto.VoteRequest;
import com.minimq.protocol.gen.MqProto.VoteResponse;
import com.minimq.replica.RaftNode.LogView;
import com.minimq.replica.RaftNode.Role;
import com.minimq.replica.RaftNode.Transport;

/**
 * RaftNode (M3-4) semantics over an in-memory transport: exactly-one-leader
 * under randomized timeouts (the manual's 20-round acceptance), the
 * up-to-date vote gate, PreVote protection with its no-PreVote counter
 * experiment, and term durability across a kill -9 restart.
 *
 * <p>
 * Timings are scaled down for test speed (heartbeat 15ms, election timeout
 * 80-160ms) but keep the etcd 10:1-ish shape.
 */
class RaftNodeTest {

    @TempDir
    File tempDir;

    /** Fast but well-shaped clock policy. */
    private static final long HEARTBEAT_MS = 15L;
    private static final long ELECTION_BASE_MS = 80L;

    // ------------------------------------------------------------------
    // in-memory cluster harness
    // ------------------------------------------------------------------

    /** A fixed log view: what the node believes its log currency is. */
    static final class FixedLog implements LogView {
        volatile long endOffset;
        volatile long epoch;

        FixedLog(long epoch, long endOffset) {
            this.epoch = epoch;
            this.endOffset = endOffset;
        }

        @Override
        public long logEndOffset() {
            return endOffset;
        }

        @Override
        public long lastLogEpoch() {
            return epoch;
        }
    }

    /**
     * Direct-delivery transport with a partition knob: a muted node neither
     * sends nor receives (messages to/from it are dropped on the floor).
     */
    static final class InMemoryBus implements Transport {
        final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<Integer, RaftNode>();
        final Set<Integer> muted = Collections.newSetFromMap(
                new ConcurrentHashMap<Integer, Boolean>());
        final List<VoteResponse> responses = Collections.synchronizedList(
                new ArrayList<VoteResponse>());

        void register(RaftNode node) {
            nodes.put(node.selfId(), node);
        }

        void mute(int nodeId) {
            muted.add(nodeId);
        }

        void unmute(int nodeId) {
            muted.remove(nodeId);
        }

        private boolean cut(int senderId, int destId) {
            return muted.contains(senderId) || muted.contains(destId);
        }

        @Override
        public void sendVoteRequest(int destId, VoteRequest request) {
            if (cut(request.getCandidateId(), destId)) {
                return;
            }
            RaftNode target = nodes.get(destId);
            if (target != null) {
                target.handleVoteRequest(request);
            }
        }

        @Override
        public void sendVoteResponse(int destId, VoteResponse response) {
            responses.add(response);
            if (cut(response.getResponderId(), destId)) {
                return;
            }
            RaftNode target = nodes.get(destId);
            if (target != null) {
                target.handleVoteResponse(response);
            }
        }

        @Override
        public void sendHeartbeatRequest(int destId,
                com.minimq.protocol.gen.MqProto.HeartbeatRequest request) {
            if (cut(request.getLeaderId(), destId)) {
                return;
            }
            RaftNode target = nodes.get(destId);
            if (target != null) {
                target.handleHeartbeatRequest(request);
            }
        }

        @Override
        public void sendHeartbeatResponse(int destId,
                com.minimq.protocol.gen.MqProto.HeartbeatResponse response) {
            if (cut(response.getResponderId(), destId)) {
                return;
            }
            RaftNode target = nodes.get(destId);
            if (target != null) {
                target.handleHeartbeatResponse(response);
            }
        }
    }

    private RaftNode buildNode(int id, int nodeCount, InMemoryBus bus, LogView log,
            RaftNode.Listener listener, boolean preVote) throws IOException {
        RaftState state = RaftState.open(
                new File(new File(tempDir, "node-" + id), "raft-state"));
        RaftNode node = RaftNode.newBuilder()
                .selfId(id)
                .nodeCount(nodeCount)
                .state(state)
                .log(log)
                .transport(bus)
                .listener(listener)
                .heartbeatIntervalMs(HEARTBEAT_MS)
                .electionTimeoutBaseMs(ELECTION_BASE_MS)
                .preVoteEnabled(preVote)
                .build();
        bus.register(node);
        return node;
    }

    private static RaftNode awaitSingleLeader(long timeoutMs, RaftNode... nodes)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode leader = null;
            int leaders = 0;
            for (RaftNode n : nodes) {
                if (n.role() == Role.LEADER) {
                    leader = n;
                    leaders++;
                }
            }
            if (leaders == 1) {
                return leader;
            }
            Thread.sleep(5);
        }
        StringBuilder roles = new StringBuilder();
        for (RaftNode n : nodes) {
            roles.append("node-").append(n.selfId()).append('=')
                    .append(n.role()).append("/term=").append(n.term()).append(' ');
        }
        fail("no single leader within " + timeoutMs + "ms: " + roles);
        return null; // unreachable
    }

    private static void shutdownAll(RaftNode... nodes) {
        for (RaftNode n : nodes) {
            n.shutdown();
        }
    }

    // ------------------------------------------------------------------
    // acceptance tests
    // ------------------------------------------------------------------

    /**
     * The manual's 20-round acceptance: with randomized timeouts, three
     * nodes launched concurrently settle on EXACTLY one leader every single
     * time. Randomization is doing the arbitration, so flakiness here is a
     * real finding, never "just a test issue".
     */
    @Test
    void threeNodesElectExactlyOneLeaderTwentyRounds() throws Exception {
        for (int round = 0; round < 20; round++) {
            File roundDir = new File(tempDir, "round-" + round);
            roundDir.mkdirs();
            InMemoryBus bus = new InMemoryBus();
            RaftNode[] nodes = new RaftNode[3];
            try {
                for (int i = 0; i < 3; i++) {
                    File stateDir = new File(roundDir, "node-" + i);
                    stateDir.mkdirs();
                    RaftState state = RaftState.open(new File(stateDir, "raft-state"));
                    RaftNode node = RaftNode.newBuilder()
                            .selfId(i).nodeCount(3)
                            .state(state)
                            .log(new FixedLog(0, 0))
                            .transport(bus)
                            .heartbeatIntervalMs(HEARTBEAT_MS)
                            .electionTimeoutBaseMs(ELECTION_BASE_MS)
                            .build();
                    bus.register(node);
                    nodes[i] = node;
                }
                for (RaftNode n : nodes) {
                    n.start();
                }
                RaftNode leader = awaitSingleLeader(3_000, nodes);
                assertTrue(leader.term() >= 1, "a leader always holds a campaign term");
            } finally {
                shutdownAll(nodes);
            }
        }
    }

    /** The single-node cluster (M3-6 zero-regression mode): majority of one. */
    @Test
    void singleNodeClusterElectsItselfImmediately() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        RaftState state = RaftState.open(new File(new File(tempDir, "solo"), "raft-state"));
        RaftNode solo = RaftNode.newBuilder()
                .selfId(0).nodeCount(1)
                .state(state)
                .log(new FixedLog(0, 0))
                .transport(bus)
                .heartbeatIntervalMs(HEARTBEAT_MS)
                .electionTimeoutBaseMs(ELECTION_BASE_MS)
                .build();
        solo.start();
        try {
            long deadline = System.currentTimeMillis() + 1_000;
            while (solo.role() != Role.LEADER && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(Role.LEADER, solo.role(), "majority of 1: the first timeout wins");
        } finally {
            solo.shutdown();
        }
    }

    /**
     * Iron rule #4 end to end: a candidate whose (epoch, offset) is behind
     * the voter's log gets refused even with a shining new term — the vote
     * gate compares log currency, not term size.
     */
    @Test
    void laggingCandidateGetsNoVote() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        RaftNode voter = buildNode(0, 3, bus, new FixedLog(3, 5000), null, true);
        voter.start();
        try {
            // a stale-log candidate with a huge term
            voter.handleVoteRequest(VoteRequest.newBuilder()
                    .setCandidateId(2).setTerm(99).setPreVote(false)
                    .setLastLogEpoch(1).setLastLogOffset(100)
                    .build());
            long deadline = System.currentTimeMillis() + 500;
            while (bus.responses.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertFalse(bus.responses.isEmpty(), "voter must answer");
            VoteResponse resp = bus.responses.get(0);
            assertFalse(resp.getGranted(), "a lagging candidate must be refused");
            assertEquals(99L, resp.getTerm(), "the voter adopted the big term though");

            // an equally-current candidate IS granted
            bus.responses.clear();
            voter.handleVoteRequest(VoteRequest.newBuilder()
                    .setCandidateId(2).setTerm(100).setPreVote(false)
                    .setLastLogEpoch(3).setLastLogOffset(5000)
                    .build());
            deadline = System.currentTimeMillis() + 500;
            while (bus.responses.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertTrue(bus.responses.get(0).getGranted(),
                    "an at-least-as-current candidate earns the vote");
        } finally {
            voter.shutdown();
        }
    }

    /**
     * PreVote acceptance: a node isolated from the quorum keeps timing out
     * and keeps probing, but its term never moves; on healing it rejoins as
     * a follower under the SAME incumbent, same term, no disruption.
     */
    @Test
    void preVoteHealedIsolateDoesNotDisturbIncumbent() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        RaftNode[] nodes = new RaftNode[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = buildNode(i, 3, bus, new FixedLog(0, 0), null, true);
        }
        for (RaftNode n : nodes) {
            n.start();
        }
        try {
            RaftNode leader = awaitSingleLeader(3_000, nodes);
            long incumbentTerm = leader.term();

            // isolate node X: it can neither probe nor hear heartbeats
            RaftNode isolate = null;
            for (RaftNode n : nodes) {
                if (n != leader) {
                    isolate = n;
                }
            }
            bus.mute(isolate.selfId());
            long muteMs = 4 * ELECTION_BASE_MS; // several election timeouts pass
            long deadline = System.currentTimeMillis() + muteMs;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(incumbentTerm, isolate.term(),
                    "PreVote must not inflate the isolate's term");
            assertNotEquals(Role.LEADER, isolate.role());

            // heal: the isolate falls back in line, the incumbent is untouched
            bus.unmute(isolate.selfId());
            Thread.sleep(4 * ELECTION_BASE_MS);
            assertEquals(Role.LEADER, leader.role(), "incumbent keeps its seat");
            assertEquals(incumbentTerm, leader.term(), "no new election was forced");
            assertEquals(Role.FOLLOWER, isolate.role(), "healed node rejoins as follower");
        } finally {
            shutdownAll(nodes);
        }
    }

    /**
     * The counter experiment, same choreography with PreVote OFF: the
     * isolate's term inflates while partitioned, and on healing the
     * incumbent is deposed at the first contact — demonstrating exactly the
     * disruption PreVote exists to prevent.
     */
    @Test
    void withoutPreVoteHealedIsolateDeposesIncumbent() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        final List<String> roleLog = Collections.synchronizedList(new ArrayList<String>());
        RaftNode[] nodes = new RaftNode[3];
        for (int i = 0; i < 3; i++) {
            final int id = i;
            nodes[i] = buildNode(i, 3, bus, new FixedLog(0, 0), new RaftNode.Listener() {
                @Override
                public void onRoleChange(int nodeId, Role newRole, long term) {
                    roleLog.add(nodeId + "->" + newRole + "@" + term);
                }
            }, false); // PreVote disabled
        }
        for (RaftNode n : nodes) {
            n.start();
        }
        try {
            RaftNode leader = awaitSingleLeader(3_000, nodes);
            long incumbentTerm = leader.term();

            RaftNode isolate = null;
            for (RaftNode n : nodes) {
                if (n != leader) {
                    isolate = n;
                }
            }
            bus.mute(isolate.selfId());
            long deadline = System.currentTimeMillis() + 4 * ELECTION_BASE_MS;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(isolate.term() > incumbentTerm,
                    "without PreVote the isolate inflates its term while partitioned");

            // heal: the inflated term demotes the incumbent at first contact
            bus.unmute(isolate.selfId());
            deadline = System.currentTimeMillis() + 2_000;
            boolean deposed = false;
            while (System.currentTimeMillis() < deadline) {
                if (leader.role() != Role.LEADER || leader.term() > incumbentTerm) {
                    deposed = true;
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(deposed, "the incumbent must be deposed by the inflated term");
        } finally {
            shutdownAll(nodes);
        }
    }

    /**
     * Iron rule #2 end to end through the node: the process "crashes" after
     * granting a vote, restarts on the same state file, and still refuses a
     * second vote in the spent term.
     */
    @Test
    void restartedNodeRefusesSecondVoteInSpentTerm() throws Exception {
        InMemoryBus bus = new InMemoryBus();
        File stateFile = new File(new File(tempDir, "node-0"), "raft-state");
        stateFile.getParentFile().mkdirs();

        RaftNode first = RaftNode.newBuilder()
                .selfId(0).nodeCount(3)
                .state(RaftState.open(stateFile))
                .log(new FixedLog(0, 0))
                .transport(bus)
                .heartbeatIntervalMs(HEARTBEAT_MS)
                .electionTimeoutBaseMs(ELECTION_BASE_MS)
                .build();
        first.start();
        first.handleVoteRequest(VoteRequest.newBuilder()
                .setCandidateId(1).setTerm(5).setPreVote(false)
                .setLastLogEpoch(0).setLastLogOffset(0)
                .build());
        long deadline = System.currentTimeMillis() + 500;
        while (bus.responses.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(bus.responses.get(0).getGranted(), "first vote granted");
        first.shutdown(); // kill -9: no clean shutdown hooks

        RaftState reborn = RaftState.open(stateFile);
        RaftNode second = RaftNode.newBuilder()
                .selfId(0).nodeCount(3)
                .state(reborn)
                .log(new FixedLog(0, 0))
                .transport(bus)
                .heartbeatIntervalMs(HEARTBEAT_MS)
                .electionTimeoutBaseMs(ELECTION_BASE_MS)
                .build();
        second.start();
        try {
            bus.responses.clear();
            second.handleVoteRequest(VoteRequest.newBuilder()
                    .setCandidateId(2).setTerm(5).setPreVote(false)
                    .setLastLogEpoch(0).setLastLogOffset(0)
                    .build());
            deadline = System.currentTimeMillis() + 500;
            while (bus.responses.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertFalse(bus.responses.get(bus.responses.size() - 1).getGranted(),
                    "the restarted node must remember its spent vote");
        } finally {
            second.shutdown();
        }
    }
}
