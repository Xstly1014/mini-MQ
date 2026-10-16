package com.minimq.broker;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.replica.EpochCheckpoint;
import com.minimq.replica.FollowerSyncer;
import com.minimq.replica.RaftNode;
import com.minimq.replica.RaftState;
import com.minimq.replica.ReplicaManager;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Cluster wiring (M3-6): the whole replication/election assembly behind one
 * front door — {@code Kafka data plane (fetch/ISR/HW) + Raft election
 * kernel}, the KRaft shape.
 *
 * <h3>What runs where</h3>
 * <ul>
 *   <li><b>RaftNode</b> owns the election on its own single-threaded event
 *   loop; this class is its {@link RaftNode.Transport} /
 *   {@link RaftNode.Listener} seam — role changes promote/demote the rest
 *   of the assembly.</li>
 *   <li><b>ReplicaManager</b> (leader side) tracks the ISR and the HW; the
 *   sweep timer keeps a dead follower from pinning the watermark.</li>
 *   <li><b>FollowerSyncer</b> (follower side) pulls raw bytes over
 *   {@link ClusterClient} and appends them verbatim.</li>
 *   <li><b>Replication thread</b>: FOLLOWER_FETCH requests land here (their
 *   own frame TYPE routes them without decoding), keeping replication
 *   traffic off the business pool — a stalled sync must never steal
 *   producer/consumer threads.</li>
 * </ul>
 *
 * <h3>Leader transitions</h3>
 * Winning an election records {@code (epoch, LEO)} in the
 * {@link EpochCheckpoint} BEFORE serving follower fetches: that entry is the
 * arbitration table — a follower whose position crossed the epoch boundary
 * gets told to TRUNCATE back to it, which is how a revived stale leader's
 * dirty tail is cleaned up (KIP-101 shape). The HW seeds optimistically at
 * the winner's LEO and the first follower reports pull it to the truth (see
 * ReplicaManager).
 *
 * <h3>Sync restart</h3>
 * A follower whose syncer died on leaderLost never sees a role CHANGE when
 * the next leader's heartbeat arrives (it was FOLLOWER all along) — that is
 * exactly what {@link RaftNode.Listener}'s onLeaderSeen callback exists
 * for.
 */
public final class ReplicationService
        implements RaftNode.Transport, FollowerSyncer.LeaderChannel,
        ReplicationGate, ReplicationInbound, ClusterClient.Router {

    private static final Logger LOG = Logger.getLogger(ReplicationService.class.getName());

    private final BrokerConfig config;
    private final CommitLog log;
    private final ConsumeQueueStore store;
    private final int selfId;
    private final List<InetSocketAddress> nodes;

    private final RaftState raftState;
    private final EpochCheckpoint checkpoint;
    private final RaftNode node;
    private final ReplicaManager replicas;
    private final FollowerSyncer syncer;
    private final ClusterClient client;
    private final ExecutorService replicationThread;
    private final ScheduledExecutorService sweeper;

    /** Data-plane authority (volatile: read on business pool, raft thread writes). */
    private volatile boolean leader;
    /** Who we believe leads (hint source); -1 = no leader seen yet. */
    private volatile int lastSeenLeaderId = -1;

    ReplicationService(BrokerConfig config, CommitLog log, ConsumeQueueStore store)
            throws IOException {
        this.config = config;
        this.log = log;
        this.store = store;
        this.selfId = config.getNodeId();
        this.nodes = config.getClusterNodes();

        File metaDir = metaDir(config);
        this.raftState = RaftState.open(new File(metaDir, "raft-state"));
        this.checkpoint = EpochCheckpoint.open(new File(metaDir, "leader-epoch-checkpoint"));

        List<Integer> followerIds = new ArrayList<Integer>();
        for (int id = 0; id < nodes.size(); id++) {
            if (id != selfId) {
                followerIds.add(id);
            }
        }
        this.replicas = ReplicaManager.newStandby(selfId, followerIds,
                config.getMinIsr(), config.getReplicaLagMaxMs(),
                new java.util.function.LongSupplier() {
                    @Override
                    public long getAsLong() {
                        return log.maxOffset();
                    }
                });

        this.node = RaftNode.newBuilder()
                .selfId(selfId)
                .nodeCount(nodes.size())
                .state(raftState)
                .log(new RaftNode.LogView() {
                    @Override
                    public long logEndOffset() {
                        return log.maxOffset();
                    }

                    @Override
                    public long lastLogEpoch() {
                        return checkpoint.latestEpoch();
                    }
                })
                .transport(this)
                .listener(new RaftNode.Listener() {
                    @Override
                    public void onRoleChange(int nodeId, RaftNode.Role newRole, long term) {
                        if (newRole == RaftNode.Role.LEADER) {
                            onElectedLeader(term);
                        } else {
                            onDemoted();
                        }
                    }

                    @Override
                    public void onLeaderSeen(int nodeId, int leaderId, long term) {
                        onHeartbeatFromLeader(leaderId);
                    }
                })
                .heartbeatIntervalMs(config.getHeartbeatIntervalMs())
                .electionTimeoutBaseMs(config.getElectionTimeoutBaseMs())
                .build();

        this.syncer = new FollowerSyncer(log, store, checkpoint, this,
                config.getReplicaMaxFetchBytes());
        this.client = new ClusterClient(selfId, nodes, this);
        this.replicationThread = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("mini-mq-replication"));
        this.sweeper = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("mini-mq-isr-sweep"));
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    /** Starts the election; leadership follows from the votes, never config. */
    public void start() {
        sweeper.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (leader) {
                        replicas.sweep(); // shrink-before-stall: the HW deadlock killer
                    }
                } catch (Throwable t) {
                    LOG.log(Level.WARNING, "ISR sweep failed", t);
                }
            }
        }, config.getReplicaLagMaxMs(),
                Math.max(config.getReplicaLagMaxMs() / 2, 100L), TimeUnit.MILLISECONDS);
        node.start();
    }

    /** Stops election, sync, sweep, and the cluster connections. */
    public void close() {
        node.shutdown();
        syncer.shutdown();
        sweeper.shutdownNow();
        replicationThread.shutdownNow();
        replicas.becomeFollower(); // release any acks=all producers still parked
        client.close();
    }

    // ------------------------------------------------------------------
    // role transitions (raft thread)
    // ------------------------------------------------------------------

    private void onElectedLeader(long term) {
        synchronized (this) {
            leader = true;
            lastSeenLeaderId = selfId;
            syncer.shutdown(); // a leader never syncs from anyone
        }
        try {
            // The arbitration entry: everything this epoch accepts starts at
            // the winner's LEO. Must be durable before follower fetches are
            // served — a crash here means an epoch with no boundary row.
            checkpoint.recordEpoch(term, log.maxOffset());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "cannot persist epoch " + term + " boundary", e);
        }
        replicas.becomeLeader();
        LOG.info("node-" + selfId + " elected leader, epoch " + term
                + ", log end " + log.maxOffset());
    }

    private void onDemoted() {
        synchronized (this) {
            leader = false;
            replicas.becomeFollower(); // wakes parked acks=all producers (they fail)
            ensureFollowerSyncer();
        }
    }

    private void onHeartbeatFromLeader(int leaderId) {
        lastSeenLeaderId = leaderId;
        synchronized (this) {
            ensureFollowerSyncer();
        }
    }

    /** Caller holds `this`; idempotent — heartbeats arrive every interval. */
    private void ensureFollowerSyncer() {
        if (!leader && !syncer.isRunning()) {
            syncer.start();
        }
    }

    // ------------------------------------------------------------------
    // RaftNode.Transport (raft thread): fire-and-forget over the cluster
    // ------------------------------------------------------------------

    @Override
    public void sendVoteRequest(int destId, MqProto.VoteRequest request) {
        client.send(destId, new Frame(MessageType.VOTE, nextRequestId(), request.toByteArray()));
    }

    @Override
    public void sendVoteResponse(int destId, MqProto.VoteResponse response) {
        client.send(destId, new Frame(MessageType.VOTE_RESP, nextRequestId(),
                response.toByteArray()));
    }

    @Override
    public void sendHeartbeatRequest(int destId, MqProto.HeartbeatRequest request) {
        client.send(destId, new Frame(MessageType.HEARTBEAT, nextRequestId(),
                request.toByteArray()));
    }

    @Override
    public void sendHeartbeatResponse(int destId, MqProto.HeartbeatResponse response) {
        client.send(destId, new Frame(MessageType.HEARTBEAT_RESP, nextRequestId(),
                response.toByteArray()));
    }

    private static long nextRequestId() {
        return REQUEST_IDS.incrementAndGet();
    }

    private static final java.util.concurrent.atomic.AtomicLong REQUEST_IDS =
            new java.util.concurrent.atomic.AtomicLong(1);

    // ------------------------------------------------------------------
    // FollowerSyncer.LeaderChannel (follower sync thread)
    // ------------------------------------------------------------------

    @Override
    public FollowerSyncer.FetchedBatch fetch(long fromOffset, long followerEpoch, int maxBytes)
            throws IOException {
        int leaderId = lastSeenLeaderId;
        if (leaderId < 0 || leaderId == selfId) {
            return FollowerSyncer.FetchedBatch.leaderLost();
        }
        MqProto.FetchRequest req = MqProto.FetchRequest.newBuilder()
                .setOffset(fromOffset)
                .setFollowerId(selfId)
                .setFollowerEpoch(followerEpoch)
                .setMaxBytes(maxBytes)
                .build();
        MqProto.FetchResponse resp = client.fetch(leaderId, req,
                config.getReplicationAwaitTimeoutMs());
        return toBatch(resp);
    }

    /** Wire shape -> syncer shape: error codes become directives. */
    private static FollowerSyncer.FetchedBatch toBatch(MqProto.FetchResponse resp) {
        MqProto.ErrorCode code = resp.getErrorCode();
        if (code == MqProto.ErrorCode.NOT_LEADER || code == MqProto.ErrorCode.STALE_EPOCH) {
            return FollowerSyncer.FetchedBatch.leaderLost();
        }
        if (code == MqProto.ErrorCode.TRUNCATE_TO) {
            return FollowerSyncer.FetchedBatch.truncate(resp.getLeaderEpoch(),
                    resp.getErrorValue());
        }
        List<FollowerSyncer.RawRecord> records = new ArrayList<FollowerSyncer.RawRecord>();
        for (MqProto.RecordData rd : resp.getRecordsList()) {
            records.add(new FollowerSyncer.RawRecord(rd.getOffset(), rd.getPayload().toByteArray()));
        }
        return FollowerSyncer.FetchedBatch.ok(resp.getLeaderEpoch(),
                resp.getEpochStartOffset(), resp.getHighWatermark(), records);
    }

    // ------------------------------------------------------------------
    // ReplicationInbound (server EventLoop) + ClusterClient.Router (client IO)
    // ------------------------------------------------------------------

    /**
     * One entry for both transports: frames from the server pipeline
     * (DispatcherHandler) and from the ClusterClient's own connections
     * (PeerHandler — where a simultaneous-open connection can deliver a
     * follower's fetch). Election messages enqueue onto the Raft inbox —
     * safe on any thread; FOLLOWER_FETCH (the heavy one: mmap reads,
     * arbitration, bookkeeping) moves to the replication thread, and the
     * response goes back on the channel it came from.
     */
    @Override
    public void onFrame(Channel channel, Frame frame) {
        routeRaftFrame(frame, channel);
    }

    private void routeRaftFrame(Frame frame, final Channel replyTo) {
        try {
            switch (frame.type()) {
                case VOTE:
                    node.handleVoteRequest(MqProto.VoteRequest.parseFrom(frame.body()));
                    return;
                case VOTE_RESP:
                    node.handleVoteResponse(MqProto.VoteResponse.parseFrom(frame.body()));
                    return;
                case HEARTBEAT:
                    node.handleHeartbeatRequest(MqProto.HeartbeatRequest.parseFrom(frame.body()));
                    return;
                case HEARTBEAT_RESP:
                    node.handleHeartbeatResponse(MqProto.HeartbeatResponse.parseFrom(frame.body()));
                    return;
                case FOLLOWER_FETCH:
                    replicationThread.execute(new Runnable() {
                        @Override
                        public void run() {
                            serveFollowerFetch(replyTo, frame);
                        }
                    });
                    return;
                default:
                    return;
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.WARNING, "malformed cluster frame dropped: " + frame, e);
        }
    }

    // ------------------------------------------------------------------
    // FOLLOWER_FETCH service (replication thread)
    // ------------------------------------------------------------------

    private void serveFollowerFetch(Channel channel, Frame frame) {
        if (channel == null || !channel.isActive()) {
            return; // requester hung up mid-flight; it will retry
        }
        MqProto.FetchRequest req;
        try {
            req = MqProto.FetchRequest.parseFrom(frame.body());
        } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.WARNING, "malformed FOLLOWER_FETCH dropped: " + frame, e);
            return;
        }
        MqProto.FetchResponse resp = leaderFollowerFetch(req);
        channel.writeAndFlush(prefixedFrame(MessageType.FETCH_RESP, frame.requestId(), resp));
    }

    /**
     * The leader side of replication: arbitrate the epoch, stream raw bytes,
     * and record the follower's position (which is what moves the HW, which
     * is what wakes acks=all producers — one fetch, three jobs).
     */
    private MqProto.FetchResponse leaderFollowerFetch(MqProto.FetchRequest req) {
        if (!leader) {
            return notLeaderResponse();
        }
        long epoch = checkpoint.latestEpoch();
        long epochStart = checkpoint.startOffsetFor(epoch);
        if (req.getFollowerEpoch() > epoch) {
            // The follower synced from a fresher leader than my checkpoint
            // knows: I am the stale one, not it.
            return notLeaderResponse();
        }
        if (req.getFollowerEpoch() < epoch && req.getOffset() > epochStart) {
            // The arbitration: the follower's tail crossed into a lineage
            // this epoch superseded. Cut it back to the boundary; its next
            // fetch re-enters the current history.
            return MqProto.FetchResponse.newBuilder()
                    .setErrorCode(MqProto.ErrorCode.TRUNCATE_TO)
                    .setErrorValue(epochStart)
                    .setLeaderEpoch(epoch)
                    .setHighWatermark(replicas.highWatermark())
                    .build();
        }
        List<CommitLog.RawRecord> raw = log.readRawFrom(
                req.getOffset(), req.getMaxBytes() > 0
                        ? Math.min(req.getMaxBytes(), config.getReplicaMaxFetchBytes())
                        : config.getReplicaMaxFetchBytes());
        long next = req.getOffset();
        if (!raw.isEmpty()) {
            CommitLog.RawRecord last = raw.get(raw.size() - 1);
            next = last.offset + last.raw.length;
        }
        // Record the position BEFORE answering: the ack the follower bases
        // on this response must already be covered by the bookkeeping.
        long hw = replicas.onFollowerFetch(req.getFollowerId(), req.getOffset());
        MqProto.FetchResponse.Builder b = MqProto.FetchResponse.newBuilder()
                .setLeaderEpoch(epoch)
                .setEpochStartOffset(epochStart)
                .setNextOffset(next)
                .setHighWatermark(hw);
        for (CommitLog.RawRecord r : raw) {
            b.addRecords(MqProto.RecordData.newBuilder()
                    .setOffset(r.offset)
                    .setSize(r.raw.length)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(r.raw)));
        }
        return b.build();
    }

    private MqProto.FetchResponse notLeaderResponse() {
        MqProto.FetchResponse.Builder b = MqProto.FetchResponse.newBuilder()
                .setErrorCode(MqProto.ErrorCode.NOT_LEADER)
                .setNextOffset(-1L);
        String hint = leaderHint();
        if (hint != null) {
            b.setLeaderHint(hint);
        }
        return b.build();
    }

    // ------------------------------------------------------------------
    // ReplicationGate (business pool)
    // ------------------------------------------------------------------

    @Override
    public boolean isLeader() {
        return leader;
    }

    @Override
    public long highWatermark() {
        return leader ? replicas.highWatermark() : -1L;
    }

    @Override
    public String leaderHint() {
        int id = lastSeenLeaderId;
        if (id < 0 || id >= nodes.size()) {
            return null;
        }
        InetSocketAddress addr = nodes.get(id);
        return addr.getHostString() + ":" + addr.getPort();
    }

    @Override
    public boolean acksAllAdmissible() {
        return leader && replicas.acksAllAdmissible();
    }

    @Override
    public boolean awaitReplicated(long offset, long timeoutMs) throws InterruptedException {
        return leader && replicas.awaitReplicated(offset, timeoutMs);
    }

    @Override
    public void onLocalAppend() {
        replicas.onLocalAppend();
    }

    // ------------------------------------------------------------------
    // monitoring
    // ------------------------------------------------------------------

    /** Election role (monitoring / tests). */
    public RaftNode.Role role() {
        return node.role();
    }

    /** Election term (monitoring / tests). */
    public long term() {
        return node.term();
    }

    /** ISR snapshot including the leader (monitoring / tests). */
    public java.util.Set<Integer> isrSnapshot() {
        return replicas.isr();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static File metaDir(BrokerConfig config) {
        File storeDir = config.getStoreDir();
        File parent = storeDir.getParentFile();
        return parent != null ? new File(parent, "meta") : new File("meta");
    }

    /** FETCH_RESP bodies are [4B metaLen][meta] (the v1.2 layout, protocol.md). */
    private static Frame prefixedFrame(MessageType type, long requestId,
            com.google.protobuf.GeneratedMessageV3 message) {
        int metaLen = message.getSerializedSize();
        byte[] body = new byte[4 + metaLen];
        ByteBuffer.wrap(body).putInt(metaLen);
        try {
            message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(body, 4, metaLen));
        } catch (IOException e) {
            throw new RuntimeException("protobuf write to heap array failed", e); // cannot happen
        }
        return new Frame(type, requestId, body);
    }
}
