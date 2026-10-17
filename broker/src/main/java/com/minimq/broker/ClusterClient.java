package com.minimq.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Node-to-node cluster transport (M3): one lazily-established TCP connection
 * per peer, shared by the election (fire-and-forget VOTE / HEARTBEAT frames)
 * and the replication sync (request-response FOLLOWER_FETCH with requestId
 * pairing).
 *
 * <h3>Why one connection per peer is enough</h3>
 * Raft messages are tiny and infrequent (one heartbeat per interval, votes
 * only around elections); replication is exactly one in-flight fetch at a
 * time by design (FollowerSyncer is a single loop). Head-of-line blocking
 * between them cannot happen at any scale this project targets.
 *
 * <h3>Failure philosophy</h3>
 * A dead peer is <i>normal</i> — that is the entire premise of M3. Raft
 * sends therefore degrade to "drop and let the timers retry": a lost vote
 * or heartbeat is retransmitted by the next timer tick, which is how Raft
 * itself tolerates loss. The synchronous {@link #fetch} is the exception:
 * its caller needs an answer (or a timeout) to make progress, so connect
 * failures surface as IOException and the syncer backs off.
 */
final class ClusterClient {

    private static final Logger LOG = Logger.getLogger(ClusterClient.class.getName());

    /**
     * Inbound routing: election frames go to the election, fetch answers to
     * their waiter, and FOLLOWER_FETCH to the replication service WITH the
     * channel it arrived on (the response must go back the way it came).
     */
    interface Router {
        /**
         * A cluster frame arriving on a client-side connection. Covers
         * VOTE/VOTE_RESP/HEARTBEAT/HEARTBEAT_RESP and — crucially —
         * FOLLOWER_FETCH: on Windows a bidirectional connect pair can be
         * merged by TCP simultaneous open into ONE connection whose both
         * ends are ClusterClient pipelines, so a follower's fetch may arrive
         * here instead of on the server port. Routing it (rather than
         * dropping it) makes both topologies work.
         */
        void onFrame(Channel channel, Frame frame);
    }

    private final int selfId;
    private final List<InetSocketAddress> nodes;
    private final Router router;
    /**
     * M4 partition shield: a FETCH_RESP from a blocked peer dies like a timeout.
     */
    private final PartitionShield shield;
    private final NioEventLoopGroup group;
    private final Bootstrap bootstrap;
    private final ConcurrentMap<Integer, Channel> channels = new ConcurrentHashMap<Integer, Channel>();
    private final ConcurrentMap<Long, PendingFetch> pendingFetches = new ConcurrentHashMap<Long, PendingFetch>();
    private final AtomicLong requestIds = new AtomicLong(1);
    private volatile boolean closed;

    ClusterClient(int selfId, List<InetSocketAddress> nodes, Router router,
            PartitionShield shield) {
        this.selfId = selfId;
        this.nodes = nodes;
        this.router = router;
        this.shield = shield;
        this.group = new NioEventLoopGroup(1, new DefaultThreadFactory("mini-mq-cluster"));
        this.bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3_000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new FrameDecoder(),
                                new FrameEncoder(),
                                new PeerHandler());
                    }
                });
    }

    // ------------------------------------------------------------------
    // raft path: fire-and-forget, never blocks the caller
    // ------------------------------------------------------------------

    /**
     * Sends a VOTE / VOTE_RESP / HEARTBEAT / HEARTBEAT_RESP frame. Never
     * blocks, never throws: an unreachable peer drops the frame and the
     * protocol's timers produce the next one.
     */
    void send(int destId, Frame frame) {
        if (closed || destId == selfId || destId < 0 || destId >= nodes.size()) {
            return;
        }
        Channel ch = channels.get(destId);
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(frame);
            return;
        }
        bootstrap.connect(nodes.get(destId)).addListener((io.netty.channel.ChannelFuture f) -> {
            if (!f.isSuccess()) {
                LOG.fine("node-" + destId + " unreachable, frame dropped: " + frame);
                return;
            }
            Channel fresh = f.channel();
            Channel prev = channels.put(destId, fresh);
            if (prev != null && prev != fresh) {
                prev.close();
            }
            fresh.writeAndFlush(frame);
        });
    }

    // ------------------------------------------------------------------
    // replication path: one synchronous FOLLOWER_FETCH round trip
    // ------------------------------------------------------------------

    /**
     * Issues a FOLLOWER_FETCH and blocks for the paired FETCH_RESP (the
     * follower-sync thread's single in-flight request). The response body
     * follows the v1.2 layout [4B metaLen][meta] like every FETCH_RESP.
     */
    MqProto.FetchResponse fetch(int destId, MqProto.FetchRequest request, long timeoutMs)
            throws IOException {
        if (closed || destId < 0 || destId >= nodes.size()) {
            throw new IOException("invalid cluster peer node-" + destId);
        }
        Channel ch = connectSync(destId);
        long id = requestIds.incrementAndGet();
        CompletableFuture<MqProto.FetchResponse> future = new CompletableFuture<MqProto.FetchResponse>();
        pendingFetches.put(id, new PendingFetch(future, destId));
        try {
            ch.writeAndFlush(new Frame(MessageType.FOLLOWER_FETCH, id, request.toByteArray()));
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IOException("FOLLOWER_FETCH to node-" + destId + " timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("FOLLOWER_FETCH interrupted", e);
        } catch (Exception e) {
            throw new IOException("FOLLOWER_FETCH to node-" + destId + " failed", e);
        } finally {
            pendingFetches.remove(id);
        }
    }

    /**
     * A fetch that can be interrupted when Raft discovers a different
     * leader. PartitionShield deliberately leaves TCP connections open, so
     * waiting for the full replication timeout would otherwise pin the
     * follower's only sync thread on the old leader.
     */
    MqProto.FetchResponse fetchUntilLeaderChanges(int destId, MqProto.FetchRequest request,
            long timeoutMs, java.util.function.IntSupplier currentLeader)
            throws IOException {
        if (closed || destId < 0 || destId >= nodes.size()) {
            throw new IOException("invalid cluster peer node-" + destId);
        }
        Channel ch = connectSync(destId);
        long id = requestIds.incrementAndGet();
        CompletableFuture<MqProto.FetchResponse> future = new CompletableFuture<MqProto.FetchResponse>();
        pendingFetches.put(id, new PendingFetch(future, destId));
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            ch.writeAndFlush(new Frame(MessageType.FOLLOWER_FETCH, id, request.toByteArray()));
            while (true) {
                if (currentLeader.getAsInt() != destId) {
                    throw new IOException("leader changed while fetching from node-" + destId);
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("FOLLOWER_FETCH to node-" + destId + " timed out");
                }
                try {
                    return future.get(Math.min(TimeUnit.NANOSECONDS.toMillis(remaining), 100L),
                            TimeUnit.MILLISECONDS);
                } catch (TimeoutException stillWaiting) {
                    // Re-check the leader identity at short intervals.
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new IOException("FOLLOWER_FETCH to node-" + destId + " failed", e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("FOLLOWER_FETCH interrupted", e);
        } finally {
            pendingFetches.remove(id);
        }
    }

    private Channel connectSync(int destId) throws IOException {
        Channel ch = channels.get(destId);
        if (ch != null && ch.isActive()) {
            return ch;
        }
        try {
            ch = bootstrap.connect(nodes.get(destId)).sync().channel();
        } catch (Exception e) {
            throw new IOException("cannot reach node-" + destId + " at " + nodes.get(destId), e);
        }
        Channel prev = channels.put(destId, ch);
        if (prev != null && prev != ch) {
            prev.close();
        }
        return ch;
    }

    void close() {
        closed = true;
        for (Channel ch : channels.values()) {
            ch.close();
        }
        channels.clear();
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------
    // inbound
    // ------------------------------------------------------------------

    private void completeFetch(Frame frame) {
        PendingFetch pending = pendingFetches.remove(frame.requestId());
        if (pending == null) {
            return;
        }
        // Partition shield: the peer this response came from is blocked —
        // to the syncer it must look exactly like the leader went silent
        // (immediate failure, backoff, retry) rather than a real answer.
        if (shield.blocksInbound(pending.peerId)) {
            pending.future.completeExceptionally(
                    new IOException("node-" + pending.peerId + " partitioned (shield)"));
            return;
        }
        MqProto.FetchResponse resp = parsePrefixed(frame.body());
        if (resp != null) {
            pending.future.complete(resp);
        } else {
            pending.future.completeExceptionally(
                    new IOException("malformed FETCH_RESP for request " + frame.requestId()));
        }
    }

    /** A fetch in flight plus the peer it went to (for response attribution). */
    private static final class PendingFetch {
        final CompletableFuture<MqProto.FetchResponse> future;
        final int peerId;

        PendingFetch(CompletableFuture<MqProto.FetchResponse> future, int peerId) {
            this.future = future;
            this.peerId = peerId;
        }
    }

    /** Parses the v1.2 FETCH_RESP body layout [4B metaLen][meta]. */
    static MqProto.FetchResponse parsePrefixed(byte[] body) {
        if (body.length < 4) {
            return null;
        }
        int metaLen = ByteBuffer.wrap(body, 0, 4).getInt();
        if (metaLen <= 0 || 4 + metaLen > body.length) {
            return null;
        }
        try {
            return MqProto.FetchResponse.parseFrom(ByteBuffer.wrap(body, 4, metaLen));
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * Routes peer frames: election messages and fetches onward, fetch answers to
     * waiters.
     */
    private final class PeerHandler extends SimpleChannelInboundHandler<Frame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            switch (frame.type()) {
                case VOTE:
                case VOTE_RESP:
                case HEARTBEAT:
                case HEARTBEAT_RESP:
                case FOLLOWER_FETCH:
                    // FOLLOWER_FETCH included: see Router#onFrame — on a
                    // simultaneous-open connection this handler is the
                    // "server" for the follower's fetch, and the reply must
                    // ride this very channel (requestId pairing still works:
                    // the follower's PeerHandler completes FETCH_RESPs).
                    router.onFrame(ctx.channel(), frame);
                    break;
                case FETCH_RESP:
                    // ours by construction: consumer FETCH_RESP never arrives
                    // on a cluster connection (consumers dial the server port)
                    completeFetch(frame);
                    break;
                default:
                    // unknown on this channel: ignore, the frame layer stays
                    // the single place that can kill a connection
                    break;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // forget the cached channel so the next send reconnects
            channels.values().removeIf(c -> c == ctx.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "cluster connection error, closing " + ctx.channel(), cause);
            ctx.close();
        }
    }
}
