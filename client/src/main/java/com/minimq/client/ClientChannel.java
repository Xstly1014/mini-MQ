package com.minimq.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.protocol.MessageType;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Single shared connection with request-response pairing: the pending map
 * (requestId -> future) is THE async fundamental of the whole client —
 * responses may arrive in any order, they are matched by requestId, never
 * by arrival order.
 *
 * Failure handling (M1 manual pit list):
 * <ul>
 *   <li>timeout — the scheduled task removes the entry and fails the future;
 *       a late response for an already-removed id is ignored,</li>
 *   <li>disconnect — channelInactive fails ALL pending futures, otherwise
 *       they would hang forever (future leak),</li>
 *   <li>close() — refuses new requests, fails pending ones, shuts the group.</li>
 * </ul>
 */
public final class ClientChannel implements Transport {

    private static final Logger LOG = Logger.getLogger(ClientChannel.class.getName());

    private final EventLoopGroup group;
    private final Channel channel;
    private final ConcurrentHashMap<Long, CompletableFuture<Frame>> pending;
    private final AtomicLong requestIds = new AtomicLong(1);
    private final ScheduledExecutorService timeoutScheduler;
    private final long timeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ClientChannel(EventLoopGroup group, Channel channel,
                          ConcurrentHashMap<Long, CompletableFuture<Frame>> pending, long timeoutMs) {
        this.group = group;
        this.channel = channel;
        this.pending = pending;
        this.timeoutMs = timeoutMs;
        this.timeoutScheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "mini-mq-client-timeout");
                t.setDaemon(true);
                return t;
            }
        });
    }

    public static ClientChannel connect(String host, int port, long timeoutMs)
            throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try {
            final ConcurrentHashMap<Long, CompletableFuture<Frame>> pending =
                    new ConcurrentHashMap<Long, CompletableFuture<Frame>>();
            Bootstrap b = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new FrameEncoder(),
                                    new FrameDecoder(),
                                    new ResponseHandler(pending));
                        }
                    });
            Channel channel = b.connect(host, port).sync().channel();
            return new ClientChannel(group, channel, pending, timeoutMs);
        } catch (InterruptedException e) {
            group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
            throw e;
        } catch (RuntimeException e) {
            group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).awaitUninterruptibly();
            throw e;
        }
    }

    @Override
    public CompletableFuture<Frame> request(MessageType type, byte[] body) {
        CompletableFuture<Frame> future = new CompletableFuture<Frame>();
        if (closed.get()) {
            future.completeExceptionally(new IllegalStateException("channel closed"));
            return future;
        }
        long id = requestIds.getAndIncrement();
        pending.put(id, future);
        timeoutScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                CompletableFuture<Frame> f = pending.remove(id);
                if (f != null) {
                    f.completeExceptionally(new TimeoutException(
                            "request " + id + " timed out after " + timeoutMs + "ms"));
                }
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);

        Frame frame = new Frame(type, id, body);
        channel.writeAndFlush(frame).addListener(f -> {
            if (!f.isSuccess()) {
                CompletableFuture<Frame> p = pending.remove(id);
                if (p != null) {
                    p.completeExceptionally(f.cause());
                }
            }
        });
        return future;
    }

    /** Number of requests awaiting a response (diagnostics / tests). */
    public int pendingCount() {
        return pending.size();
    }

    /**
     * Response side of the pairing. Static on purpose: it only touches the
     * shared pending map, and this way it is unit-testable with an
     * EmbeddedChannel and reusable if channels multiply (M3).
     */
    static final class ResponseHandler extends SimpleChannelInboundHandler<Frame> {
        private final ConcurrentHashMap<Long, CompletableFuture<Frame>> pending;

        ResponseHandler(ConcurrentHashMap<Long, CompletableFuture<Frame>> pending) {
            this.pending = pending;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame msg) {
            CompletableFuture<Frame> f = pending.remove(msg.requestId());
            if (f != null) {
                f.complete(msg);
            }
            // else: already timed out — a late answer is silently dropped
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failAll(pending, new IOException("connection closed"));
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "channel error, closing", cause);
            ctx.close();
        }
    }

    private static void failAll(ConcurrentHashMap<Long, CompletableFuture<Frame>> pending,
                                Throwable cause) {
        Iterator<Map.Entry<Long, CompletableFuture<Frame>>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue().completeExceptionally(cause);
            it.remove();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        channel.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
        failAll(pending, new IOException("channel closed"));
        timeoutScheduler.shutdownNow();
        // Fire-and-forget ON PURPOSE: awaiting the loop's graceful shutdown
        // deadlocks when close() is invoked from the loop's own thread. That
        // is exactly what FailoverTransport.invalidate does — a NOT_LEADER
        // ack completes the request future synchronously on the event loop,
        // and its whenComplete callback closes this very channel. The
        // shutdown task needs the same loop thread that would be blocked
        // awaiting it (M3-8: first-connect-to-a-follower hung the producer
        // forever with this exact stack).
        group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
    }
}
