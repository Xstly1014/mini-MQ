package com.minimq.broker;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.ZeroCopyFrame;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * EventLoop-side dispatcher: the ONLY thing it does on the IO thread is route
 * decoded frames to the business pool. Slow work (storage append, batch
 * decode) never touches the EventLoop — a blocked EventLoop stalls every
 * channel bound to it.
 *
 * Backpressure: when the bounded business pool rejects a task, the rejection
 * is translated into a semantic response (RETRY_LATER for produces) instead
 * of blocking or dropping silently. This is why MQ backpressure uses
 * AbortPolicy rather than CallerRunsPolicy: the caller here is the EventLoop,
 * and running the task inline would be exactly the blocking we must avoid.
 *
 * Zero-copy FETCH responses (M2 task 2b): a {@link ZeroCopyFrame} goes out
 * as ONE frame in two steps — the head (header + meta) through the ordinary
 * encoder, then each {@link FileRegion} straight to the transport (sendfile
 * on Linux). Channel writes are queued to the channel's EventLoop in
 * submission order, so head-then-regions ordering is guaranteed even though
 * we submit from a business-pool thread; a single flush at the end keeps the
 * whole frame in one TCP burst. Netty releases each region when its write
 * terminates (success, failure, or channel close); if submitting ever throws
 * before a region is handed over, we release it ourselves — FileRegion holds
 * a file handle, leaking it is not an option.
 */
@ChannelHandler.Sharable
public class DispatcherHandler extends SimpleChannelInboundHandler<Frame> {

    private static final Logger LOG = Logger.getLogger(DispatcherHandler.class.getName());

    private final Executor businessPool;
    private final BrokerRequestHandler handler;

    public DispatcherHandler(Executor businessPool, BrokerRequestHandler handler) {
        this.businessPool = businessPool;
        this.handler = handler;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Frame frame) {
        if (frame.type() == MessageType.HEARTBEAT) {
            return; // reserved for M3 lease heartbeats
        }
        final Channel channel = ctx.channel();
        try {
            businessPool.execute(new Runnable() {
                @Override
                public void run() {
                    Object response = null;
                    try {
                        response = handler.handle(frame);
                    } catch (Throwable t) {
                        LOG.log(Level.WARNING, "request handling failed: " + frame, t);
                        response = handler.errorResponse(frame, t);
                    }
                    if (response != null) {
                        // Thread-safe: Netty hands the write to the channel's
                        // EventLoop; we never touch the pipeline directly.
                        writeResponse(channel, response);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            Frame busy = handler.busyResponse(frame);
            if (busy != null) {
                ctx.writeAndFlush(busy);
            } else {
                LOG.warning("business pool saturated, dropped " + frame);
            }
        }
    }

    /**
     * Writes one response: an ordinary {@link Frame} in a single
     * writeAndFlush, or a {@link ZeroCopyFrame} as head + FileRegions.
     */
    private static void writeResponse(Channel channel, Object response) {
        if (!(response instanceof ZeroCopyFrame)) {
            channel.writeAndFlush(response);
            return;
        }
        ZeroCopyFrame zc = (ZeroCopyFrame) response;
        try {
            // BODY_LENGTH on the head already covers the regions' bytes, so
            // the peer's FrameDecoder reassembles them into one frame body.
            channel.write(zc.head());
            for (FileRegion region : zc.regions()) {
                channel.write(region);
            }
            channel.flush();
        } catch (Throwable t) {
            // Handing a region to the channel failed: Netty never saw it, so
            // it will NOT release it — that responsibility falls back to us.
            LOG.log(Level.WARNING, "zero-copy write submit failed, releasing " + zc, t);
            zc.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Frame-level protocol errors (bad magic, oversized frame, unknown
        // type): the byte stream cannot be trusted anymore — drop the peer.
        LOG.log(Level.WARNING, "protocol error, closing " + ctx.channel(), cause);
        ctx.close();
    }
}
