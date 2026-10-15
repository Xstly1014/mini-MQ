package com.minimq.broker;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
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
                    Frame response = null;
                    try {
                        response = handler.handle(frame);
                    } catch (Throwable t) {
                        LOG.log(Level.WARNING, "request handling failed: " + frame, t);
                        response = handler.errorResponse(frame, t);
                    }
                    if (response != null) {
                        // Thread-safe: Netty hands the write to the channel's
                        // EventLoop; we never touch the pipeline directly.
                        channel.writeAndFlush(response);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Frame-level protocol errors (bad magic, oversized frame, unknown
        // type): the byte stream cannot be trusted anymore — drop the peer.
        LOG.log(Level.WARNING, "protocol error, closing " + ctx.channel(), cause);
        ctx.close();
    }
}
