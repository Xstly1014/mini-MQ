package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * The request timeout must actually fire: a server that accepts and never
 * answers (the classic hung-peer case) leaves no future hanging and clears
 * the pending map.
 */
class ClientChannelTimeoutTest {

    @Test
    void requestTimesOutWhenPeerNeverAnswers() throws Exception {
        EventLoopGroup serverGroup = new NioEventLoopGroup(1);
        ServerBootstrap dummy = new ServerBootstrap()
                .group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 16)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // no handlers: bytes are read and dropped, never answered
                    }
                });
        int port = ((java.net.InetSocketAddress) dummy.bind(0).sync().channel()
                .localAddress()).getPort();
        try {
            ClientChannel channel = ClientChannel.connect("127.0.0.1", port, 150);
            try {
                channel.request(MessageType.FETCH, new byte[0]).get(2, TimeUnit.SECONDS);
                throw new AssertionError("request must time out");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof TimeoutException,
                        "expected TimeoutException, got: " + e.getCause());
            }
            assertEquals(0, channel.pendingCount(),
                    "timed-out request must be removed from the pending map");
            channel.close();
        } finally {
            serverGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
        }
    }

    @Test
    void closedChannelRejectsNewRequests() throws Exception {
        EventLoopGroup serverGroup = new NioEventLoopGroup(1);
        ServerBootstrap dummy = new ServerBootstrap()
                .group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                    }
                });
        int port = ((java.net.InetSocketAddress) dummy.bind(0).sync().channel()
                .localAddress()).getPort();
        try {
            ClientChannel channel = ClientChannel.connect("127.0.0.1", port, 5000);
            channel.close();
            try {
                channel.request(MessageType.FETCH, new byte[0]).get(1, TimeUnit.SECONDS);
                throw new AssertionError("closed channel must fail requests");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof IllegalStateException);
            }
        } finally {
            serverGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
        }
    }
}
