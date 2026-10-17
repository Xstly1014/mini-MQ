package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

class ClusterClientTest {

    @Test
    void fetchStopsWhenLeaderChangesAndNextLeaderFetchSucceeds() throws Exception {
        EventLoopGroup acceptor = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup(1);
        Channel oldLeader = null;
        Channel newLeader = null;
        ClusterClient client = null;
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch oldFetchSent = new CountDownLatch(1);
            oldLeader = startServer(acceptor, workers, false, oldFetchSent);
            newLeader = startServer(acceptor, workers, true, new CountDownLatch(0));

            List<InetSocketAddress> nodes = Arrays.asList(
                    (InetSocketAddress) oldLeader.localAddress(),
                    (InetSocketAddress) newLeader.localAddress());
            client = new ClusterClient(2, nodes, new ClusterClient.Router() {
                @Override
                public void onFrame(Channel channel, Frame frame) {
                    // The test servers only receive FOLLOWER_FETCH requests.
                }
            }, new PartitionShield());

            final AtomicInteger currentLeader = new AtomicInteger(0);
            final ClusterClient transport = client;
            final MqProto.FetchRequest request = MqProto.FetchRequest.newBuilder()
                    .setTopic("t")
                    .setOffset(0)
                    .setMaxCount(1)
                    .build();

            java.util.concurrent.Future<java.io.IOException> oldFetch = caller.submit(
                    new java.util.concurrent.Callable<java.io.IOException>() {
                        @Override
                        public java.io.IOException call() {
                            try {
                                transport.fetchUntilLeaderChanges(0, request, 5000, currentLeader::get);
                                return null;
                            } catch (java.io.IOException expected) {
                                return expected;
                            }
                        }
                    });
            assertTrue(oldFetchSent.await(2, TimeUnit.SECONDS), "old leader did not receive fetch");
            currentLeader.set(1);
            java.io.IOException stopped = oldFetch.get(2, TimeUnit.SECONDS);
            assertTrue(stopped != null, "old fetch unexpectedly completed successfully");
            assertTrue(stopped.getMessage().contains("leader changed"), stopped.toString());
            assertNoPendingFetches(transport);

            MqProto.FetchResponse response = transport.fetchUntilLeaderChanges(
                    1, request, 5000, currentLeader::get);
            assertEquals(1, response.getRecordsCount());
            assertEquals("new-leader", response.getRecords(0).getPayload().toStringUtf8());
            assertNoPendingFetches(transport);
        } finally {
            if (client != null) {
                client.close();
            }
            if (oldLeader != null) {
                oldLeader.close().syncUninterruptibly();
            }
            if (newLeader != null) {
                newLeader.close().syncUninterruptibly();
            }
            caller.shutdownNow();
            workers.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            acceptor.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static void assertNoPendingFetches(ClusterClient client) throws Exception {
        Field pendingField = ClusterClient.class.getDeclaredField("pendingFetches");
        pendingField.setAccessible(true);
        assertTrue(((Map<?, ?>) pendingField.get(client)).isEmpty(),
                "completed fetch retained a pending request");
    }

    private static Channel startServer(EventLoopGroup acceptor, EventLoopGroup workers,
            final boolean respond, final CountDownLatch fetchSeen) throws Exception {
        return new ServerBootstrap()
                .group(acceptor, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new FrameDecoder(), new FrameEncoder(),
                                new SimpleChannelInboundHandler<Frame>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
                                        if (frame.type() != MessageType.FOLLOWER_FETCH) {
                                            return;
                                        }
                                        fetchSeen.countDown();
                                        if (!respond) {
                                            return;
                                        }
                                        MqProto.FetchResponse response = MqProto.FetchResponse.newBuilder()
                                                .addRecords(MqProto.RecordData.newBuilder()
                                                        .setOffset(0)
                                                        .setTopic("t")
                                                        .setPayload(ByteString.copyFromUtf8("new-leader")))
                                                .setNextOffset(1)
                                                .build();
                                        byte[] meta = response.toByteArray();
                                        byte[] body = new byte[4 + meta.length];
                                        java.nio.ByteBuffer.wrap(body).putInt(meta.length).put(meta);
                                        ctx.writeAndFlush(new Frame(MessageType.FETCH_RESP,
                                                frame.requestId(), body));
                                    }
                                });
                    }
                }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
    }
}
