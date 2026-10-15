package com.minimq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minimq.protocol.Frame;
import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;
import com.minimq.storage.CommitLog;

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

class BrokerServerTest {

    @TempDir
    Path tempDir;

    private static final int SEGMENT_SIZE = 256 * 1024;

    @Test
    void loopbackProduceFetchRoundTrip() throws Exception {
        BrokerConfig config = BrokerConfig.defaults()
                .port(0) // ephemeral port
                .storeDir(tempDir.toFile())
                .segmentSize(SEGMENT_SIZE);
        BrokerServer server = new BrokerServer(config);
        server.start();
        try {
            int port = server.port();
            assertTrue(port > 0);
            assertTrue(server.isRunning());

            EventLoopGroup group = new NioEventLoopGroup(1);
            final LinkedBlockingQueue<Frame> responses = new LinkedBlockingQueue<Frame>();
            try {
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
                                        new SimpleChannelInboundHandler<Frame>() {
                                            @Override
                                            protected void channelRead0(
                                                    ChannelHandlerContext ctx, Frame msg) {
                                                responses.offer(msg);
                                            }
                                        });
                            }
                        });
                Channel ch = b.connect("127.0.0.1", port).sync().channel();
                try {
                    // PRODUCE over the wire
                    ch.writeAndFlush(TestFrames.produce(1, "smoke", "hello")).sync();
                    Frame ack = responses.poll(5, TimeUnit.SECONDS);
                    assertNotNull(ack, "produce ack timed out");
                    assertEquals(MessageType.PRODUCE_ACK, ack.type());
                    assertEquals(1, ack.requestId());
                    MqProto.ProduceAck parsedAck = TestFrames.parseAck(ack);
                    assertEquals(MqProto.ProduceStatus.OK, parsedAck.getStatus());
                    assertEquals(1, parsedAck.getOffsetsCount());
                    assertTrue(parsedAck.getOffsets(0) >= 0);

                    // FETCH it back
                    ch.writeAndFlush(TestFrames.fetch(2, "smoke", 0, 10)).sync();
                    Frame resp = responses.poll(5, TimeUnit.SECONDS);
                    assertNotNull(resp, "fetch response timed out");
                    assertEquals(MessageType.FETCH_RESP, resp.type());
                    assertEquals(2, resp.requestId());
                    MqProto.FetchResponse parsed = TestFrames.parseFetch(resp);
                    assertEquals(1, parsed.getRecordsCount());
                    assertEquals("hello", parsed.getRecords(0).getPayload().toStringUtf8());
                    assertEquals("smoke", parsed.getRecords(0).getTopic());
                } finally {
                    ch.close().sync();
                }
            } finally {
                group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
            }
        } finally {
            server.shutdown();
        }

        // graceful shutdown flushed the mmap: the record survives reopen
        CommitLog reopened = CommitLog.open(tempDir.toFile(), SEGMENT_SIZE);
        try {
            assertEquals(1, reopened.readFrom(0, 10).size());
        } finally {
            reopened.close();
        }
    }

    @Test
    void shutdownIsIdempotentAndPortIsReleasable() throws Exception {
        BrokerConfig config = BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.toFile())
                .segmentSize(SEGMENT_SIZE);
        BrokerServer server = new BrokerServer(config);
        server.start();
        int port = server.port();
        server.shutdown();
        server.shutdown(); // second call: no-op, no exception
        assertTrue(!server.isRunning());

        // the port is actually released after graceful shutdown
        java.net.ServerSocket probe = new java.net.ServerSocket(port);
        probe.close();
    }

    @Test
    void doubleStartIsHarmless() throws Exception {
        BrokerConfig config = BrokerConfig.defaults()
                .port(0)
                .storeDir(tempDir.toFile())
                .segmentSize(SEGMENT_SIZE);
        BrokerServer server = new BrokerServer(config);
        try {
            server.start();
            server.start(); // second call: no-op
            assertTrue(server.isRunning());
        } finally {
            server.shutdown();
        }
    }
}
