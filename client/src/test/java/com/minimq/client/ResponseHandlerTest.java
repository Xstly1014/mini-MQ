package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;

import io.netty.channel.embedded.EmbeddedChannel;

class ResponseHandlerTest {

    @Test
    void completesFutureMatchingRequestId() {
        ConcurrentHashMap<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
        CompletableFuture<Frame> f = new CompletableFuture<>();
        pending.put(5L, f);

        EmbeddedChannel ch = new EmbeddedChannel(new ClientChannel.ResponseHandler(pending));
        ch.writeInbound(new Frame(MessageType.PRODUCE_ACK, 5, new byte[]{1}));

        Frame received = f.getNow(null);
        assertNotNull(received, "future must complete on response arrival");
        assertEquals(MessageType.PRODUCE_ACK, received.type());
        assertTrue(pending.isEmpty(), "completed entries leave the pending map");
        ch.close();
    }

    @Test
    void lateResponseForTimedOutRequestIsIgnored() {
        ConcurrentHashMap<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
        // id 9 already timed out and was removed — its late response arrives now
        EmbeddedChannel ch = new EmbeddedChannel(new ClientChannel.ResponseHandler(pending));
        ch.writeInbound(new Frame(MessageType.FETCH_RESP, 9, new byte[0]));
        // reaching here without throwing = late answers are dropped silently
        assertTrue(pending.isEmpty());
        ch.close();
    }

    @Test
    void disconnectFailsAllPendingFutures() throws Exception {
        ConcurrentHashMap<Long, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
        CompletableFuture<Frame> a = new CompletableFuture<>();
        CompletableFuture<Frame> b = new CompletableFuture<>();
        pending.put(1L, a);
        pending.put(2L, b);

        EmbeddedChannel ch = new EmbeddedChannel(new ClientChannel.ResponseHandler(pending));
        ch.close(); // fires channelInactive

        // both futures must be DONE (exceptionally) — never left hanging
        assertTrue(a.isCompletedExceptionally());
        assertTrue(b.isCompletedExceptionally());
        assertTrue(pending.isEmpty(), "inactive cleanup must clear the map");
    }
}
