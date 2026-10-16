package com.minimq.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * FailoverTransport against scripted nodes (no broker): every redirect path
 * from the M3 manual — NOT_LEADER{hint}, NOT_LEADER without hint, dead
 * connections, lying hints, exhausted budgets — plus the pinning behavior
 * that makes redirects amortize to zero in steady state.
 */
class FailoverTransportTest {

    /** Decides one request's fate. */
    interface Responder {
        CompletableFuture<Frame> respond(Frame request, int requestIndex);
    }

    /** Java 8: CompletableFuture.failedFuture does not exist yet. */
    private static CompletableFuture<Frame> failed(Throwable t) {
        CompletableFuture<Frame> f = new CompletableFuture<Frame>();
        f.completeExceptionally(t);
        return f;
    }

    /**
     * One cluster node: a fixed address and a response script consumed in
     * order (globally across connections — reconnects see the tail, which is
     * exactly the "state survives the connection" property of a real broker).
     */
    private static final class FakeNode {
        final InetSocketAddress addr;
        final List<Responder> script = new ArrayList<Responder>();
        final AtomicInteger served = new AtomicInteger();
        volatile Responder tail = (req, i) -> failed(new IOException("script exhausted"));

        FakeNode(String host, int port) {
            // unresolved: no DNS in tests (and none needed for equality)
            this.addr = InetSocketAddress.createUnresolved(host, port);
        }

        CompletableFuture<Frame> serve(Frame request) {
            int idx = served.getAndIncrement();
            Responder r = idx < script.size() ? script.get(idx) : tail;
            return r.respond(request, idx);
        }
    }

    /** A live connection to a FakeNode; records closes for assertions. */
    private static final class FakeConnection implements Transport {
        final FakeNode node;
        volatile boolean closed;

        FakeConnection(FakeNode node) {
            this.node = node;
        }

        @Override
        public CompletableFuture<Frame> request(MessageType type, byte[] body) {
            if (closed) {
                return failed(new IOException("connection closed"));
            }
            return node.serve(new Frame(type, -1, body));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** Maps addresses to nodes; unknown addresses refuse the connection. */
    private static final class FakeCluster {
        final List<InetSocketAddress> bootstrap = new ArrayList<InetSocketAddress>();
        final List<FakeNode> nodes = new ArrayList<FakeNode>();

        FakeNode node(String host, int port, boolean bootstrapNode) {
            FakeNode n = new FakeNode(host, port);
            nodes.add(n);
            if (bootstrapNode) {
                bootstrap.add(n.addr);
            }
            return n;
        }

        /** A bootstrap entry with no live node behind it: connect refused. */
        void bootstrapOnly(String host, int port) {
            bootstrap.add(InetSocketAddress.createUnresolved(host, port));
        }

        FailoverConnector connector() {
            return new FailoverConnector();
        }

        final class FailoverConnector implements FailoverTransport.Connector {
            @Override
            public Transport connect(InetSocketAddress addr, long requestTimeoutMs) throws IOException {
                for (FakeNode n : nodes) {
                    if (n.addr.equals(addr)) {
                        return new FakeConnection(n);
                    }
                }
                throw new IOException("connection refused: " + addr);
            }
        }
    }

    private static FailoverTransport transport(FakeCluster cluster) {
        return new FailoverTransport(cluster.bootstrap, 1_000L, 8, 1L, 2L, cluster.connector());
    }

    private static CompletableFuture<Frame> okAck(long offset) {
        return CompletableFuture.completedFuture(new Frame(MessageType.PRODUCE_ACK, 0,
                MqProto.ProduceAck.newBuilder()
                        .setStatus(MqProto.ProduceStatus.OK).addOffsets(offset)
                        .build().toByteArray()));
    }

    private static CompletableFuture<Frame> notLeaderAck(String hint) {
        MqProto.ProduceAck.Builder b = MqProto.ProduceAck.newBuilder()
                .setStatus(MqProto.ProduceStatus.ERROR)
                .setErrorCode(MqProto.ErrorCode.NOT_LEADER)
                .addOffsets(-1L);
        if (hint != null) {
            b.setLeaderHint(hint);
        }
        return CompletableFuture.completedFuture(
                new Frame(MessageType.PRODUCE_ACK, 0, b.build().toByteArray()));
    }

    /** FETCH_RESP frames carry the v1.2 [4B metaLen][meta] body layout. */
    static Frame fetchResp(MqProto.FetchResponse meta) {
        byte[] m = meta.toByteArray();
        byte[] body = new byte[4 + m.length];
        ByteBuffer.wrap(body).putInt(m.length).put(m);
        return new Frame(MessageType.FETCH_RESP, 0, body);
    }

    private static byte[] produceBody() {
        return MqProto.ProduceRequest.newBuilder()
                .addMessages(MqProto.MessageData.newBuilder().setTopic("t"))
                .build().toByteArray();
    }

    // ------------------------------------------------------------------
    // the two redirect paths
    // ------------------------------------------------------------------

    @Test
    void notLeaderWithHintRedirectsAndPinsToTheNewLeader() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        FakeNode leader = cluster.node("new-leader", 9, false); // not even in bootstrap
        a.script.add((req, i) -> notLeaderAck("new-leader:9"));
        leader.tail = (req, i) -> okAck(42L);
        FailoverTransport t = transport(cluster);

        Frame resp = t.request(MessageType.PRODUCE, produceBody()).get(5, TimeUnit.SECONDS);

        assertEquals(MqProto.ProduceStatus.OK,
                MqProto.ProduceAck.parseFrom(resp.body()).getStatus());
        assertEquals(1, a.served.get(), "the stale node answered exactly once");
        assertEquals(1, leader.served.get());
        assertEquals(leader.addr, t.currentNode(), "the hint becomes the pinned leader");

        // pinned: the next request goes straight to the new leader, no detour
        t.request(MessageType.PRODUCE, produceBody()).get(5, TimeUnit.SECONDS);
        assertEquals(2, leader.served.get());
        assertEquals(1, a.served.get(), "the stale node is never asked again");
        t.close();
    }

    @Test
    void notLeaderWithoutHintReBootstrapsToTheNextNode() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        a.script.add((req, i) -> notLeaderAck(null));
        b.tail = (req, i) -> okAck(7L);
        FailoverTransport t = transport(cluster);

        Frame resp = t.request(MessageType.PRODUCE, produceBody()).get(5, TimeUnit.SECONDS);

        assertEquals(MqProto.ProduceStatus.OK,
                MqProto.ProduceAck.parseFrom(resp.body()).getStatus());
        assertEquals(b.addr, t.currentNode(), "no hint: ask the next bootstrap node");
        t.close();
    }

    // ------------------------------------------------------------------
    // the dead-node path
    // ------------------------------------------------------------------

    @Test
    void connectionFailureFallsThroughToAnotherBootstrapNode() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        a.tail = (req, i) -> failed(new IOException("node died"));
        b.tail = (req, i) -> okAck(3L);
        FailoverTransport t = transport(cluster);

        Frame resp = t.request(MessageType.PRODUCE, produceBody()).get(5, TimeUnit.SECONDS);

        assertEquals(MqProto.ProduceStatus.OK,
                MqProto.ProduceAck.parseFrom(resp.body()).getStatus());
        assertEquals(b.addr, t.currentNode());
        t.close();
    }

    @Test
    void unreachableEverywhereFailsTheRequestFast() throws Exception {
        FakeCluster cluster = new FakeCluster();
        // bootstrap entries with NO node behind them: every connect refused
        cluster.bootstrapOnly("a-host", 1);
        cluster.bootstrapOnly("b-host", 2);
        FailoverTransport t = new FailoverTransport(cluster.bootstrap, 100L, 2, 1L, 2L,
                cluster.connector());

        CompletableFuture<Frame> f = t.request(MessageType.PRODUCE, produceBody());
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> f.get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof IOException);
        assertTrue(e.getCause().getMessage().contains("no reachable node"));
        t.close();
    }

    // ------------------------------------------------------------------
    // hint fallback and budget exhaustion
    // ------------------------------------------------------------------

    @Test
    void hintThatCannotBeConnectedFallsBackToBootstrap() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        // cluster.node("ghost", 9, false) deliberately NOT registered: the
        // connector refuses it — a hint naming a dead/unreachable node
        a.script.add((req, i) -> notLeaderAck("ghost:9"));
        b.tail = (req, i) -> okAck(5L);
        FailoverTransport t = transport(cluster);

        Frame resp = t.request(MessageType.PRODUCE, produceBody()).get(5, TimeUnit.SECONDS);

        assertEquals(MqProto.ProduceStatus.OK,
                MqProto.ProduceAck.parseFrom(resp.body()).getStatus());
        assertEquals(b.addr, t.currentNode(), "the lying hint is abandoned for bootstrap");
        t.close();
    }

    @Test
    void exhaustedRedirectBudgetFailsInsteadOfLoopingForever() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        a.tail = (req, i) -> notLeaderAck("b-host:2"); // everyone points elsewhere
        b.tail = (req, i) -> notLeaderAck("a-host:1");
        FailoverTransport t = new FailoverTransport(cluster.bootstrap, 100L, 3, 1L, 2L,
                cluster.connector());

        CompletableFuture<Frame> f = t.request(MessageType.PRODUCE, produceBody());
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> f.get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof IOException);
        assertTrue(e.getCause().getMessage().contains("gave up after 3"),
                "budget exhaustion is reported, message was: " + e.getCause().getMessage());
        t.close();
    }

    // ------------------------------------------------------------------
    // consumer redirect: NOT_LEADER on the FETCH path
    // ------------------------------------------------------------------

    @Test
    void consumerFetchSurvivesALeaderChangeThroughTheFailoverTransport() throws Exception {
        FakeCluster cluster = new FakeCluster();
        FakeNode a = cluster.node("a-host", 1, true);
        FakeNode b = cluster.node("b-host", 2, true);
        a.script.add((req, i) -> CompletableFuture.completedFuture(fetchResp(
                MqProto.FetchResponse.newBuilder()
                        .setNextOffset(0)
                        .setErrorCode(MqProto.ErrorCode.NOT_LEADER)
                        .setLeaderHint("b-host:2")
                        .build())));
        b.script.add((req, i) -> CompletableFuture.completedFuture(fetchResp(
                MqProto.FetchResponse.newBuilder()
                        .setNextOffset(41)
                        .addRecords(MqProto.RecordData.newBuilder()
                                .setOffset(0).setTopic("orders")
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("m1")))
                        .build())));

        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("offsets");
        try {
            // assemble by hand so the SCRIPTED connector (not the real
            // ClientChannel one) serves the cluster
            FailoverTransport transport = transport(cluster);
            OffsetStore store = OffsetStore.load(
                    new java.io.File(dir.toFile(), "consumer-offsets-g.properties"));
            MQConsumer consumer = new MQConsumer(transport, "orders", store, 10, 1_000L);
            try {
                List<ClientRecord> records = consumer.poll();
                assertEquals(1, records.size());
                assertEquals("m1", new String(records.get(0).getPayload()));
                consumer.commit();
                assertEquals(41L, consumer.committedOffset(),
                        "the redirected fetch's next_offset becomes the committed position");
            } finally {
                consumer.close();
            }
        } finally {
            java.nio.file.Files.walk(dir, java.nio.file.FileVisitOption.FOLLOW_LINKS)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    // ------------------------------------------------------------------
    // helpers under test: hint parsing
    // ------------------------------------------------------------------

    @Test
    void addressParsingHandlesHostsAndPorts() {
        InetSocketAddress a = FailoverTransport.parseAddress("node-1:8899");
        assertNotNull(a);
        assertEquals("node-1", a.getHostString());
        assertEquals(8899, a.getPort());
        assertEquals(null, FailoverTransport.parseAddress("no-port"));
        assertEquals(null, FailoverTransport.parseAddress("bad:port"));
    }

    /** Non-redirect responses must pass through untouched. */
    @Test
    void okResponsesAreNotTreatedAsRedirects() {
        Frame ack = new Frame(MessageType.PRODUCE_ACK, 0, MqProto.ProduceAck.newBuilder()
                .setStatus(MqProto.ProduceStatus.OK).build().toByteArray());
        assertEquals(null, FailoverTransport.redirectHint(ack));
    }
}
