package com.minimq.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;
import com.minimq.protocol.gen.MqProto;

/**
 * Cluster transport (M3-7): one leader-pinned connection that follows the
 * cluster's failovers, so producer and consumer SDKs stay leader-agnostic.
 *
 * <h3>The two redirect paths (both mandatory)</h3>
 * A NOT_LEADER response is the <i>graceful</i> signal — the node that
 * answered knows who leads and says so in {@code leader_hint}. The client
 * connects there and resends. But the hint cannot be assumed to exist: the
 * old leader may have died outright, leaving only a timeout or a refused
 * connection. That is the bootstrap path — walk the static node list until
 * some node answers, and let ITS NOT_LEADER (or a real response) decide the
 * next hop. The two paths converge on the same retry loop.
 *
 * <h3>Bounded retries with jittered backoff</h3>
 * Each request carries a redirect budget. A backoff that grows linearly and
 * caps at {@code maxBackoffMs}, plus per-attempt jitter, spaces the retries
 * out over the failover window (detection ~1.5s + election ~0.5s + margin)
 * without hammering a cluster that is mid-election. Exhausting the budget
 * fails the request — the layers above (producer retries, consumer poll
 * loop) already have their own opinions about persistence.
 *
 * <h3>Concurrency</h3>
 * The current connection is swapped under {@code lock}; identity-checked
 * invalidation keeps a late failure report from closing a connection a
 * concurrent request already replaced. Redirect decisions are made in
 * response callbacks (IO or timer threads), so connects happen there too —
 * blocking a callback briefly is fine, blocking the USER thread never
 * happens: {@link #request} returns a future immediately.
 */
public final class FailoverTransport implements Transport {

    private static final Logger LOG = Logger.getLogger(FailoverTransport.class.getName());

    /** Seam for tests: how a node address becomes a live Transport. */
    public interface Connector {
        Transport connect(InetSocketAddress addr, long requestTimeoutMs) throws Exception;
    }

    private static final Connector DEFAULT_CONNECTOR = new Connector() {
        @Override
        public Transport connect(InetSocketAddress addr, long requestTimeoutMs) throws Exception {
            return ClientChannel.connect(addr.getHostString(), addr.getPort(), requestTimeoutMs);
        }
    };

    private final List<InetSocketAddress> bootstrap;
    private final long requestTimeoutMs;
    private final int maxRedirects;
    private final long backoffMs;
    private final long maxBackoffMs;
    private final Connector connector;
    private final ScheduledExecutorService scheduler;

    private final Object lock = new Object();
    private Transport current;
    private InetSocketAddress currentAddr;
    /** Round-robin cursor into the bootstrap list (the "ask someone else" path). */
    private int nextBootstrapIndex;
    /** Set after a NOT_LEADER with hint: the address to prefer on reconnect. */
    private InetSocketAddress preferred;
    private volatile boolean closed;

    FailoverTransport(List<InetSocketAddress> bootstrap, long requestTimeoutMs,
            int maxRedirects, long backoffMs, long maxBackoffMs, Connector connector) {
        if (bootstrap == null || bootstrap.isEmpty()) {
            throw new IllegalArgumentException("cluster needs at least one bootstrap node");
        }
        this.bootstrap = new ArrayList<InetSocketAddress>(bootstrap);
        this.requestTimeoutMs = requestTimeoutMs;
        this.maxRedirects = maxRedirects;
        this.backoffMs = backoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.connector = connector;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "mini-mq-failover-retry");
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * Opens a failover transport over a static node list. No connection is
     * made until the first request (or a redirect) needs one.
     */
    public static FailoverTransport open(List<InetSocketAddress> nodes, long requestTimeoutMs) {
        return new FailoverTransport(nodes, requestTimeoutMs, 16, 100L, 500L, DEFAULT_CONNECTOR);
    }

    @Override
    public CompletableFuture<Frame> request(MessageType type, byte[] body) {
        CompletableFuture<Frame> result = new CompletableFuture<Frame>();
        if (closed) {
            result.completeExceptionally(new IllegalStateException("transport closed"));
            return result;
        }
        attempt(type, body, maxRedirects, result);
        return result;
    }

    /** Where requests currently go (diagnostics / tests); null when down. */
    public InetSocketAddress currentNode() {
        synchronized (lock) {
            return currentAddr;
        }
    }

    // ------------------------------------------------------------------
    // the retry loop
    // ------------------------------------------------------------------

    private void attempt(final MessageType type, final byte[] body, final int attemptsLeft,
            final CompletableFuture<Frame> result) {
        if (closed) {
            result.completeExceptionally(new IllegalStateException("transport closed"));
            return;
        }
        final Transport t;
        try {
            t = acquire();
        } catch (IOException e) {
            result.completeExceptionally(new IOException("no reachable node in " + bootstrap, e));
            return;
        }
        t.request(type, body).whenComplete((resp, err) -> {
            if (closed) {
                result.completeExceptionally(new IllegalStateException("transport closed"));
                return;
            }
            if (err != null) {
                // dead/paused node: forget it, ask another bootstrap node
                invalidate(t);
                retryOrFail(type, body, attemptsLeft, result,
                        "node " + currentNodeOrNull() + " failed: " + err);
                return;
            }
            String hint = redirectHint(resp);
            if (hint == null) {
                result.complete(resp); // the normal case: a real answer
                return;
            }
            // NOT_LEADER: follow the hint when it exists, else re-bootstrap
            invalidate(t);
            synchronized (lock) {
                preferred = hint.isEmpty() ? null : parseAddress(hint);
            }
            retryOrFail(type, body, attemptsLeft, result,
                    hint.isEmpty() ? "not leader (no hint), re-bootstrapping"
                            : "not leader, following hint " + hint);
        });
    }

    private void retryOrFail(final MessageType type, final byte[] body, final int attemptsLeft,
            final CompletableFuture<Frame> result, String reason) {
        if (attemptsLeft <= 0) {
            result.completeExceptionally(new IOException(
                    "gave up after " + maxRedirects + " redirects, last: " + reason));
            return;
        }
        // Linear growth capped, plus jitter so many clients failing over do
        // not re-synchronize their retry storms.
        int attemptNumber = maxRedirects - attemptsLeft + 1;
        long delay = Math.min(backoffMs * attemptNumber, maxBackoffMs)
                + ThreadLocalRandom.current().nextLong(50L);
        LOG.fine("failover retry #" + attemptNumber + " in " + delay + "ms (" + reason + ")");
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                attempt(type, body, attemptsLeft - 1, result);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------
    // connection management
    // ------------------------------------------------------------------

    /**
     * Returns the live connection, establishing one if needed. Candidate
     * order: the redirect hint (if any), then the bootstrap list round-robin.
     */
    private Transport acquire() throws IOException {
        synchronized (lock) {
            if (current != null) {
                return current;
            }
            InetSocketAddress hint = preferred;
            if (hint != null) {
                Transport t = tryConnect(hint);
                if (t != null) {
                    adopt(hint, t);
                    return t;
                }
                preferred = null; // the hint lied or died: fall through
            }
            for (int i = 0; i < bootstrap.size(); i++) {
                InetSocketAddress addr = bootstrap.get(
                        (nextBootstrapIndex + i) % bootstrap.size());
                Transport t = tryConnect(addr);
                if (t != null) {
                    nextBootstrapIndex = (nextBootstrapIndex + i + 1) % bootstrap.size();
                    adopt(addr, t);
                    return t;
                }
            }
            throw new IOException("all " + bootstrap.size() + " bootstrap nodes unreachable");
        }
    }

    private void adopt(InetSocketAddress addr, Transport t) {
        current = t;
        currentAddr = addr;
        LOG.info("failover transport connected to " + addr);
    }

    /** Identity-checked: a stale callback must not close a fresher connection. */
    private void invalidate(Transport t) {
        synchronized (lock) {
            if (current == t) {
                current = null;
                currentAddr = null;
            }
        }
        try {
            t.close();
        } catch (Exception e) {
            LOG.log(Level.FINE, "closing a failed node connection", e);
        }
    }

    private Transport tryConnect(InetSocketAddress addr) {
        try {
            return connector.connect(addr, requestTimeoutMs);
        } catch (Exception e) {
            LOG.fine("connect to " + addr + " failed: " + e);
            return null;
        }
    }

    private String currentNodeOrNull() {
        synchronized (lock) {
            return currentAddr == null ? "none" : currentAddr.toString();
        }
    }

    // ------------------------------------------------------------------
    // NOT_LEADER detection
    // ------------------------------------------------------------------

    /**
     * Inspects a response for a redirect directive.
     *
     * @return null = deliver as-is (no redirect); "" = NOT_LEADER without a
     *         hint (re-bootstrap); "host:port" = NOT_LEADER, go there
     */
    static String redirectHint(Frame resp) {
        try {
            switch (resp.type()) {
                case PRODUCE_ACK: {
                    MqProto.ProduceAck ack = MqProto.ProduceAck.parseFrom(resp.body());
                    return ack.getErrorCode() == MqProto.ErrorCode.NOT_LEADER
                            ? ack.getLeaderHint() : null;
                }
                case FETCH_RESP: {
                    byte[] body = resp.body();
                    if (body.length < 4) {
                        return null;
                    }
                    int metaLen = ByteBuffer.wrap(body).getInt(0);
                    if (metaLen <= 0 || 4 + metaLen > body.length) {
                        return null;
                    }
                    MqProto.FetchResponse fr = MqProto.FetchResponse.parseFrom(
                            ByteBuffer.wrap(body, 4, metaLen));
                    return fr.getErrorCode() == MqProto.ErrorCode.NOT_LEADER
                            ? fr.getLeaderHint() : null;
                }
                default:
                    return null;
            }
        } catch (Exception e) {
            // Unparseable bodies are not our problem: deliver them and let
            // the SDK layer's own parsing decide their fate.
            return null;
        }
    }

    static InetSocketAddress parseAddress(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0 || colon == hostPort.length() - 1) {
            return null;
        }
        try {
            // Unresolved on purpose: the constructor's eager DNS lookup can
            // block a response callback for seconds on a dead host; the
            // connector resolves when it actually connects (Netty's async
            // resolver), and equality/round-robin never needed resolution.
            return InetSocketAddress.createUnresolved(
                    hostPort.substring(0, colon),
                    Integer.parseInt(hostPort.substring(colon + 1)));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
        Transport t;
        synchronized (lock) {
            t = current;
            current = null;
            currentAddr = null;
        }
        if (t != null) {
            t.close();
        }
    }
}
