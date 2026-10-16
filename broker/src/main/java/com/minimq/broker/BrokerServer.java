package com.minimq.broker;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.minimq.protocol.FrameDecoder;
import com.minimq.protocol.FrameEncoder;
import com.minimq.storage.CommitLog;
import com.minimq.storage.ConsumeQueueStore;
import com.minimq.storage.FlushMode;
import com.minimq.storage.FlushService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;

/**
 * Netty server with the three-tier threading model (M1 task 2):
 *
 * <ul>
 *   <li>BossGroup — 1 thread, accept only (a once-per-connection microsecond
 *   operation; more threads would be waste),</li>
 *   <li>WorkerGroup — CPU x 2 threads, read/write + frame codec; each channel
 *   is bound to one EventLoop for its lifetime, so per-connection events are
 *   serial and lock-free,</li>
 *   <li>Business pool — bounded queue, AbortPolicy; saturation becomes
 *   RETRY_LATER instead of ever blocking an EventLoop.</li>
 * </ul>
 *
 * Graceful shutdown order: stop accepting (close the listener) -> drain the
 * business pool (workers still alive so responses get written) -> shut down
 * the thread groups -> flush and close storage. A shutdown hook wires this
 * into Ctrl-C / SIGTERM; kill -9 skips it (M4 chaos tests compare the two).
 */
public class BrokerServer {

    private static final Logger LOG = Logger.getLogger(BrokerServer.class.getName());

    private final BrokerConfig config;
    private final Object lifecycleLock = new Object();

    private CommitLog commitLog;
    private ConsumeQueueStore consumeQueueStore;
    private FlushService flushService;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor businessPool;
    private Channel serverChannel;
    private Thread shutdownHook;
    private volatile boolean started;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
    }

    public void start() throws IOException, InterruptedException {
        synchronized (lifecycleLock) {
            if (started) {
                return;
            }
            commitLog = CommitLog.open(config.getStoreDir(), config.getSegmentSize());
            // ConsumeQueue root sits NEXT TO the commitlog dir (same store
            // root, e.g. data/commitlog + data/consumequeue). The dispatcher
            // replays/rebuilds the index tail at open; FETCH goes through it.
            consumeQueueStore = ConsumeQueueStore.open(consumeQueueRoot(config), commitLog);
            BrokerRequestHandler handler = createRequestHandler(commitLog, consumeQueueStore);
            businessPool = createBusinessPool(config);
            DispatcherHandler dispatcher = new DispatcherHandler(businessPool, handler);
            // SYNC/ASYNC tiers need the group-commit flush thread; OS_FLUSH
            // (M1 behavior) skips it entirely.
            if (config.getFlushMode() != FlushMode.OS_FLUSH) {
                flushService = new FlushService(
                        commitLog, config.getFlushLingerUs(), config.getFlushBatchCount());
                flushService.start();
            }

            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("mini-mq-boss"));
            workerGroup = new NioEventLoopGroup(config.getWorkerThreads(),
                    new DefaultThreadFactory("mini-mq-worker"));

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // Backpressure on the outbound path: writes beyond the
                    // high water mark flip the channel non-writable.
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new io.netty.channel.WriteBufferWaterMark(32 * 1024, 64 * 1024))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new FrameDecoder(),
                                    new FrameEncoder(),
                                    dispatcher);
                        }
                    });
            serverChannel = bootstrap.bind(config.getPort()).sync().channel();
            started = true;

            shutdownHook = new Thread(new Runnable() {
                @Override
                public void run() {
                    LOG.info("shutdown hook: closing broker");
                    shutdown();
                }
            }, "mini-mq-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    /** Actual listening port (useful when configured with port 0). */
    public int port() {
        Channel ch = serverChannel;
        if (ch == null) {
            throw new IllegalStateException("broker not started");
        }
        return ((java.net.InetSocketAddress) ch.localAddress()).getPort();
    }

    /**
     * Sum of queued-but-not-run tasks across worker EventLoops — the
     * EventLoop-zero-blocking monitor. Steady-state 0 while business work is
     * slow proves dispatch really is O(us) and never runs storage on IO
     * threads; a growing number is the "add workers / audit the pipeline"
     * signal.
     */
    public int workerPendingTasks() {
        EventLoopGroup group = workerGroup;
        if (group == null) {
            throw new IllegalStateException("broker not started");
        }
        int sum = 0;
        for (EventExecutor ex : group) {
            if (ex instanceof io.netty.util.concurrent.SingleThreadEventExecutor) {
                sum += ((io.netty.util.concurrent.SingleThreadEventExecutor) ex).pendingTasks();
            }
        }
        return sum;
    }

    public boolean isRunning() {
        return started;
    }

    /**
     * The group-commit flush thread, or null in OS_FLUSH. Exposed for the
     * bench: forceSweeps() is the aggregation observability (messages per
     * force = how well the linger window batches concurrent writers).
     */
    public FlushService flushService() {
        return flushService;
    }

    /** Idempotent; safe to call from a shutdown hook or directly. */
    public void shutdown() {
        synchronized (lifecycleLock) {
            if (!started) {
                return;
            }
            started = false;
            try {
                if (shutdownHook != null) {
                    try {
                        Runtime.getRuntime().removeShutdownHook(shutdownHook);
                    } catch (IllegalStateException ignored) {
                        // JVM already shutting down: we ARE the hook
                    }
                    shutdownHook = null;
                }
                // 1. stop accepting new connections and requests
                serverChannel.close().syncUninterruptibly();
                // 2. drain in-flight business work (workers alive: responses flow)
                businessPool.shutdown();
                if (!businessPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    businessPool.shutdownNow();
                }
                // 3. stop IO threads
                bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
                workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
                // 4. final force (covers everything appended, wakes any SYNC
                //    waiter still parked) then flush and close storage
                if (flushService != null) {
                    flushService.close();
                    flushService = null;
                }
                // 5. stop the index dispatcher (it reads the log; must not
                //    outlive it), force the queues it filled
                if (consumeQueueStore != null) {
                    consumeQueueStore.close();
                    consumeQueueStore = null;
                }
                commitLog.flush();
                commitLog.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "storage close failed", e);
            } finally {
                serverChannel = null;
                bossGroup = null;
                workerGroup = null;
                businessPool = null;
                commitLog = null;
            }
        }
    }

    /**
     * Request handler factory; tests override this to inject slow/failing
     * handlers for backpressure and failure-path integration tests.
     */
    protected BrokerRequestHandler createRequestHandler(CommitLog commitLog,
            ConsumeQueueStore consumeQueues) {
        return new BrokerRequestHandler(commitLog, consumeQueues,
                config.getFlushMode(), config.getFlushAwaitTimeoutMs(),
                config.isZeroCopyFetch());
    }

    /**
     * {@code data/commitlog} -> {@code data/consumequeue}; a store dir without
     * a parent keeps the consumequeue dir in the working directory.
     */
    private static File consumeQueueRoot(BrokerConfig config) {
        File storeDir = config.getStoreDir();
        File parent = storeDir.getParentFile();
        return parent != null ? new File(parent, "consumequeue") : new File("consumequeue");
    }

    private static ThreadPoolExecutor createBusinessPool(BrokerConfig config) {
        return new ThreadPoolExecutor(
                config.getBusinessThreads(), config.getBusinessThreads(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(config.getBusinessQueueCapacity()),
                new DefaultThreadFactory("mini-mq-biz"),
                // Rejection is the backpressure signal: the dispatcher turns
                // it into RETRY_LATER. CallerRunsPolicy would run storage
                // appends on the EventLoop — forbidden.
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.defaults();
        if (args.length > 0) {
            config.port(Integer.parseInt(args[0]));
        }
        if (args.length > 1) {
            config.storeDir(new java.io.File(args[1]));
        }
        BrokerServer server = new BrokerServer(config);
        server.start();
        LOG.info("mini-mq broker listening on port " + server.port()
                + ", store=" + config.getStoreDir().getAbsolutePath());
        Thread.currentThread().join();
    }
}
