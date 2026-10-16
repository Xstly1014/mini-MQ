package com.minimq.storage;

import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

/**
 * Thread-local scratch pools for the codec hot path (M2 task 3).
 *
 * <p>JFR evidence (docs/bench-v2.md): after the Frame ownership-transfer fix
 * and the zero-copy FETCH, the per-record encode buffer ({@code new
 * byte[total]} per append, ~200MB per 200k-message round) and the per-record
 * {@link CRC32} (200k objects per round, encode + decode + decodeHeader) were
 * the top allocation sources whose lifetime is fully inside ONE synchronous
 * call stack on the calling thread.
 *
 * <p><b>Why ThreadLocal and not Netty Recycler</b>: Recycler's value is
 * cross-thread return (thread-local stack + WeakOrderQueue handoff). Nothing
 * here ever crosses a thread — the buffer is borrowed and dead within
 * {@code CommitLog.append}, the CRC within one {@code MessageCodec} call.
 * The lease is the stack frame: a scratch object is implicitly invalidated by
 * the NEXT borrow on the same thread, and since every borrower has already
 * returned, a use-after-free is impossible by construction. That is strictly
 * safer than an explicit-recycle pool (no early/missing recycle hazard).
 *
 * <p><b>Hit-ratio monitoring</b>: pools fail silently — a size mix that
 * always grows, or a future caller that borrows huge buffers, degrades to
 * plain allocation with nobody noticing. {@link LongAdder} counters keep the
 * health visible; the M2 DoD gate is hit ratio &gt; 90% under the 30-min soak.
 *
 * <p>Public (but the borrow methods stay package-private): the soak harness
 * in another module reports {@link #stats()} every minute; borrowing stays an
 * internal storage-layer concern.
 */
public final class CodecPools {

    private static final ThreadLocal<byte[]> ENCODE_BUF = new ThreadLocal<byte[]>();
    private static final ThreadLocal<CRC32> CRC = new ThreadLocal<CRC32>();

    /** Scratch buffers start here and double-ish on demand; never shrink. */
    private static final int INITIAL_ENCODE_CAPACITY = 64 * 1024;

    static final LongAdder encodeBorrows = new LongAdder();
    static final LongAdder encodeHits = new LongAdder();
    static final LongAdder crcBorrows = new LongAdder();
    static final LongAdder crcHits = new LongAdder();

    /**
     * Scratch array with {@code buf.length >= minCapacity}. A hit means the
     * existing thread-local buffer already satisfied the request (no
     * allocation). The returned array's contents are UNDEFINED — the caller
     * must fully overwrite the range it reads back (encodeInto writes
     * [0, total) and consumers never read past that).
     */
    static byte[] borrowEncodeBuffer(int minCapacity) {
        byte[] buf = ENCODE_BUF.get();
        encodeBorrows.increment();
        if (buf != null && buf.length >= minCapacity) {
            encodeHits.increment();
            return buf;
        }
        int capacity = Math.max(Math.max(minCapacity, INITIAL_ENCODE_CAPACITY),
                buf == null ? 0 : buf.length * 2);
        buf = new byte[capacity];
        ENCODE_BUF.set(buf);
        return buf;
    }

    /**
     * A reset CRC32 bound to this thread. Borrowing IS the reset — callers
     * never return anything, so stale state cannot leak across calls.
     */
    static CRC32 borrowCrc32() {
        CRC32 crc = CRC.get();
        crcBorrows.increment();
        if (crc == null) {
            crc = new CRC32();
            CRC.set(crc);
        } else {
            crcHits.increment();
            crc.reset();
        }
        return crc;
    }

    /** Pool health for soak-run reporting, e.g. "encode hit 99.2% (1,048k/1,056k)". */
    public static String stats() {
        return String.format(
                "encode-buffer hit ratio %.1f%% (%,d/%,d), crc32 hit ratio %.1f%% (%,d/%,d)",
                ratio(encodeHits.sum(), encodeBorrows.sum()),
                encodeHits.sum(), encodeBorrows.sum(),
                ratio(crcHits.sum(), crcBorrows.sum()),
                crcHits.sum(), crcBorrows.sum());
    }

    private static double ratio(long hits, long total) {
        return total == 0 ? 100.0 : 100.0 * hits / total;
    }

    private CodecPools() {
    }
}
