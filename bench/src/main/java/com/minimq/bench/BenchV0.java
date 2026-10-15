package com.minimq.bench;

import com.minimq.storage.Message;
import com.minimq.storage.MessageCodec;
import com.minimq.storage.MappedFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * M0 four-mode write-path benchmark over CommitLog-format records.
 *
 * <pre>
 * A: mmap append, never force        - page-cache only, OS flushes lazily
 * B: mmap append + force per record  - durability per message (fsync-like)
 * C: mmap append + force every N     - group commit
 * D: FileChannel + DirectBuffer      - one write syscall per record, no force
 * </pre>
 *
 * The record bytes are encoded once and reused, so the benchmark measures the
 * pure write path (mmap put vs syscall), not codec overhead. Each mode gets
 * its own file; a warmup phase fills the JIT before timing.
 */
public final class BenchV0 {

    private static final int WARMUP = 10_000;

    public static void main(String[] args) throws Exception {
        int count = intArg(args, 0, 100_000);
        int payloadSize = intArg(args, 1, 1000);
        int forceEveryN = intArg(args, 2, 100);
        File dir = new File("data", "bench");
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("cannot create " + dir);
        }

        int recordSize = MessageCodec.HEADER_SIZE + payloadSize;
        System.out.printf("# mini-MQ bench-v0  count=%d  payload=%dB  record=%dB  forceEveryN=%d%n",
                count, payloadSize, recordSize, forceEveryN);
        System.out.println();
        System.out.println("| mode | msgs | MiB | elapsed(ms) | msg/s | MiB/s | note |");
        System.out.println("|------|------|-----|-------------|-------|-------|------|");

        Result a = runMmap(dir, "A", count, payloadSize, 0, "mmap, no force (page cache)");
        // Per-record force is dominated by fsync latency: a smaller sample keeps
        // the run practical while still yielding a stable msg/s figure.
        int bCount = Math.max(1, count / 10);
        Result b = runMmap(dir, "B", bCount, payloadSize, 1, "mmap, force every record");
        Result c = runMmap(dir, "C", count, payloadSize, forceEveryN,
                "mmap, force every " + forceEveryN + " records");
        Result d = runChannel(dir, "D", count, payloadSize,
                "FileChannel + DirectBuffer, no force");

        System.out.println();
        System.out.printf("A/B throughput ratio: %.0fx (expect A/B >= 10x)%n",
                ratio(a.msgsPerSec, b.msgsPerSec));
        System.out.printf("A/D throughput ratio: %.2fx (mmap vs per-record syscall)%n",
                ratio(a.msgsPerSec, d.msgsPerSec));
        System.out.printf("C/A throughput ratio: %.4fx (group-commit cost)%n",
                ratio(c.msgsPerSec, a.msgsPerSec));
        System.out.printf("avg force latency: B=%.1fms, C=%.1fms per force%n",
                b.forceMs, c.forceMs);
    }

    private static Result runMmap(File dir, String mode, int count, int payloadSize,
            int forceEvery, String note) throws IOException {
        byte[] record = MessageCodec.encode(new Message(42L, 42L, new byte[payloadSize]));
        long totalBytes = (long) (WARMUP + count) * record.length;
        File file = modeFile(dir, mode);
        MappedFile mf = new MappedFile(file, (int) totalBytes);
        try {
            for (int i = 0; i < WARMUP; i++) {
                mf.append(record, 0, record.length);
            }
            long t0 = System.nanoTime();
            long forceNanos = 0;
            for (int i = 1; i <= count; i++) {
                mf.append(record, 0, record.length);
                if (forceEvery > 0 && i % forceEvery == 0) {
                    long f0 = System.nanoTime();
                    mf.flush();
                    forceNanos += System.nanoTime() - f0;
                }
            }
            long elapsed = System.nanoTime() - t0;
            mf.flush(); // settle dirty pages after timing, keeps modes comparable
            int forces = forceEvery > 0 ? count / forceEvery : 0;
            return report(mode, count, totalBytes - (long) WARMUP * record.length,
                    elapsed, note, forceNanos, forces);
        } finally {
            mf.close();
        }
    }

    private static Result runChannel(File dir, String mode, int count, int payloadSize,
            String note) throws IOException {
        byte[] record = MessageCodec.encode(new Message(42L, 42L, new byte[payloadSize]));
        long totalBytes = (long) (WARMUP + count) * record.length;
        File file = modeFile(dir, mode);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            FileChannel channel = raf.getChannel();
            ByteBuffer direct = ByteBuffer.allocateDirect(record.length)
                    .order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < WARMUP; i++) {
                direct.clear();
                direct.put(record);
                direct.flip();
                channel.write(direct);
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                direct.clear();
                direct.put(record);
                direct.flip();
                channel.write(direct);
            }
            long elapsed = System.nanoTime() - t0;
            channel.force(false); // settle dirty pages after timing
            return report(mode, count, (long) count * record.length, elapsed, note, 0, 0);
        }
    }

    /** Per-mode directory with a valid numeric segment file name. */
    private static File modeFile(File dir, String mode) throws IOException {
        File modeDir = new File(dir, mode.toLowerCase());
        if (!modeDir.exists() && !modeDir.mkdirs()) {
            throw new IOException("cannot create " + modeDir);
        }
        File file = new File(modeDir, "00000000000000000000");
        if (!file.delete() && file.exists()) {
            throw new IOException("cannot remove " + file);
        }
        return file;
    }

    private static Result report(String mode, int count, long bytes, long elapsedNanos,
            String note, long forceNanos, int forces) {
        double elapsedMs = elapsedNanos / 1_000_000.0;
        long msgsPerSec = (long) (count / (elapsedNanos / 1_000_000_000.0));
        double mib = bytes / 1024.0 / 1024.0;
        double mibPerSec = mib / (elapsedNanos / 1_000_000_000.0);
        double forceMs = forces == 0 ? 0 : forceNanos / 1_000_000.0 / forces;
        System.out.printf("| %s | %d | %.1f | %.1f | %,d | %.1f | %s |%n",
                mode, count, mib, elapsedMs, msgsPerSec, mibPerSec, note);
        return new Result(mode, msgsPerSec, mibPerSec, forceMs);
    }

    private static double ratio(long a, long b) {
        return b == 0 ? 0 : (double) a / b;
    }

    private static int intArg(String[] args, int idx, int dflt) {
        if (args == null || args.length <= idx) {
            return dflt;
        }
        return Integer.parseInt(args[idx]);
    }

    private static final class Result {
        final String mode;
        final long msgsPerSec;
        final double mibPerSec;
        final double forceMs;

        Result(String mode, long msgsPerSec, double mibPerSec, double forceMs) {
            this.mode = mode;
            this.msgsPerSec = msgsPerSec;
            this.mibPerSec = mibPerSec;
            this.forceMs = forceMs;
        }
    }
}
