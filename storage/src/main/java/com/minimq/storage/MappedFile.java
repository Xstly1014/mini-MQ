package com.minimq.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * One pre-allocated mmap-backed segment file. The file name is the segment's
 * starting global offset (zero-padded to 20 digits), RocketMQ style.
 *
 * Concurrency: v0 assumes single-writer (the CommitLog thread); M1 will add
 * real synchronization when the broker goes multi-threaded.
 */
public class MappedFile implements Closeable {

    private final File file;
    private final long fileFromOffset;
    private final int fileSize;
    private MappedByteBuffer mappedByteBuffer;
    private volatile int wrotePosition;

    public MappedFile(File file, int fileSize) throws IOException {
        this.file = file;
        this.fileFromOffset = parseOffset(file.getName());
        this.fileSize = fileSize;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long len = raf.length();
            if (len != 0 && len != fileSize) {
                throw new IOException("segment " + file + " has length " + len
                        + ", expected 0 (fresh) or " + fileSize + " (configured segment size)");
            }
            if (len == 0) {
                raf.setLength(fileSize); // pre-allocate the full segment
            }
            this.mappedByteBuffer = raf.getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }
        this.mappedByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    static String fileName(long fromOffset) {
        return String.format("%020d", fromOffset);
    }

    private static long parseOffset(String name) {
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a segment file name: " + name);
        }
    }

    public boolean isFull() {
        return wrotePosition >= fileSize;
    }

    public int free() {
        return fileSize - wrotePosition;
    }

    /**
     * Appends {@code len} bytes and returns the start offset inside this file,
     * or {@code -1} if the remaining space is too small (the caller rolls to the
     * next segment).
     */
    public int append(byte[] data, int off, int len) {
        if (wrotePosition + len > fileSize) {
            return -1;
        }
        int start = wrotePosition;
        mappedByteBuffer.position(start);
        mappedByteBuffer.put(data, off, len);
        wrotePosition = start + len;
        return start;
    }

    /** msync-style flush of the dirty pages of this segment. */
    public void flush() {
        mappedByteBuffer.force();
    }

    /**
     * Recovery support: rewinds the write pointer to {@code validPos} and zeroes
     * {@code [validPos, dirtyEnd)} so a later append overwrites cleanly. Bytes
     * beyond {@code dirtyEnd} are unreachable for sequential writes anyway.
     */
    public void truncate(int validPos, int dirtyEnd) {
        int end = Math.min(dirtyEnd, fileSize);
        if (end > validPos) {
            mappedByteBuffer.position(validPos);
            mappedByteBuffer.put(new byte[end - validPos]);
        }
        wrotePosition = validPos;
    }

    public void setWrotePosition(int wrotePosition) {
        this.wrotePosition = wrotePosition;
    }

    /**
     * Read view of {@code [pos, limit)} of this segment, little-endian,
     * independent position. Used by codec/iterator/recovery.
     */
    public ByteBuffer slice(int pos, int limit) {
        ByteBuffer dup = mappedByteBuffer.duplicate();
        dup.position(pos).limit(limit);
        return dup.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Raw access for corruption injection in tests. */
    void putByte(int pos, byte b) {
        mappedByteBuffer.put(pos, b);
    }

    public int wrotePosition() {
        return wrotePosition;
    }

    public long fileFromOffset() {
        return fileFromOffset;
    }

    public int fileSize() {
        return fileSize;
    }

    public File getFile() {
        return file;
    }

    @Override
    public void close() {
        if (mappedByteBuffer == null) {
            return;
        }
        unmap(mappedByteBuffer);
        mappedByteBuffer = null;
    }

    /**
     * Releases the mapping eagerly. On JDK 9+ this uses Unsafe.invokeCleaner via
     * reflection (compiled at release 8, resolved at runtime); on JDK 8 the GC
     * fallback usually clears the mapping soon enough for tests.
     */
    private static void unmap(MappedByteBuffer buffer) {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Throwable ignored) {
            System.gc();
        }
    }
}
