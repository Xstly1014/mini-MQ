package com.minimq.protocol;

import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.ObjectUtil;

/**
 * A {@link FileRegion} over a <b>shared</b> FileChannel (a CommitLog segment):
 * unlike {@link io.netty.channel.DefaultFileRegion}, which closes the channel
 * it was handed when released, this class never closes it — ownership stays
 * with the segment.
 *
 * <p>That distinction is the fix for the M2-2b disconnect bug: the broker
 * built regions over the segment's live channel, Netty released the region
 * after the first zero-copy FETCH, {@code DefaultFileRegion.deallocate()}
 * closed the segment channel, and every later FETCH died with
 * ClosedChannelException inside sendfile, tearing down the connection. The
 * transferTo contract is a faithful copy of DefaultFileRegion's: the
 * {@code position} argument is the offset within this region (Netty passes
 * {@code region.transferred()}), and the write starts at
 * {@code this.position + position} for {@code count - position} bytes.
 */
public final class SharedFileRegion extends AbstractReferenceCounted implements FileRegion {

    private final FileChannel channel;
    private final long position;
    private final long count;
    private long transferred;

    public SharedFileRegion(FileChannel channel, long position, long count) {
        this.channel = ObjectUtil.checkNotNull(channel, "channel");
        if (position < 0 || count < 0) {
            throw new IllegalArgumentException(
                    "position: " + position + ", count: " + count + " (expected both >= 0)");
        }
        this.position = position;
        this.count = count;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    /** Netty 4.1 legacy abstract method (deprecated typo of transferred). */
    @SuppressWarnings("deprecation")
    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws java.io.IOException {
        long remaining = count - position;
        if (position < 0 || remaining < 0) {
            throw new IllegalArgumentException("position out of range: " + position
                    + " (expected: 0 - " + (count - 1) + ')');
        }
        if (remaining == 0) {
            return 0L;
        }
        if (refCnt() == 0) {
            throw new IllegalReferenceCountException(0);
        }
        long written = channel.transferTo(this.position + position, remaining, target);
        if (written > 0) {
            transferred += written;
        }
        return written;
    }

    /** The channel is owned by the CommitLog segment: nothing to deallocate. */
    @Override
    protected void deallocate() {
        // deliberately empty
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }
}
