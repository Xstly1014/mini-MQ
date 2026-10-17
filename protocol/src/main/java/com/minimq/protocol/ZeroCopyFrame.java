package com.minimq.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.netty.channel.FileRegion;

/**
 * A frame whose raw tail is sent as {@link FileRegion}s instead of heap
 * bytes (M2 task 2b). On the wire it is ONE ordinary frame —
 * {@link Frame#frameBodyLength()} on the head covers the regions, so the
 * peer's FrameDecoder sees an ordinary body — but the bytes never enter the
 * broker's user space: Netty pushes them from the page cache straight to the
 * socket (sendfile on Linux; a copy-through fallback elsewhere, still
 * correct).
 *
 * Lifecycle: the regions are ReferenceCounted. Netty releases each region
 * once its write completes (success, failure or channel close). The class
 * hands ownership to the writer the moment it is constructed — if a caller
 * constructs one and does NOT write it, it must call {@link #release()} to
 * avoid a file-handle leak (covered by dedicated tests).
 */
public final class ZeroCopyFrame {

    private final Frame head;
    private final List<FileRegion> regions;

    public ZeroCopyFrame(Frame head, List<FileRegion> regions) {
        this.head = head;
        this.regions = Collections.unmodifiableList(new ArrayList<FileRegion>(regions));
    }

    /** The frame header + meta body ([4B metaLen][meta]); trailerLength set. */
    public Frame head() {
        return head;
    }

    /** Raw-tail regions, in order; together exactly head.trailerLength() bytes. */
    public List<FileRegion> regions() {
        return regions;
    }

    /** Emergency release for regions that were never written out. */
    public void release() {
        for (FileRegion r : regions) {
            if (r.refCnt() > 0) {
                r.release();
            }
        }
    }

    @Override
    public String toString() {
        return "ZeroCopyFrame{head=" + head + ", regions=" + regions.size() + '}';
    }
}
