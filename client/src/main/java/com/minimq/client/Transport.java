package com.minimq.client;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import com.minimq.protocol.Frame;
import com.minimq.protocol.MessageType;

/**
 * Request-response transport seam. Exists so producer retry logic can be
 * unit-tested with scripted responses (RETRY_LATER storms, timeouts) instead
 * of a live broker; {@link ClientChannel} is the real implementation.
 */
public interface Transport extends Closeable {

    /**
     * Sends a request; the returned future completes with the response frame
     * carrying the same request id, or exceptionally on timeout / disconnect.
     */
    CompletableFuture<Frame> request(MessageType type, byte[] body);

    @Override
    void close();
}
