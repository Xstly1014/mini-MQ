package com.minimq.storage;

/**
 * Flush durability tiers, mirroring innodb_flush_log_at_trx_commit semantics:
 *
 * <ul>
 *   <li>{@link #SYNC_FLUSH} — the ack for a PRODUCE completes only after the
 *   flush thread forced those bytes to disk (MySQL "Follower" semantics:
 *   business threads park until the Leader's force covers their end offset).
 *   kill -9 must lose nothing that was acked.</li>
 *   <li>{@link #ASYNC_FLUSH} — a background thread forces on the same
 *   linger/count schedule, but acks return immediately; a crash may lose up to
 *   one flush window (innodb value 2).</li>
 *   <li>{@link #OS_FLUSH} — bytes reach the page cache only; the OS decides
 *   when they hit the disk (innodb value 0 / M1 behavior).</li>
 * </ul>
 */
public enum FlushMode {
    SYNC_FLUSH,
    ASYNC_FLUSH,
    OS_FLUSH
}
