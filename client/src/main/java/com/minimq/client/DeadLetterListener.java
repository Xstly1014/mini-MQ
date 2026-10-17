package com.minimq.client;

/**
 * Alarm hook (M4): fired exactly when a record is dead-lettered — the
 * moment that must page someone, because the stream just moved past data
 * nobody has successfully processed.
 *
 * <p>
 * Called synchronously on the consumer's thread from
 * {@link MQConsumer#reportFailure}: the dead letter is already durably
 * appended, so the listener may block (send an alert, write a ticket)
 * without risking loss. It must not throw — an alarm that takes the
 * consumer down defeats its own purpose (failures are swallowed and
 * logged by the caller).
 */
public interface DeadLetterListener {

  /**
   * @param record    the poison record, payload included
   * @param attempts  total consume attempts that failed (>= maxRetries)
   * @param lastError the failure cause of the final attempt
   */
  void onDeadLetter(ClientRecord record, int attempts, Throwable lastError);
}
