package com.minimq.client.chaos;

/**
 * One row of the reliability matrix: the numbers the
 * docs/reliability-matrix.md table is built from. RPO counts only data
 * the producer was TOLD was safe (acked, acks=all) that the consumer never
 * saw — the only honest definition of "lost". Un-acked sends that failed
 * their retries are at-least-once's declared boundary, not losses.
 */
final class CellResult {

  final String cell;
  /** acked seqs the reconciler never consumed (hard metric). */
  int rpo;
  /** injection moment -> first successful post-injection ack, ms. */
  long rtoMs;
  /** seqs consumed more than once (what leaked past ALL dedup lines). */
  int duplicates;
  /** consume-stream inversions: a seq arriving after a bigger seq. */
  int reordered;
  /** anything the cell must declare honestly (unavailability spans etc). */
  String note = "";

  CellResult(String cell) {
    this.cell = cell;
  }

  String markdownRow() {
    return "| " + cell + " | " + rpo + " | " + rtoMs + "ms | "
        + duplicates + " | " + reordered + " | " + note + " |";
  }
}
