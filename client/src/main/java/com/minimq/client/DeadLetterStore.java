package com.minimq.client;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Dead-letter log (M4): poison records — ones whose consume attempts
 * exhausted {@code maxRetries} — parked where they block nothing, in an
 * append-only file next to the consumer's offsets. The stream moves on;
 * ops (or a re-drive tool) replays the file when the bug it captures is
 * fixed.
 *
 * <p>
 * Line format (one record per line, {@code |}-separated, UTF-8):
 *
 * <pre>
 * epochMillis | topic | offset | keyHash | attempts | error | payloadBase64
 * </pre>
 *
 * The error string has its own two hostile characters ({@code |} and
 * newline) percent-escaped, so a stack trace can never forge a second
 * line. The payload rides along base64-encoded — a dead letter without
 * its payload is a bug report without the bug.
 *
 * <p>
 * Append + flush per record: dead-lettering is rare (it took
 * {@code maxRetries} failures to get here), so durability per line costs
 * nothing and pays in "the poison pill survived the consumer's own
 * crash".
 */
final class DeadLetterStore implements Closeable {

  private final File file;
  private final Writer out;

  private DeadLetterStore(File file, Writer out) {
    this.file = file;
    this.out = out;
  }

  /**
   * Opens (appends to) the dead-letter log; {@code file == null} keeps
   * records in memory only — the shape unit tests assert against.
   */
  static DeadLetterStore open(File file) throws IOException {
    if (file == null) {
      return new DeadLetterStore(null, null);
    }
    File dir = file.getParentFile();
    if (dir != null && !dir.exists() && !dir.mkdirs()) {
      throw new IOException("cannot create dead-letter dir " + dir);
    }
    Writer w = new OutputStreamWriter(
        new FileOutputStream(file, true), StandardCharsets.UTF_8);
    return new DeadLetterStore(file, w);
  }

  /** Appends one dead letter; durable when this returns. */
  synchronized void append(ClientRecord record, int attempts, Throwable lastError)
      throws IOException {
    StringBuilder sb = new StringBuilder(128 + record.getPayload().length);
    sb.append(System.currentTimeMillis()).append('|')
        .append(escape(record.getTopic())).append('|')
        .append(record.getOffset()).append('|')
        .append(record.getKeyHash()).append('|')
        .append(attempts).append('|')
        .append(escape(String.valueOf(lastError))).append('|')
        .append(Base64.getEncoder().encodeToString(record.getPayload()))
        .append('\n');
    if (out != null) {
      out.write(sb.toString());
      out.flush();
    } else {
      inMemory.add(sb.toString());
    }
  }

  /** File this log appends to, or null for the in-memory flavor. */
  File file() {
    return file;
  }

  private final List<String> inMemory = new ArrayList<String>();

  /** In-memory flavor only: the raw lines appended so far (tests). */
  List<String> inMemoryLines() {
    return inMemory;
  }

  @Override
  public synchronized void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }

  /** Escapes the two characters a field must never contain. */
  private static String escape(String s) {
    return s.replace("%", "%25").replace("|", "%7C").replace("\r", "%0D").replace("\n", "%0A");
  }
}
