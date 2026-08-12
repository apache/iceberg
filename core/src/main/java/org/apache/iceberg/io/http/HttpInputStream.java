/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io.http;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SeekableInputStream} that reads an HTTP URL via range GETs, for pre-signed object-store
 * URLs that need no object-store credentials on the reader.
 *
 * <p>Sequential reads are served from a fixed-size in-memory chunk buffer, each chunk fetched with
 * a single range GET fully consumed within the response handler so connections return to the pool.
 * Positional reads ({@link #readFully}, {@link #readTail}) each issue their own range GET.
 *
 * <p>Transient socket/TLS errors and retryable HTTP responses (throttling and transient server
 * errors; see {@link HttpStatusCategory}) are retried with exponential backoff up to {@value
 * #MAX_RETRIES} times, so a throttled or briefly unavailable endpoint is not hammered. A missing
 * location ({@code 404}) surfaces as {@link NotFoundException} and a forbidden response ({@code
 * 403}, e.g. an expired pre-signed URL) as {@link ForbiddenException}; both are terminal, as is any
 * other non-retryable status. Status codes are classified in one place by {@link
 * HttpStatusCategory}.
 */
class HttpInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(HttpInputStream.class);

  private static final int MAX_RETRIES = 3;
  private static final int MIN_RETRY_WAIT_MS = 100;
  private static final int MAX_RETRY_WAIT_MS = 5_000;
  private static final int MAX_RETRY_DURATION_MS = 30_000;
  private static final double RETRY_SCALE_FACTOR = 2.0;

  private final StackTraceElement[] createStack;
  private final CloseableHttpClient client;
  private final String location;
  private final String url;
  private final int chunkSize;

  private final Counter readBytes;
  private final Counter readOperations;

  /** Cached chunk buffer. {@code bufferFileStart} is the file offset of {@code buffer[0]}. */
  private byte[] buffer;

  private long bufferFileStart = -1L;
  private int bufferLimit = 0;

  private long next = 0;
  private boolean closed = false;

  HttpInputStream(
      CloseableHttpClient client,
      String location,
      String url,
      int chunkSize,
      MetricsContext metrics) {
    this.client = client;
    this.location = location;
    this.url = url;
    this.chunkSize = chunkSize;
    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    ensureBuffered();

    if (buffer == null || !inBuffer(next)) {
      return -1; // EOF
    }

    int bufPos = (int) (next - bufferFileStart);
    next += 1;
    readBytes.increment();
    readOperations.increment();
    return buffer[bufPos] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    Preconditions.checkPositionIndexes(off, off + len, b.length);
    if (len == 0) {
      return 0;
    }

    ensureBuffered();

    if (buffer == null || !inBuffer(next)) {
      return -1; // EOF
    }

    int bufPos = (int) (next - bufferFileStart);
    int available = bufferLimit - bufPos;
    int toCopy = Math.min(len, available);
    System.arraycopy(buffer, bufPos, b, off, toCopy);
    next += toCopy;
    readBytes.increment(toCopy);
    readOperations.increment();
    return toCopy;
  }

  @Override
  public void readFully(long position, byte[] out, int offset, int length) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    Preconditions.checkPositionIndexes(offset, offset + length, out.length);
    String range = String.format(Locale.ROOT, "bytes=%s-%s", position, position + length - 1);
    byte[] data = fetchRange(range);
    if (data.length < length) {
      throw new EOFException(
          "Reached end of "
              + HttpUrlClient.redact(location)
              + " with "
              + (length - data.length)
              + " bytes left to read");
    }

    System.arraycopy(data, 0, out, offset, length);
  }

  @Override
  public int readTail(byte[] out, int offset, int length) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    Preconditions.checkPositionIndexes(offset, offset + length, out.length);
    String range = String.format(Locale.ROOT, "bytes=-%s", length);
    byte[] data = fetchRange(range);
    int toCopy = Math.min(data.length, length);
    System.arraycopy(data, 0, out, offset, toCopy);
    return toCopy;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    buffer = null;
  }

  private boolean inBuffer(long filePos) {
    return filePos >= bufferFileStart && filePos < bufferFileStart + bufferLimit;
  }

  /**
   * Ensures the buffer covers {@code next}. Issues a new range GET if {@code next} is outside the
   * current buffer window.
   */
  private void ensureBuffered() throws IOException {
    if (buffer != null && inBuffer(next)) {
      return;
    }

    // Fetch a new chunk starting at the current position.
    String range = String.format(Locale.ROOT, "bytes=%s-%s", next, next + chunkSize - 1);
    byte[] data = fetchRange(range);
    if (data == null || data.length == 0) {
      buffer = null;
      return;
    }

    buffer = data;
    bufferFileStart = next;
    bufferLimit = data.length;
  }

  /**
   * Fetches a byte range from the URL, retrying transient network and HTTP failures with
   * exponential backoff so a throttled or briefly unavailable endpoint is not repeatedly hammered.
   */
  private byte[] fetchRange(String range) throws IOException {
    AtomicReference<byte[]> result = new AtomicReference<>();
    Tasks.range(1)
        .retry(MAX_RETRIES)
        .exponentialBackoff(
            MIN_RETRY_WAIT_MS, MAX_RETRY_WAIT_MS, MAX_RETRY_DURATION_MS, RETRY_SCALE_FACTOR)
        .onlyRetryOn(
            TransientHttpException.class,
            SocketException.class,
            SocketTimeoutException.class,
            SSLException.class)
        .run(ignored -> result.set(doFetchRange(range, url)), IOException.class);
    return result.get();
  }

  private byte[] doFetchRange(String range, String requestUrl) throws IOException {
    HttpGet request = new HttpGet(requestUrl);
    request.setHeader(HttpHeaders.RANGE, range);

    return client.execute(
        request,
        response -> {
          int statusCode = response.getCode();
          return switch (HttpStatusCategory.classify(statusCode)) {
            case OK, PARTIAL_CONTENT ->
                response.getEntity() != null
                    ? EntityUtils.toByteArray(response.getEntity())
                    : new byte[0];
            case RANGE_NOT_SATISFIABLE -> {
              // Range starts at or past EOF (e.g. a reader probing for more blocks); treat as
              // empty.
              EntityUtils.consumeQuietly(response.getEntity());
              yield new byte[0];
            }
            case NOT_FOUND ->
                throw new NotFoundException(
                    "Location does not exist: %s", HttpUrlClient.redact(requestUrl));
            case FORBIDDEN ->
                throw new ForbiddenException(
                    "Access forbidden for %s", HttpUrlClient.redact(requestUrl));
            case TRANSIENT ->
                throw new TransientHttpException(
                    String.format(
                        Locale.ROOT,
                        "Transient HTTP %d for %s",
                        statusCode,
                        HttpUrlClient.redact(requestUrl)));
            case TERMINAL ->
                throw new IOException(
                    String.format(
                        Locale.ROOT,
                        "Unexpected HTTP %d for %s",
                        statusCode,
                        HttpUrlClient.redact(requestUrl)));
          };
        });
  }

  /** Marks an IOException as retryable: throttling or a transient server error. */
  private static final class TransientHttpException extends IOException {
    TransientHttpException(String message) {
      super(message);
    }
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close();
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
