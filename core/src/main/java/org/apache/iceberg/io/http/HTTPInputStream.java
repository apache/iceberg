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
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
 * <p>Transient socket/TLS errors and 5xx responses are retried up to {@value #MAX_RETRIES} times;
 * any other unexpected status (e.g. an expired pre-signed URL) fails immediately.
 */
class HTTPInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPInputStream.class);

  @VisibleForTesting static final int CHUNK_SIZE = 4 * 1024 * 1024; // 4 MB
  private static final int MAX_RETRIES = 3;

  private final StackTraceElement[] createStack;
  private final CloseableHttpClient client;
  private final String location;
  private final String url;

  private final Counter readBytes;
  private final Counter readOperations;

  /** Cached chunk buffer. {@code bufferFileStart} is the file offset of {@code buffer[0]}. */
  private byte[] buffer;

  private long bufferFileStart = -1L;
  private int bufferLimit = 0;

  private long next = 0;
  private boolean closed = false;

  HTTPInputStream(CloseableHttpClient client, String location, String url, MetricsContext metrics) {
    this.client = client;
    this.location = location;
    this.url = url;
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
    Preconditions.checkPositionIndexes(offset, offset + length, out.length);
    String range = String.format(Locale.ROOT, "bytes=%s-%s", position, position + length - 1);
    byte[] data = fetchRange(range);
    if (data.length < length) {
      throw new EOFException(
          "Reached end of " + location + " with " + (length - data.length) + " bytes left to read");
    }

    System.arraycopy(data, 0, out, offset, length);
  }

  @Override
  public int readTail(byte[] out, int offset, int length) throws IOException {
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
    String range = String.format(Locale.ROOT, "bytes=%s-%s", next, next + CHUNK_SIZE - 1);
    byte[] data = fetchRange(range);
    if (data == null || data.length == 0) {
      buffer = null;
      return;
    }

    buffer = data;
    bufferFileStart = next;
    bufferLimit = data.length;
  }

  /** Fetches a byte range from the URL, with retries on transient network and server errors. */
  private byte[] fetchRange(String range) throws IOException {
    IOException lastException = null;
    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        return doFetchRange(range, url);
      } catch (TransientHttpException | SocketException | SocketTimeoutException | SSLException e) {
        lastException = e;
        LOG.warn(
            "Retrying range fetch for {} range={} (attempt {})", location, range, attempt + 1, e);
      }
    }

    throw lastException;
  }

  private byte[] doFetchRange(String range, String requestUrl) throws IOException {
    HttpGet request = new HttpGet(requestUrl);
    request.setHeader(HttpHeaders.RANGE, range);

    return client.execute(
        request,
        response -> {
          int statusCode = response.getCode();

          if (statusCode == HttpStatus.SC_NOT_FOUND) {
            throw new NotFoundException("Location does not exist: %s", requestUrl);
          } else if (statusCode == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
            // Range starts at or past EOF (e.g. a reader probing for more blocks); treat as empty.
            EntityUtils.consumeQuietly(response.getEntity());
            return new byte[0];
          } else if (statusCode >= 500) {
            throw new TransientHttpException(
                String.format(Locale.ROOT, "Transient HTTP %d for %s", statusCode, requestUrl));
          } else if (statusCode != HttpStatus.SC_PARTIAL_CONTENT
              && statusCode != HttpStatus.SC_OK) {
            throw new IOException(
                String.format(Locale.ROOT, "Unexpected HTTP %d for %s", statusCode, requestUrl));
          }

          return response.getEntity() != null
              ? EntityUtils.toByteArray(response.getEntity())
              : new byte[0];
        });
  }

  /** Marks an IOException as retryable (transient server errors, 5xx). */
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
