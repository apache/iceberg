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
package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.SSLException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OSSInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(OSSInputStream.class);

  private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS =
      ImmutableList.of(SSLException.class, SocketTimeoutException.class, SocketException.class);

  private final StackTraceElement[] createStack;
  private final OSS client;
  private final OSSURI uri;

  private OSSObject currentObject;
  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private final Counter readBytes;
  private final Counter readOperations;

  private int skipSize = 1024 * 1024;
  private RetryPolicy<Object> retryPolicy =
      RetryPolicy.builder()
          .handle(RETRYABLE_EXCEPTIONS)
          .onRetry(
              e -> {
                LOG.warn(
                    "Retrying read from OSS, reopening stream (attempt {})", e.getAttemptCount());
                resetForRetry();
              })
          .onFailure(
              e ->
                  LOG.error(
                      "Failed to read from OSS input stream after exhausting all retries",
                      e.getException()))
          .withMaxRetries(3)
          .build();

  OSSInputStream(OSS client, OSSURI uri) {
    this(client, uri, MetricsContext.nullMetrics());
  }

  OSSInputStream(OSS client, OSSURI uri, MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.createStack = Thread.currentThread().getStackTrace();

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "Position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read());
      if (bytesRead != -1) {
        pos += 1;
        next += 1;
        readBytes.increment();
        readOperations.increment();
      }

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read(b, off, len));
      if (bytesRead > 0) {
        pos += bytesRead;
        next += bytesRead;
        readBytes.increment(bytesRead);
        readOperations.increment();
      }

      return bytesRead;
    } catch (FailsafeException ex) {
      if (ex.getCause() instanceof IOException) {
        throw (IOException) ex.getCause();
      }

      throw ex;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    GetObjectRequest request =
        new GetObjectRequest(uri.bucket(), uri.key()).withRange(position, position + length - 1);
    OSSObject object = client.getObject(request);
    try {
      IOUtil.readFully(object.getObjectContent(), buffer, offset, length);
      readBytes.increment(length);
      readOperations.increment();
    } finally {
      // Use close() instead of forcedClose() here because the range response data has been
      // fully consumed. close() goes through InputStream -> EofSensorInputStream path which
      // detects EOF and returns the connection to Apache HttpClient's connection pool.
      // forcedClose() calls httpResponse.close() -> ConnectionHolder.close() which hardcodes
      // releaseConnection(false), always destroying the connection even if data was consumed.
      object.close();
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key()).withRange(-1, length);
    OSSObject object = client.getObject(request);
    try {
      int bytesRead = IOUtil.readRemaining(object.getObjectContent(), buffer, offset, length);
      readBytes.increment(bytesRead);
      readOperations.increment();
      return bytesRead;
    } finally {
      // Same as readFully: data is fully consumed, use close() to allow connection reuse.
      object.close();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream(false);
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), skipSize)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} to offset {}", uri, next);
        try {
          ByteStreams.skipFully(stream, skip);
          pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    LOG.debug("Seek with new stream for {} to offset {}", uri, next);
    pos = next;
    openStream();
  }

  private void openStream() throws IOException {
    openStream(false);
  }

  private void openStream(boolean closeQuietly) throws IOException {
    GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key()).withRange(pos, -1);

    closeStream(closeQuietly);

    currentObject = client.getObject(request);
    stream = currentObject.getObjectContent();
  }

  @VisibleForTesting
  void resetForRetry() throws IOException {
    openStream(true);
  }

  private void closeStream(boolean closeQuietly) throws IOException {
    if (currentObject != null) {
      // forcedClose() calls httpResponse.close() -> ConnectionHolder.close() which sets
      // released=true and closes the socket without consuming remaining TCP data.
      // After that, objectContent.close() goes through EofSensorInputStream ->
      // ResponseEntityProxy.streamClosed() which detects the connection is already released
      // (open=false) and swallows any IOException internally. So close() after forcedClose()
      // should not throw. The catch below is a safety net for the edge case where
      // forcedClose() itself fails and the connection is still open.
      try {
        currentObject.forcedClose();
      } catch (Exception e) {
        LOG.warn("An error occurred while aborting the stream", e);
      }

      try {
        currentObject.close();
      } catch (IOException e) {
        if (!closeQuietly) {
          throw e;
        }

        LOG.warn("An error occurred while closing the stream", e);
      } finally {
        currentObject = null;
        stream = null;
      }
    }
  }

  public void setSkipSize(int skipSize) {
    this.skipSize = skipSize;
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation"})
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
