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
package org.apache.iceberg.aws.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;
import javax.net.ssl.SSLException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3InputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  private static final List<Class<? extends Throwable>> RETRYABLE_EXCEPTIONS =
      ImmutableList.of(SSLException.class, SocketTimeoutException.class, SocketException.class);

  private final StackTraceElement[] createStack;
  private final S3Client s3;
  private final S3URI location;
  private final S3FileIOProperties s3FileIOProperties;

  private InputStream stream;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private final Counter readBytes;
  private final Counter readOperations;
  private ExecutorService executorService;

  private int skipSize = 1024 * 1024;
  private RetryPolicy<Object> retryPolicy =
      RetryPolicy.builder()
          .handle(RETRYABLE_EXCEPTIONS)
          .onRetry(
              e -> {
                LOG.warn(
                    "Retrying read from S3, reopening stream (attempt {})", e.getAttemptCount());
                resetForRetry();
              })
          .onFailure(
              e ->
                  LOG.error(
                      "Failed to read from S3 input stream after exhausting all retries",
                      e.getException()))
          .withMaxRetries(3)
          .build();

  S3InputStream(S3Client s3, S3URI location) {
    this(s3, location, new S3FileIOProperties(), MetricsContext.nullMetrics(), null);
  }

  S3InputStream(
      S3Client s3, S3URI location, S3FileIOProperties s3FileIOProperties, MetricsContext metrics) {
    this(s3, location, s3FileIOProperties, metrics, null);
  }

  S3InputStream(
      S3Client s3,
      S3URI location,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      ExecutorService executorService) {
    this.s3 = s3;
    this.location = location;
    this.s3FileIOProperties = s3FileIOProperties;
    this.executorService = executorService;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();
  }

  @Override
  public long getPos() {
    return next;
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      executorService =
          ThreadPools.newExitingWorkerPool(
              "iceberg-s3fileio-read", s3FileIOProperties.readThreads());
    }
    return executorService;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();
    try {
      int bytesRead = Failsafe.with(retryPolicy).get(() -> stream.read());
      pos += 1;
      next += 1;
      readBytes.increment();
      readOperations.increment();

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
      pos += bytesRead;
      next += bytesRead;
      readBytes.increment(bytesRead);
      readOperations.increment();

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

    String range = String.format("bytes=%s-%s", position, position + length - 1);

    try (InputStream stream = readRange(range)) {
      IOUtil.readFully(stream, buffer, offset, length);
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    String range = String.format("bytes=-%s", length);

    try (InputStream stream = readRange(range)) {
      return IOUtil.readRemaining(stream, buffer, offset, length);
    }
  }

  private InputStream readRange(String range) {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).range(range);

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    return s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
  }

  /**
   * Vectored read implementation that coalesces nearby ranges to reduce S3 requests. Step Through
   * Process: 1. Coalesce ranges - merge nearby ranges within skipSize tolerance and create linked
   * lists to populate 2. Submit S3 requests concurrently for each coalesced range 3. For each S3
   * response stream, skip gaps and read into individual range buffers Given ranges [0-100],
   * [200-300], [10000-10100] with skipSize=1024: - First two ranges are close (gap=100 < 1024) so
   * they coalesce into [0-300] - Third range is far (gap=9700 > 1024) so it stays separate as
   * [10000-10100] - Results in 2 S3 requests for [0-300] and [10000-10100] - Reads the stream into
   * the buffer, stopping when it reaches the length + offset - Then Skips to the next linked
   * buffers start - For coalesced [0-300]: read [0-100], skip 100 bytes, read [200-300]
   *
   * @param ranges the ranges to read and their futures
   * @param allocate the function to allocate ByteBuffer
   */
  @Override
  public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate) {
    Map<FileRange, List<FileRange>> linkedRanges = Maps.newHashMap();
    List<FileRange> coalesced = coalesce(ranges, linkedRanges);

    Tasks.foreach(coalesced)
        .retry(3)
        .executeWith(executorService())
        .onFailure((range, thrown) -> LOG.error("Failed to stream range: {}", range, thrown))
        .throwFailureWhenFinished()
        .run(
            coalescedRange -> {
              String range =
                  String.format(
                      "bytes=%s-%s",
                      coalescedRange.offset(),
                      coalescedRange.offset() + coalescedRange.length() - 1);
              try (InputStream stream = readRange(range)) {
                long currentPos = coalescedRange.offset();

                for (FileRange linkedRange : linkedRanges.get(coalescedRange)) {
                  // Skip to the start of this linked range
                  long skipBytes = linkedRange.offset() - currentPos;
                  if (skipBytes > 0) {
                    ByteStreams.skipFully(stream, skipBytes);
                  }

                  // Read into the linked range buffer
                  ByteBuffer buffer = allocate.apply(linkedRange.length());
                  ByteStreams.readFully(stream, buffer.array(), 0, linkedRange.length());
                  linkedRange.byteBuffer().complete(buffer);

                  currentPos = linkedRange.offset() + linkedRange.length();
                }
              } catch (IOException e) {
                LOG.error("Failed to stream range: {}", range, e);
                throw new RuntimeException(e);
              }
            });
  }

  /**
   * Merges nearby ranges to reduce S3 requests. Ranges are merged if the gap between them is
   * smaller than skipSize.
   *
   * <p>With skipSize=1024, ranges [0-100], [200-300], [5000-5100] become: - [0-300] containing
   * [0-100] and [200-300] (gap=100 < 1024) - [5000-5100] stays separate (gap=4700 > 1024)
   *
   * @param ranges the ranges to coalesce
   * @param linkedRanges map to store linked ranges for each coalesced range
   * @return a new list of coalesced ranges
   */
  private List<FileRange> coalesce(
      List<FileRange> ranges, Map<FileRange, List<FileRange>> linkedRanges) {
    if (ranges.size() < 2) {
      for (FileRange range : ranges) {
        linkedRanges.put(range, Lists.newArrayList(range));
      }
      return Lists.newArrayList(ranges);
    }

    List<FileRange> sorted = Lists.newArrayList(ranges);
    Collections.sort(sorted);

    List<FileRange> result = Lists.newArrayList();
    List<FileRange> linked = Lists.newArrayList();
    FileRange firstRange = sorted.get(0);

    linked.add(firstRange);
    long start = firstRange.offset();
    long end = start + firstRange.length();

    for (int i = 1; i < sorted.size(); i++) {
      FileRange nextRange = sorted.get(i);

      if (end + skipSize >= nextRange.offset()) {
        linked.add(nextRange);
        end = Math.max(end, nextRange.offset() + nextRange.length());
      } else {
        FileRange coalesced = new FileRange(start, (int) (end - start));
        linkedRanges.put(coalesced, Lists.newArrayList(linked));
        result.add(coalesced);

        linked.clear();
        linked.add(nextRange);
        start = nextRange.offset();
        end = start + nextRange.length();
      }
    }

    FileRange coalesced = new FileRange(start, (int) (end - start));
    linkedRanges.put(coalesced, Lists.newArrayList(linked));
    result.add(coalesced);
    return result;
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
        LOG.debug("Read-through seek for {} to offset {}", location, next);
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
    LOG.debug("Seek with new stream for {} to offset {}", location, next);
    pos = next;
    openStream();
  }

  private void openStream() throws IOException {
    openStream(false);
  }

  private void openStream(boolean closeQuietly) throws IOException {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .range(String.format("bytes=%s-", pos));

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    closeStream(closeQuietly);

    try {
      stream = s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
    } catch (NoSuchKeyException e) {
      throw new NotFoundException(e, "Location does not exist: %s", location);
    }
  }

  @VisibleForTesting
  void resetForRetry() throws IOException {
    openStream(true);
  }

  private void closeStream(boolean closeQuietly) throws IOException {
    if (stream != null) {
      // if we aren't at the end of the stream, and the stream is abortable, then
      // call abort() so we don't read the remaining data with the Apache HTTP client
      abortStream();
      try {
        stream.close();
      } catch (IOException e) {
        if (closeQuietly) {
          stream = null;
          LOG.warn("An error occurred while closing the stream", e);
          return;
        }

        // the Apache HTTP client will throw a ConnectionClosedException
        // when closing an aborted stream, which is expected
        if (!e.getClass().getSimpleName().equals("ConnectionClosedException")) {
          throw e;
        }
      }
      stream = null;
    }
  }

  private void abortStream() {
    try {
      if (stream instanceof Abortable && stream.read() != -1) {
        ((Abortable) stream).abort();
      }
    } catch (Exception e) {
      LOG.warn("An error occurred while aborting the stream", e);
    }
  }

  public void setSkipSize(int skipSize) {
    this.skipSize = skipSize;
  }

  @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize"})
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
