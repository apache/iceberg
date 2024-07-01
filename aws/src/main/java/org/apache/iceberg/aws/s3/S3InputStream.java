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

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.Arrays;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3InputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

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

  private int skipSize = 1024 * 1024;

  S3InputStream(S3Client s3, S3URI location) {
    this(s3, location, new S3FileIOProperties(), MetricsContext.nullMetrics());
  }

  S3InputStream(
      S3Client s3, S3URI location, S3FileIOProperties s3FileIOProperties, MetricsContext metrics) {
    this.s3 = s3;
    this.location = location;
    this.s3FileIOProperties = s3FileIOProperties;

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
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    next = newPos;
  }


  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int result;
    try {
      result = stream.read();
    } catch (SocketException exception) {
      if (exception.getMessage().equals("Connection reset")) {
        openStream();
        result = stream.read();
      } else {
        throw exception;
      }
    }

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return result;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead;
    try {
       bytesRead = stream.read(b, off, len);
    } catch (SocketException exception) {
      if (exception.getMessage().equals("Connection reset")) {
        openStream();
        bytesRead = stream.read(b, off, len);
      } else {
        throw exception;
      }
    }

    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    String range = String.format("bytes=%s-%s", position, position + length - 1);

    IOUtil.readFully(readRange(range), buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    String range = String.format("bytes=-%s", length);

    return IOUtil.readRemaining(readRange(range), buffer, offset, length);
  }

  private InputStream readRange(String range) {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).range(range);

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    return s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream();
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
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .range(String.format("bytes=%s-", pos));

    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);

    closeStream();

    try {
      stream = s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
    } catch (NoSuchKeyException e) {
      throw new NotFoundException(e, "Location does not exist: %s", location);
    }
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      // if we aren't at the end of the stream, and the stream is abortable, then
      // call abort() so we don't read the remaining data with the Apache HTTP client
      abortStream();
      try {
        stream.close();
      } catch (IOException e) {
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

  @SuppressWarnings("checkstyle:NoFinalizer")
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
