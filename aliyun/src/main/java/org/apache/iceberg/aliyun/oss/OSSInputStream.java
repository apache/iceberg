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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OSSInputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OSSInputStream.class);
  private static final int SKIP_SIZE = 1024 * 1024;

  private final StackTraceElement[] createStack;
  private final OSS client;
  private final OSSURI uri;

  private InputStream stream = null;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  private final Counter readBytes;
  private final Counter readOperations;
  private AliyunProperties aliyunProperties;

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

  OSSInputStream(OSS client, OSSURI uri, MetricsContext metrics, AliyunProperties aliyunProperties) {
    this.client = client;
    this.uri = uri;
    this.createStack = Thread.currentThread().getStackTrace();
    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    this.aliyunProperties = aliyunProperties;
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

    pos += 1;
    next += 1;
    readBytes.increment();
    readOperations.increment();

    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int bytesRead = stream.read(b, off, len);
    pos += bytesRead;
    next += bytesRead;
    readBytes.increment(bytesRead);
    readOperations.increment();

    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    super.close();
    closeStream();
    closed = true;
  }

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position.
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), SKIP_SIZE)) {
        // already buffered or seek is small enough
        LOG.debug("Read-through seek for {} from {} to offset {}", uri, pos, next);
        try {
          ByteStreams.skipFully(stream, skip);
          pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream.
        }
      }
    }

    // close the stream and open at desired position.
    LOG.debug("Seek with new stream for {} to offset {}", uri, next);
    pos = next;
    openStream();
  }

  private void openStream() throws IOException {
    closeStream();
    // read data from oss and store in memory
    // avoid exception about connection reset
    GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key()).withRange(pos, -1);
    if (aliyunProperties.ossLoadBeforeReading())  {
      try (InputStream ossInputStream = client.getObject(request).getObjectContent(); ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[16384];
        int len;
        while ((len = ossInputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, len);
        }
        outputStream.flush();
        byte[] data = outputStream.toByteArray();
        stream = new ByteArrayInputStream(data);
      }
    } else {
      stream = client.getObject(request).getObjectContent();
    }
  }

  private void closeStream() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by: \n\t{}", trace);
    }
  }
}
