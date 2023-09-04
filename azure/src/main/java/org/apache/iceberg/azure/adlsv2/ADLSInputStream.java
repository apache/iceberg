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
package org.apache.iceberg.azure.adlsv2;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.iceberg.azure.AzureProperties;
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

class ADLSInputStream extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSInputStream.class);

  // max skip size, if skip is larger, then the stream will be reopened at new position
  private static final int SKIP_SIZE = 1024 * 1024;

  private final StackTraceElement[] createStack;
  private final DataLakeFileClient fileClient;
  private Long fileSize;
  private final AzureProperties azureProperties;

  private InputStream stream;
  private long pos;
  private long next;
  private boolean closed;

  private final Counter readBytes;
  private final Counter readOperations;

  ADLSInputStream(
      DataLakeFileClient fileClient,
      Long fileSize,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this.fileClient = fileClient;
    this.fileSize = fileSize;
    this.azureProperties = azureProperties;

    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);

    this.createStack = Thread.currentThread().getStackTrace();

    openStream();
  }

  private void openStream() {
    DataLakeFileOpenInputStreamResult result =
        fileClient.openInputStream(getInputOptions(new FileRange(pos)));
    this.fileSize = result.getProperties().getFileSize();
    this.stream = result.getInputStream();
  }

  private DataLakeFileInputStreamOptions getInputOptions(FileRange range) {
    DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
    azureProperties.adlsReadBlockSize().ifPresent(options::setBlockSize);
    options.setRange(range);
    return options;
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "Cannot seek: position %s is negative", newPos);

    // this allows a seek beyond the end of the stream but the next read will fail
    this.next = newPos;
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

  private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
      // already at specified position
      return;
    }

    if ((stream != null) && (next > pos)) {
      // seeking forwards
      long skip = next - pos;
      if (skip <= Math.max(stream.available(), SKIP_SIZE)) {
        // already buffered or seek is small enough
        try {
          ByteStreams.skipFully(stream, skip);
          this.pos = next;
          return;
        } catch (IOException ignored) {
          // will retry by re-opening the stream
        }
      }
    }

    // close the stream and open at desired position
    this.pos = next;
    openStream();
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    FileRange range = new FileRange(position, position + length);

    IOUtil.readFully(openRange(range), buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);

    if (this.fileSize == null) {
      this.fileSize = fileClient.getProperties().getFileSize();
    }
    long readStart = fileSize - length;

    return IOUtil.readRemaining(openRange(new FileRange(readStart)), buffer, offset, length);
  }

  private InputStream openRange(FileRange range) {
    return fileClient.openInputStream(getInputOptions(range)).getInputStream();
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closed = true;
    if (stream != null) {
      stream.close();
    }
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
