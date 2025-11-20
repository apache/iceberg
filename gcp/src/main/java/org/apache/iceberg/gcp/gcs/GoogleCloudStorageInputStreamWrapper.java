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
package org.apache.iceberg.gcp.gcs;

import com.google.api.client.util.Preconditions;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;

class GoogleCloudStorageInputStreamWrapper extends SeekableInputStream implements RangeReadable {
  private final Counter readBytesCounter;
  private final Counter readOperationsCounter;
  private final GoogleCloudStorageInputStream stream;

  GoogleCloudStorageInputStreamWrapper(
      GoogleCloudStorageInputStream stream, MetricsContext metrics) {
    Preconditions.checkArgument(null != stream, "Invalid input stream : null");
    this.stream = stream;
    this.readBytesCounter =
        metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    this.readOperationsCounter = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    int readByte = stream.read();
    readBytesCounter.increment();
    readOperationsCounter.increment();
    return readByte;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = stream.read(b, off, len);
    if (bytesRead > 0) {
      readBytesCounter.increment(bytesRead);
    }
    readOperationsCounter.increment();
    return bytesRead;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    stream.readFully(position, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return stream.readTail(buffer, offset, length);
  }

  @Override
  public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    List<GcsObjectRange> objectRanges =
        ranges.stream()
            .map(
                fileRange ->
                    GcsObjectRange.builder()
                        .setOffset(fileRange.offset())
                        .setLength(fileRange.length())
                        .setByteBufferFuture(fileRange.byteBuffer())
                        .build())
            .collect(Collectors.toList());

    stream.readVectored(objectRanges, allocate);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
