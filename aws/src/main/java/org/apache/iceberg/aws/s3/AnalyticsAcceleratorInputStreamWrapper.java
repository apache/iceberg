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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;

/** A wrapper to convert {@link S3SeekableInputStream} to Iceberg {@link SeekableInputStream} */
class AnalyticsAcceleratorInputStreamWrapper extends SeekableInputStream implements RangeReadable {
  private static final Logger LOG =
      LoggerFactory.getLogger(AnalyticsAcceleratorInputStreamWrapper.class);

  private final S3SeekableInputStream delegate;
  private final Counter readBytes;
  private final Counter readOperations;

  AnalyticsAcceleratorInputStreamWrapper(S3SeekableInputStream stream) {
    this(stream, MetricsContext.nullMetrics());
  }

  AnalyticsAcceleratorInputStreamWrapper(S3SeekableInputStream stream, MetricsContext metrics) {
    this.delegate = stream;
    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public int read() throws IOException {
    int nextByteValue = this.delegate.read();
    if (nextByteValue != -1) {
      readBytes.increment();
    }
    readOperations.increment();
    return nextByteValue;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = this.delegate.read(b, off, len);
    if (bytesRead > 0) {
      readBytes.increment(bytesRead);
    }
    readOperations.increment();
    return bytesRead;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    this.delegate.readFully(position, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return this.delegate.readTail(buffer, offset, length);
  }

  @Override
  public void seek(long l) throws IOException {
    this.delegate.seek(l);
  }

  @Override
  public long getPos() {
    return this.delegate.getPos();
  }

  @Override
  public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    List<ObjectRange> objectRanges =
        ranges.stream()
            .map(
                range ->
                    new software.amazon.s3.analyticsaccelerator.common.ObjectRange(
                        range.byteBuffer(), range.offset(), range.length()))
            .collect(Collectors.toList());

    // This does not keep track of bytes read and that is a possible improvement.
    readOperations.increment();
    this.delegate.readVectored(
        objectRanges,
        allocate,
        buffer -> LOG.info("Release buffer of length {}: {}", buffer.limit(), buffer));
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }
}
