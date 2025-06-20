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
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

/** A wrapper to convert {@link S3SeekableInputStream} to Iceberg {@link SeekableInputStream} */
class AnalyticsAcceleratorInputStreamWrapper extends SeekableInputStream implements RangeReadable {

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
  public void close() throws IOException {
    this.delegate.close();
  }
}
