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
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.io.ParquetObjectRange;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

/** A wrapper to convert {@link S3SeekableInputStream} to Iceberg {@link SeekableInputStream} */
class AnalyticsAcceleratorInputStreamWrapper extends SeekableInputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(AnalyticsAcceleratorInputStreamWrapper.class);

  private final S3SeekableInputStream delegate;

  AnalyticsAcceleratorInputStreamWrapper(S3SeekableInputStream stream) {
    this.delegate = stream;
  }

  @Override
  public int read() throws IOException {
    return this.delegate.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return this.delegate.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return this.delegate.read(b, off, len);
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

  private static final Consumer<ByteBuffer> LOG_BYTE_BUFFER_RELEASED =
      buffer -> LOG.info("Release buffer of length {}: {}", buffer.limit(), buffer);

  @Override
  public void readVectored(List<ParquetObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    this.delegate.readVectored(convertRanges(ranges), allocate, LOG_BYTE_BUFFER_RELEASED);
  }

  public static List<software.amazon.s3.analyticsaccelerator.common.ObjectRange> convertRanges(
      List<ParquetObjectRange> ranges) {
    return ranges.stream()
        .map(
            range ->
                new software.amazon.s3.analyticsaccelerator.common.ObjectRange(
                    range.getByteBuffer(), range.getOffset(), range.getLength()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean readVectoredAvailable(IntFunction<ByteBuffer> allocate) {
    return true;
  }
}
