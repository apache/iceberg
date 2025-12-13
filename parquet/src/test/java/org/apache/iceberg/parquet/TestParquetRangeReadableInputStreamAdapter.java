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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestParquetRangeReadableInputStreamAdapter {

  @Test
  public void testRangeReadableAdapterReadVectoredAvailable() {
    MockRangeReadableStream mockStream = new MockRangeReadableStream();
    ParquetIO.ParquetRangeReadableInputStreamAdapter<MockRangeReadableStream> adapter =
        new ParquetIO.ParquetRangeReadableInputStreamAdapter<>(mockStream);
    ByteBufferAllocator allocator = Mockito.mock(ByteBufferAllocator.class);

    boolean result = adapter.readVectoredAvailable(allocator);

    assertThat(result).isTrue();
  }

  @Test
  public void testRangeReadableAdapterReadVectored() throws IOException {
    int length1 = 100;
    int length2 = 50;
    MockRangeReadableStream mockStream = new MockRangeReadableStream();
    ParquetIO.ParquetRangeReadableInputStreamAdapter<MockRangeReadableStream> adapter =
        new ParquetIO.ParquetRangeReadableInputStreamAdapter<>(mockStream);

    ByteBufferAllocator allocator = Mockito.mock(ByteBufferAllocator.class);
    when(allocator.allocate(length1)).thenReturn(ByteBuffer.wrap(new byte[length1]));
    when(allocator.allocate(length2)).thenReturn(ByteBuffer.wrap(new byte[length2]));

    ParquetFileRange range1 = new ParquetFileRange(0, length1);
    ParquetFileRange range2 = new ParquetFileRange(200, length2);
    List<ParquetFileRange> ranges = Arrays.asList(range1, range2);

    adapter.readVectored(ranges, allocator);

    verify(allocator).allocate(length1);
    verify(allocator).allocate(length2);

    assertThat(range1.getDataReadFuture().isDone()).isTrue();
    assertThat(range2.getDataReadFuture().isDone()).isTrue();

    ByteBuffer buffer1 = range1.getDataReadFuture().join();
    ByteBuffer buffer2 = range2.getDataReadFuture().join();

    // Verify data content from byte arrays
    byte[] array1 = new byte[length1];
    byte[] array2 = new byte[length2];
    buffer1.get(array1);
    buffer2.get(array2);

    byte[] expected1 = new byte[length1];
    byte[] expected2 = new byte[length2];

    for (int i = 0; i < length1; i++) {
      expected1[i] = (byte) i;
    }

    for (int i = 0; i < length2; i++) {
      expected2[i] = (byte) (200 + i);
    }

    assertThat(array1).isEqualTo(expected1);
    assertThat(array2).isEqualTo(expected2);
  }

  private static class MockRangeReadableStream extends SeekableInputStream
      implements RangeReadable {

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void seek(long newPos) throws IOException {
      // no-op
    }

    @Override
    public int read() throws IOException {
      return -1;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      // no-op
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      return -1;
    }

    @Override
    public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      for (FileRange range : ranges) {
        try {
          ByteBuffer buffer = allocate.apply(range.length());

          for (int i = 0; i < range.length(); i++) {
            buffer.put((byte) (range.offset() + i));
          }

          buffer.flip();
          range.byteBuffer().complete(buffer);
        } catch (Exception e) {
          range.byteBuffer().completeExceptionally(e);
        }
      }
    }
  }
}
