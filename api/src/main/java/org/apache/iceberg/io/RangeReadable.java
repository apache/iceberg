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
package org.apache.iceberg.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.util.VectoredReadUtils;

/**
 * {@code RangeReadable} is an interface that allows for implementations of {@link InputFile}
 * streams to perform positional, range-based reads, which are more efficient than unbounded reads
 * in many cloud provider object stores.
 *
 * <p>Thread safety is not a requirement of the interface and is left to the implementation.
 *
 * <p>If the implementation is also a {@link SeekableInputStream}, the position of the stream is not
 * required to be updated based on the positional reads performed by this interface. Usage of {@link
 * SeekableInputStream} should always seek to the appropriate position for {@link
 * java.io.InputStream} based reads.
 */
public interface RangeReadable extends Closeable {

  /**
   * Fill the provided buffer with the contents of the input source starting at {@code position} for
   * the given {@code offset} and {@code length}.
   *
   * @param position start position of the read
   * @param buffer target buffer to copy data
   * @param offset offset in the buffer to copy the data
   * @param length size of the read
   */
  void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  /**
   * Fill the entire buffer with the contents of the input source starting at {@code position}.
   *
   * @param position start position of the read
   * @param buffer target buffer to copy data
   */
  default void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  /**
   * Read the last {@code length} bytes from the file.
   *
   * @param buffer the buffer to write data into
   * @param offset the offset in the buffer to start writing
   * @param length the number of bytes from the end of the object to read
   * @return the actual number of bytes read
   * @throws IOException if an error occurs while reading
   */
  int readTail(byte[] buffer, int offset, int length) throws IOException;

  /**
   * Read the full size of the buffer from the end of the file.
   *
   * @param buffer the buffer to write data into
   * @return the actual number of bytes read
   * @throws IOException if an error occurs while reading
   */
  default int readTail(byte[] buffer) throws IOException {
    return readTail(buffer, 0, buffer.length);
  }

  /**
   * Is the {@link #readVectored(List, IntFunction)} method available?
   *
   * @param allocate the allocator to use for allocating ByteBuffers
   * @return True if the operation is considered available for this allocator.
   */
  default boolean readVectoredAvailable(IntFunction<ByteBuffer> allocate) {
    return true;
  }

  /**
   * Read fully a list of file ranges asynchronously from this file. As a result of the call, each
   * range will have FileRange.setData(CompletableFuture) called with a future that when complete
   * will have a ByteBuffer with the data from the file's range.
   *
   * <p>The position returned by getPos() after readVectored() is undefined.
   *
   * <p>If a file is changed while the readVectored() operation is in progress, the output is
   * undefined. Some ranges may have old data, some may have new and some may have both.
   *
   * <p>While a readVectored() operation is in progress, normal read api calls may block.
   *
   * @param ranges the byte ranges to read
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException any IOE.
   * @throws IllegalArgumentException if the any of ranges are invalid, or they overlap.
   */
  default void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    List<FileRange> validatedRanges = VectoredReadUtils.validateAndSortRanges(ranges);
    for (FileRange range : validatedRanges) {
      ByteBuffer buffer = allocate.apply(range.length());
      readFully(range.offset(), buffer.array());
      range.byteBuffer().complete(buffer);
    }
  }
}
