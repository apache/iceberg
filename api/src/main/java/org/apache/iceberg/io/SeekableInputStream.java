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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/**
 * {@code SeekableInputStream} is an interface with the methods needed to read data from a file or
 * Hadoop data stream.
 *
 * <p>This class is based on Parquet's SeekableInputStream.
 */
public abstract class SeekableInputStream extends InputStream {
  /**
   * Return the current position in the InputStream.
   *
   * @return current position in bytes from the start of the stream
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract long getPos() throws IOException;

  /**
   * Seek to a new position in the InputStream.
   *
   * @param newPos the new position to seek to
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract void seek(long newPos) throws IOException;

  public void readVectored(List<ParquetObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    throw new UnsupportedOperationException(
        "Default iceberg stream doesn't support read vector io");
  }

  public boolean readVectoredAvailable(IntFunction<ByteBuffer> allocate) {
    return false;
  }
}
