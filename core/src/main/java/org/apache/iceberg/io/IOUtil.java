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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IOUtil {
  // not meant to be instantiated
  private IOUtil() {}

  private static final int WRITE_CHUNK_SIZE = 8192;

  /**
   * Reads into a buffer from a stream, making multiple read calls if necessary.
   *
   * @param stream an InputStream to read from
   * @param bytes a buffer to write into
   * @param offset starting offset in the buffer for the data
   * @param length length of bytes to copy from the input stream to the buffer
   * @throws EOFException if the end of the stream is reached before reading length bytes
   * @throws IOException if there is an error while reading
   */
  public static void readFully(InputStream stream, byte[] bytes, int offset, int length)
      throws IOException {
    int bytesRead = readRemaining(stream, bytes, offset, length);
    if (bytesRead < length) {
      throw new EOFException(
          "Reached the end of stream with " + (length - bytesRead) + " bytes left to read");
    }
  }

  /** Writes a buffer into a stream, making multiple write calls if necessary. */
  public static void writeFully(OutputStream outputStream, ByteBuffer buffer) throws IOException {
    if (!buffer.hasRemaining()) {
      return;
    }
    byte[] chunk = new byte[WRITE_CHUNK_SIZE];
    while (buffer.hasRemaining()) {
      int chunkSize = Math.min(chunk.length, buffer.remaining());
      buffer.get(chunk, 0, chunkSize);
      outputStream.write(chunk, 0, chunkSize);
    }
  }

  /**
   * Reads into a buffer from a stream, making multiple read calls if necessary returning the number
   * of bytes read until end of stream.
   *
   * @param stream an InputStream to read from
   * @param bytes a buffer to write into
   * @param offset starting offset in the buffer for the data
   * @param length length of bytes to copy from the input stream to the buffer
   * @return the number of bytes read
   * @throws IOException if there is an error while reading
   */
  public static int readRemaining(InputStream stream, byte[] bytes, int offset, int length)
      throws IOException {
    int pos = offset;
    int remaining = length;
    while (remaining > 0) {
      int bytesRead = stream.read(bytes, pos, remaining);
      if (bytesRead < 0) {
        break;
      }

      remaining -= bytesRead;
      pos += bytesRead;
    }

    return length - remaining;
  }
}
