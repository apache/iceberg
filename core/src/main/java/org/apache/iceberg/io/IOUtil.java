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

public class IOUtil {
  // not meant to be instantiated
  private IOUtil() {
  }

  /**
   * Reads a buffer from a stream, making multiple read calls if necessary.
   *
   * @param stream an InputStream to read from
   * @param bytes a buffer
   * @param offset starting offset in the buffer for the data
   * @param length length of bytes to copy from the input stream to the buffer
   * @throws IOException if there is an error while reading
   */
  @SuppressWarnings("checkstyle:InnerAssignment")
  public static void readFully(
      InputStream stream, byte[] bytes, int offset, int length) throws IOException {
    int pos = offset;
    int bytesRead;
    while ((length - pos) > 0 && (bytesRead = stream.read(bytes, pos, length - pos)) > 0) {
      pos += bytesRead;
    }

    if (pos != length) {
      throw new IOException("End of stream reached before completing read");
    }
  }
}
