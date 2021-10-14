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

package org.apache.iceberg.io.inmemory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.io.SeekableInputStream;

final class InMemoryInputStream extends SeekableInputStream {

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream {

    SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    synchronized void seek(long newPos) {
      reset();
      long actuallySkipped = skip(newPos);
      if (actuallySkipped < newPos) {
        throw new UncheckedIOException(new IOException("Skipped to position: " + actuallySkipped +
          ". Can't seek to " + newPos + " position. EOF reached!"));
      }
    }

    synchronized long position() {
      return pos;
    }
  }

  private final SeekableByteArrayInputStream inputStream;

  InMemoryInputStream(byte[] data) {
    this.inputStream = new SeekableByteArrayInputStream(data);
  }

  @Override
  public long getPos() throws IOException {
    return inputStream.position();
  }

  @Override
  public void seek(long newPos) {
    inputStream.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
