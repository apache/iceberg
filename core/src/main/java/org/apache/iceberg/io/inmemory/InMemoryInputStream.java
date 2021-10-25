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
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.SeekableInputStream;

final class InMemoryInputStream extends SeekableInputStream {

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream {

    SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    void seek(long newPos) {
      reset();
      long actuallySkipped = skip(newPos);
      if (actuallySkipped < newPos) {
        throw new UncheckedIOException(new EOFException("Cannot seek to position: " + newPos + ", EOF reached"));
      }
    }

    @Override
    public void mark(int readAheadLimit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    long position() {
      return pos;
    }
  }

  private final SeekableByteArrayInputStream inputStream;

  InMemoryInputStream(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    this.inputStream = new SeekableByteArrayInputStream(bytes);
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
