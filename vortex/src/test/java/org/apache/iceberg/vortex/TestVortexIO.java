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
package org.apache.iceberg.vortex;

import static org.assertj.core.api.Assertions.assertThat;

import dev.vortex.io.NativeReadable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

/**
 * Verifies that each native read asks the stream for its whole range at once. An S3A stream with
 * the random read policy sends one GET for each request, sized by the length requested.
 */
public class TestVortexIO {
  private static final int FILE_SIZE = 8 << 20;

  @Test
  public void testEachReadRequestsItsWholeRange() throws IOException {
    byte[] contents = new byte[FILE_SIZE];
    for (int i = 0; i < contents.length; i++) {
      contents[i] = (byte) i;
    }

    CountingInputFile file = new CountingInputFile(new InMemoryInputFile(contents));
    int position = 1000;
    int length = 3 << 20;
    ByteBuffer buffer = ByteBuffer.allocateDirect(length);
    try (NativeReadable readable = VortexIO.readable(file)) {
      readable.readFully(position, buffer);
    }

    byte[] read = new byte[length];
    buffer.flip().get(read);
    assertThat(read).isEqualTo(Arrays.copyOfRange(contents, position, position + length));
    assertThat(file.seeks.get()).as("A read must seek once").isEqualTo(1);
    assertThat(file.firstRequest.get())
        .as("The first request must ask for the whole range")
        .isEqualTo(length);
  }

  /** An input file whose streams count their seeks and keep the length of the first read. */
  private static final class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private final AtomicInteger seeks = new AtomicInteger();
    private final AtomicInteger firstRequest = new AtomicInteger();

    private CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      SeekableInputStream stream = delegate.newStream();
      return new SeekableInputStream() {
        @Override
        public long getPos() throws IOException {
          return stream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
          seeks.incrementAndGet();
          stream.seek(newPos);
        }

        @Override
        public int read() throws IOException {
          return stream.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
          firstRequest.compareAndSet(0, length);
          return stream.read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
          stream.close();
        }
      };
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }
}
