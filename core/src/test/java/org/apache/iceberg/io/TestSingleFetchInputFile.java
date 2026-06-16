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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.junit.jupiter.api.Test;

public class TestSingleFetchInputFile {

  private static byte[] makeBytes(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i += 1) {
      data[i] = (byte) ((i * 7 + 13) % 256);
    }
    return data;
  }

  @Test
  public void testSingleFetch() throws Exception {
    byte[] bytes = makeBytes(100);
    CountingInputFile delegate = new CountingInputFile(new InMemoryInputFile(bytes));
    SingleFetchInputFile file = new SingleFetchInputFile(delegate, bytes.length, 1000L);

    try (SeekableInputStream stream = file.newStream()) {
      assertThat(stream)
          .as("Stream below threshold should be a SingleFetchInputStream")
          .isInstanceOf(SingleFetchInputStream.class);

      stream.read();
      stream.seek(50);
      stream.read(new byte[10], 0, 10);
      ((RangeReadable) stream).readFully(80, new byte[5], 0, 5);
      ((RangeReadable) stream).readTail(new byte[8], 0, 8);
    }

    assertThat(delegate.newStreamCalls())
        .as("delegate.newStream() must be called exactly once for the whole-file fetch")
        .isEqualTo(1);
  }

  @Test
  public void testPassthrough() throws Exception {
    byte[] bytes = makeBytes(100);
    CountingInputFile delegate = new CountingInputFile(new InMemoryInputFile(bytes));
    SingleFetchInputFile file = new SingleFetchInputFile(delegate, bytes.length, 50L);

    try (SeekableInputStream stream = file.newStream()) {
      assertThat(stream)
          .as("File size above threshold should bypass buffering")
          .isNotInstanceOf(SingleFetchInputStream.class);
    }

    assertThat(delegate.newStreamCalls())
        .as("delegate.newStream() must be called exactly once for passthrough")
        .isEqualTo(1);
  }

  @Test
  public void testRead() throws Exception {
    byte[] expected = makeBytes(100);
    SingleFetchInputFile file =
        new SingleFetchInputFile(new InMemoryInputFile(expected), expected.length, 1000L);

    byte[] actual = new byte[expected.length];
    try (SeekableInputStream stream = file.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    assertThat(actual).as("Bytes should match source").isEqualTo(expected);
  }

  private static final class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private int newStreamCalls;

    CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }

    @Override
    public SeekableInputStream newStream() {
      newStreamCalls += 1;
      return delegate.newStream();
    }

    int newStreamCalls() {
      return newStreamCalls;
    }
  }
}
