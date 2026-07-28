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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.util.Arrays;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEagerInputFile {

  private static final int SIZE = 100;

  private static byte[] makeBytes() {
    byte[] data = new byte[SIZE];
    for (int i = 0; i < SIZE; i++) {
      data[i] = (byte) ((i * 7 + 13) % 256);
    }
    return data;
  }

  private static EagerInputFile eagerFile(byte[] bytes) {
    return new EagerInputFile(new InMemoryInputFile(bytes), bytes.length);
  }

  @Test
  public void testNewStream() throws Exception {
    InputFile delegate = Mockito.spy(new InMemoryInputFile(makeBytes()));
    try (SeekableInputStream stream = new EagerInputFile(delegate, SIZE).newStream()) {
      assertThat(stream)
          .as("newStream() should return an EagerInputStream")
          .isInstanceOf(EagerInputStream.class);

      stream.seek(50);
      stream.read();
      stream.read(new byte[10], 0, 5);
      stream.skip(3);
      ((RangeReadable) stream).readFully(10, new byte[8], 0, 8);
      ((RangeReadable) stream).readTail(new byte[4], 0, 4);
    }
    Mockito.verify(delegate, Mockito.times(1)).newStream();
  }

  @Test
  public void testSeek() throws Exception {
    try (SeekableInputStream stream = eagerFile(makeBytes()).newStream()) {
      int forwardPos = 50;
      stream.seek(forwardPos);
      assertThat(stream.getPos())
          .as("getPos() should reflect position after a forward seek")
          .isEqualTo(forwardPos);

      int backwardPos = 30;
      stream.seek(backwardPos);
      assertThat(stream.getPos())
          .as("getPos() should reflect position after a backward seek")
          .isEqualTo(backwardPos);
    }
  }

  @Test
  public void testRead() throws Exception {
    byte[] bytes = makeBytes();
    int currentPos = 20;
    try (SeekableInputStream stream = eagerFile(bytes).newStream()) {
      stream.seek(currentPos);
      assertThat(stream.read())
          .as("read() should return the byte at the current position")
          .isEqualTo(bytes[currentPos] & 0xFF);
    }
  }

  @Test
  public void testBulkRead() throws Exception {
    byte[] bytes = makeBytes();
    int currentPos = 20;
    byte[] buf = new byte[10];
    int offset = 2;
    int length = 5;
    try (SeekableInputStream stream = eagerFile(bytes).newStream()) {
      stream.seek(currentPos);
      assertThat(stream.read(buf, offset, length))
          .as("read(byte[], offset, length) should return the requested byte count")
          .isEqualTo(length);
      assertThat(Arrays.copyOfRange(buf, offset, offset + length))
          .as("read(byte[], offset, length) should return bytes from the current stream position")
          .containsExactly(Arrays.copyOfRange(bytes, currentPos, currentPos + length));
    }
  }

  @Test
  public void testReadFully() throws Exception {
    byte[] bytes = makeBytes();
    int currentPos = 15;
    int readPos = 60;
    byte[] buf = new byte[10];
    try (SeekableInputStream stream = eagerFile(bytes).newStream()) {
      stream.seek(currentPos);
      ((RangeReadable) stream).readFully(readPos, buf, 0, buf.length);
      assertThat(buf)
          .as("readFully() should return bytes at the given absolute position")
          .containsExactly(Arrays.copyOfRange(bytes, readPos, readPos + buf.length));
      assertThat(stream.getPos())
          .as("readFully() should not advance the sequential stream position")
          .isEqualTo(currentPos);
    }
  }

  @Test
  public void testReadTail() throws Exception {
    byte[] bytes = makeBytes();
    byte[] buf = new byte[10];
    int offset = 1;
    int tailSize = 4;
    try (SeekableInputStream stream = eagerFile(bytes).newStream()) {
      assertThat(((RangeReadable) stream).readTail(buf, offset, tailSize))
          .as("readTail() should return the requested byte count")
          .isEqualTo(tailSize);
      assertThat(Arrays.copyOfRange(buf, offset, offset + tailSize))
          .as("readTail() should return the last bytes of the file")
          .containsExactly(Arrays.copyOfRange(bytes, SIZE - tailSize, SIZE));
    }
  }

  @Test
  public void testSkip() throws Exception {
    int skipLength = 5;
    try (SeekableInputStream stream = eagerFile(makeBytes()).newStream()) {
      assertThat(stream.skip(skipLength))
          .as("skip() should return the number of bytes skipped")
          .isEqualTo(skipLength);
      assertThat(stream.getPos())
          .as("getPos() should advance by the number of bytes skipped")
          .isEqualTo(skipLength);
    }
  }

  @Test
  public void testAvailable() throws Exception {
    int currentPos = 10;
    try (SeekableInputStream stream = eagerFile(makeBytes()).newStream()) {
      stream.seek(currentPos);
      assertThat(stream.available())
          .as("available() should reflect remaining bytes after current position")
          .isEqualTo(SIZE - currentPos);
    }
  }

  @Test
  public void testGetLength() {
    assertThat(eagerFile(makeBytes()).getLength())
        .as("getLength() should return fileSize")
        .isEqualTo(SIZE);
  }

  @Test
  public void testLocation() {
    InMemoryInputFile inner = new InMemoryInputFile(makeBytes());
    assertThat(new EagerInputFile(inner, SIZE).location())
        .as("location should pass through unchanged")
        .isEqualTo(inner.location());
  }

  @Test
  public void testExists() {
    assertThat(eagerFile(makeBytes()).exists()).as("exists should pass through unchanged").isTrue();
  }

  @Test
  public void testNewFileThrows() {
    assertThatThrownBy(() -> new EagerInputFile(null, SIZE))
        .as("Null delegate is not allowed")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("delegate is null");

    assertThatThrownBy(() -> new EagerInputFile(new InMemoryInputFile(makeBytes()), -1))
        .as("Negative fileSize is not allowed")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fileSize is negative");

    assertThatThrownBy(
            () ->
                new EagerInputFile(
                    new InMemoryInputFile(makeBytes()), (long) Integer.MAX_VALUE + 1))
        .as("fileSize exceeds eager loading capacity")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds eager loading capacity");
  }

  @Test
  public void testSeekThrows() throws Exception {
    try (SeekableInputStream stream = eagerFile(makeBytes()).newStream()) {
      assertThatThrownBy(() -> stream.seek(SIZE + 1))
          .as("Cannot seek past end of stream")
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("Cannot seek to position");

      assertThatThrownBy(() -> stream.seek(-1))
          .as("Cannot seek to a negative position")
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("position is negative");
    }
  }

  @Test
  public void testReadFullyThrows() throws Exception {
    try (SeekableInputStream stream = eagerFile(makeBytes()).newStream()) {
      assertThatThrownBy(() -> ((RangeReadable) stream).readFully(SIZE, new byte[1], 0, 1))
          .as("Cannot read past end of stream")
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("exceeds stream length");
    }
  }
}
