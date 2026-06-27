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

  @Test
  public void testBelowThreshold() throws Exception {
    byte[] bytes = makeBytes();
    InMemoryInputFile inner = new InMemoryInputFile(bytes);
    InputFile delegate = Mockito.spy(inner);
    EagerInputFile file = new EagerInputFile(delegate, bytes.length, 1000L);

    assertThat(file.getLength()).as("getLength() should return file size").isEqualTo(bytes.length);
    assertThat(file.location())
        .as("location() should delegate to underlying file")
        .isEqualTo(inner.location());
    assertThat(file.exists()).as("exists() should delegate to underlying file").isTrue();

    try (SeekableInputStream stream = file.newStream()) {
      RangeReadable rangeReadable = (RangeReadable) stream;

      int posAfterSingleRead = 1;
      assertThat(stream.read()).as("read() should return first byte").isEqualTo(bytes[0] & 0xFF);
      assertThat(stream.getPos())
          .as("getPos() should advance after read()")
          .isEqualTo(posAfterSingleRead);

      int bulkReadLen = 10;
      byte[] buf = new byte[bulkReadLen];
      assertThat(stream.read(buf, 0, bulkReadLen))
          .as("read(byte[]) should return requested count")
          .isEqualTo(bulkReadLen);
      for (int i = 0; i < bulkReadLen; i++) {
        assertThat(buf[i])
            .as("read(byte[]) should return correct bytes")
            .isEqualTo(bytes[posAfterSingleRead + i]);
      }

      stream.seek(50);
      assertThat(stream.getPos()).as("getPos() should be 50 after forward seek").isEqualTo(50L);
      stream.seek(10);
      assertThat(stream.getPos()).as("getPos() should be 10 after backward seek").isEqualTo(10L);

      int skipLen = 5;
      int posAfterSkip = 10 + skipLen;
      assertThat(stream.skip(skipLen)).as("skip() should return bytes skipped").isEqualTo(skipLen);
      assertThat(stream.getPos())
          .as("getPos() should advance after skip()")
          .isEqualTo(posAfterSkip);
      assertThat(stream.available())
          .as("available() should reflect remaining bytes")
          .isEqualTo(SIZE - posAfterSkip);

      int readFullyPos = 60;
      int readFullyLen = 10;
      byte[] fullBuf = new byte[readFullyLen];
      rangeReadable.readFully(readFullyPos, fullBuf, 0, readFullyLen);
      for (int i = 0; i < readFullyLen; i++) {
        assertThat(fullBuf[i])
            .as("readFully() should return correct bytes at absolute position")
            .isEqualTo(bytes[readFullyPos + i]);
      }
      assertThat(stream.getPos())
          .as("readFully() should not advance stream position")
          .isEqualTo(posAfterSkip);

      int tailLen = 8;
      byte[] tailBuf = new byte[tailLen];
      assertThat(rangeReadable.readTail(tailBuf, 0, tailLen))
          .as("readTail() should return number of bytes copied")
          .isEqualTo(tailLen);
      for (int i = 0; i < tailLen; i++) {
        assertThat(tailBuf[i])
            .as("readTail() should return last bytes of file")
            .isEqualTo(bytes[SIZE - tailLen + i]);
      }

      stream.seek(SIZE);
      assertThat(stream.read()).as("read() should return -1 at end of stream").isEqualTo(-1);

      assertThatThrownBy(() -> stream.seek(SIZE + 1))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("Cannot seek to position");
      assertThatThrownBy(() -> stream.seek(-1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("position is negative");
      assertThatThrownBy(() -> rangeReadable.readFully(SIZE, new byte[1], 0, 1))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("exceeds stream length");
    }

    Mockito.verify(delegate, Mockito.times(1)).newStream();
  }

  @Test
  public void testPassthroughAboveOrZeroThreshold() throws Exception {
    byte[] bytes = makeBytes();

    InputFile aboveThreshold = Mockito.spy(new InMemoryInputFile(bytes));
    try (SeekableInputStream stream =
        new EagerInputFile(aboveThreshold, bytes.length, 50L).newStream()) {
      assertThat(stream)
          .as("Stream above threshold should not be eagerly buffered")
          .isNotInstanceOf(EagerInputStream.class);
    }
    Mockito.verify(aboveThreshold, Mockito.times(1)).newStream();

    InputFile zeroThreshold = Mockito.spy(new InMemoryInputFile(bytes));
    try (SeekableInputStream stream =
        new EagerInputFile(zeroThreshold, bytes.length, 0L).newStream()) {
      assertThat(stream)
          .as("Stream with zero threshold should not be eagerly buffered")
          .isNotInstanceOf(EagerInputStream.class);
    }
    Mockito.verify(zeroThreshold, Mockito.times(1)).newStream();
  }
}
