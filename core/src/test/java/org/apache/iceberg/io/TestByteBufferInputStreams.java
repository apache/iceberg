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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class TestByteBufferInputStreams {

  protected abstract ByteBufferInputStream newStream();

  protected abstract void checkOriginalData();

  @Test
  public void testRead0() throws Exception {
    byte[] bytes = new byte[0];

    ByteBufferInputStream stream = newStream();

    Assertions.assertThat(stream.read(bytes)).as("Should read 0 bytes").isEqualTo(0);

    int bytesRead = stream.read(new byte[100]);
    Assertions.assertThat(bytesRead).as("Should read to end of stream").isLessThan(100);

    Assertions.assertThat(stream.read(bytes))
        .as("Should read 0 bytes at end of stream")
        .isEqualTo(0);
  }

  @Test
  public void testReadAll() throws Exception {
    byte[] bytes = new byte[35];

    ByteBufferInputStream stream = newStream();

    int bytesRead = stream.read(bytes);
    Assertions.assertThat(bytesRead).as("Should read the entire buffer").isEqualTo(bytes.length);

    for (int i = 0; i < bytes.length; i += 1) {
      Assertions.assertThat(bytes[i]).as("Byte i should be i").isEqualTo((byte) i);
      Assertions.assertThat(stream.getPos()).as("Should advance position").isEqualTo(35);
    }

    Assertions.assertThat(stream.available())
        .as("Should have no more remaining content")
        .isEqualTo(0);

    Assertions.assertThat(stream.read(bytes)).as("Should return -1 at end of stream").isEqualTo(-1);

    Assertions.assertThat(stream.available())
        .as("Should have no more remaining content")
        .isEqualTo(0);

    checkOriginalData();
  }

  @Test
  public void testSmallReads() throws Exception {
    for (int size = 1; size < 36; size += 1) {
      byte[] bytes = new byte[size];

      ByteBufferInputStream stream = newStream();
      long length = stream.available();

      int lastBytesRead = bytes.length;
      for (int offset = 0; offset < length; offset += bytes.length) {
        Assertions.assertThat(lastBytesRead)
            .as("Should read requested len")
            .isEqualTo(bytes.length);

        lastBytesRead = stream.read(bytes, 0, bytes.length);

        Assertions.assertThat(stream.getPos())
            .as("Should advance position")
            .isEqualTo(offset + lastBytesRead);

        // validate the bytes that were read
        for (int i = 0; i < lastBytesRead; i += 1) {
          Assertions.assertThat(bytes[i]).as("Byte i should be i").isEqualTo((byte) (offset + i));
        }
      }

      Assertions.assertThat(lastBytesRead % bytes.length)
          .as("Should read fewer bytes at end of buffer")
          .isEqualTo(length % bytes.length);

      Assertions.assertThat(stream.available())
          .as("Should have no more remaining content")
          .isEqualTo(0);

      Assertions.assertThat(stream.read(bytes))
          .as("Should return -1 at end of stream")
          .isEqualTo(-1);

      Assertions.assertThat(stream.available())
          .as("Should have no more remaining content")
          .isEqualTo(0);
    }

    checkOriginalData();
  }

  @Test
  public void testPartialBufferReads() throws Exception {
    for (int size = 1; size < 35; size += 1) {
      byte[] bytes = new byte[33];

      ByteBufferInputStream stream = newStream();

      int lastBytesRead = size;
      for (int offset = 0; offset < bytes.length; offset += size) {
        Assertions.assertThat(lastBytesRead).as("Should read requested len").isEqualTo(size);

        lastBytesRead = stream.read(bytes, offset, Math.min(size, bytes.length - offset));

        Assertions.assertThat(stream.getPos())
            .as("Should advance position")
            .isEqualTo(lastBytesRead > 0 ? offset + lastBytesRead : offset);
      }

      Assertions.assertThat(lastBytesRead % size)
          .as("Should read fewer bytes at end of buffer")
          .isEqualTo(bytes.length % size);

      for (int i = 0; i < bytes.length; i += 1) {
        Assertions.assertThat(bytes[i]).as("Byte i should be i").isEqualTo((byte) i);
      }

      Assertions.assertThat(stream.available())
          .as("Should have no more remaining content")
          .isEqualTo(2);

      Assertions.assertThat(stream.read(bytes)).as("Should return 2 more bytes").isEqualTo(2);

      Assertions.assertThat(stream.available())
          .as("Should have no more remaining content")
          .isEqualTo(0);

      Assertions.assertThat(stream.read(bytes))
          .as("Should return -1 at end of stream")
          .isEqualTo(-1);

      Assertions.assertThat(stream.available())
          .as("Should have no more remaining content")
          .isEqualTo(0);
    }

    checkOriginalData();
  }

  @Test
  public void testReadByte() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    for (int i = 0; i < length; i += 1) {
      Assertions.assertThat(stream.getPos()).as("Position should increment").isEqualTo(i);
      Assertions.assertThat(stream.read()).isEqualTo(i);
    }

    Assertions.assertThatThrownBy(stream::read).isInstanceOf(EOFException.class).hasMessage(null);

    checkOriginalData();
  }

  @Test
  @SuppressWarnings("LocalVariableName")
  public void testSlice() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    ByteBuffer empty = stream.slice(0);

    Assertions.assertThat(empty).as("slice(0) should produce a non-null buffer").isNotNull();
    Assertions.assertThat(empty.remaining())
        .as("slice(0) should produce an empty buffer")
        .isEqualTo(0);
    Assertions.assertThat(stream.getPos()).as("Position should be at start").isEqualTo(0);

    int i = 0;
    while (stream.available() > 0) {
      int bytesToSlice = Math.min(stream.available(), 10);
      ByteBuffer buffer = stream.slice(bytesToSlice);

      for (int j = 0; j < bytesToSlice; j += 1) {
        Assertions.assertThat(buffer.get()).as("Data should be correct").isEqualTo((byte) (i + j));
      }

      i += bytesToSlice;
    }

    Assertions.assertThat(stream.getPos()).as("Position should be at end").isEqualTo(length);

    checkOriginalData();
  }

  @Test
  public void testSliceBuffers0() throws Exception {
    ByteBufferInputStream stream = newStream();

    Assertions.assertThat(stream.sliceBuffers(0))
        .as("Should return an empty list")
        .isEqualTo(Collections.emptyList());
  }

  @Test
  public void testWholeSliceBuffers() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    List<ByteBuffer> buffers = stream.sliceBuffers(stream.available());

    Assertions.assertThat(stream.getPos()).as("Should consume all buffers").isEqualTo(length);

    Assertions.assertThatThrownBy(() -> stream.sliceBuffers(length))
        .isInstanceOf(EOFException.class)
        .hasMessage(null);

    ByteBufferInputStream copy = ByteBufferInputStream.wrap(buffers);
    for (int i = 0; i < length; i += 1) {
      Assertions.assertThat(copy.read()).as("Slice should have identical data").isEqualTo(i);
    }

    checkOriginalData();
  }

  @Test
  public void testSliceBuffersCoverage() throws Exception {
    for (int size = 1; size < 36; size += 1) {
      ByteBufferInputStream stream = newStream();
      int length = stream.available();

      List<ByteBuffer> buffers = Lists.newArrayList();
      while (stream.available() > 0) {
        buffers.addAll(stream.sliceBuffers(Math.min(size, stream.available())));
      }

      Assertions.assertThat(stream.getPos()).as("Should consume all content").isEqualTo(length);

      ByteBufferInputStream newStream = new MultiBufferInputStream(buffers);

      for (int i = 0; i < length; i += 1) {
        Assertions.assertThat(newStream.read()).as("Data should be correct").isEqualTo(i);
      }
    }

    checkOriginalData();
  }

  @Test
  public void testSliceBuffersModification() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    int sliceLength = 5;
    List<ByteBuffer> buffers = stream.sliceBuffers(sliceLength);

    Assertions.assertThat(stream.available())
        .as("Should advance the original stream")
        .isEqualTo(length - sliceLength);

    Assertions.assertThat(stream.getPos())
        .as("Should advance the original stream position")
        .isEqualTo(sliceLength);

    Assertions.assertThat(buffers.size())
        .as("Should return a slice of the first buffer")
        .isEqualTo(1);

    ByteBuffer buffer = buffers.get(0);

    Assertions.assertThat(buffer.remaining())
        .as("Should have requested bytes")
        .isEqualTo(sliceLength);

    // read the buffer one past the returned limit. this should not change the
    // next value in the original stream
    buffer.limit(sliceLength + 1);
    for (int i = 0; i < sliceLength + 1; i += 1) {
      Assertions.assertThat(buffer.get()).as("Should have correct data").isEqualTo((byte) i);
    }

    Assertions.assertThat(stream.getPos())
        .as("Reading a slice shouldn't advance the original stream")
        .isEqualTo(sliceLength);

    Assertions.assertThat(stream.read())
        .as("Reading a slice shouldn't change the underlying data")
        .isEqualTo(sliceLength);

    // change the underlying data buffer
    buffer.limit(sliceLength + 2);
    int originalValue = buffer.duplicate().get();
    ByteBuffer undoBuffer = buffer.duplicate();

    try {
      buffer.put((byte) 255);

      Assertions.assertThat(stream.getPos())
          .as("Writing to a slice shouldn't advance the original stream")
          .isEqualTo(sliceLength + 1);

      Assertions.assertThat(stream.read())
          .as("Writing to a slice should change the underlying data")
          .isEqualTo(255);

    } finally {
      undoBuffer.put((byte) originalValue);
    }
  }

  @Test
  public void testSkip() throws Exception {
    ByteBufferInputStream stream = newStream();

    while (stream.available() > 0) {
      int bytesToSkip = Math.min(stream.available(), 10);
      Assertions.assertThat(stream.skip(bytesToSkip))
          .as("Should skip all, regardless of backing buffers")
          .isEqualTo(bytesToSkip);
    }

    stream = newStream();
    Assertions.assertThat(stream.skip(0)).isEqualTo(0);

    int length = stream.available();
    Assertions.assertThat(stream.skip(length + 10))
        .as("Should stop at end when out of bytes")
        .isEqualTo(length);

    Assertions.assertThat(stream.skip(10)).as("Should return -1 when at end").isEqualTo(-1);
  }

  @Test
  public void testSkipFully() throws Exception {
    ByteBufferInputStream stream = newStream();

    long lastPosition = 0;
    while (stream.available() > 0) {
      int bytesToSkip = Math.min(stream.available(), 10);

      stream.skipFully(bytesToSkip);

      Assertions.assertThat(stream.getPos() - lastPosition)
          .as("Should skip all, regardless of backing buffers")
          .isEqualTo(bytesToSkip);

      lastPosition = stream.getPos();
    }

    ByteBufferInputStream stream2 = newStream();
    stream2.skipFully(0);
    Assertions.assertThat(stream2.getPos()).as("Check initial position").isEqualTo(0);

    int length = stream2.available();

    Assertions.assertThatThrownBy(() -> stream2.skipFully(length + 10))
        .isInstanceOf(EOFException.class)
        .hasMessageStartingWith("Not enough bytes to skip");
  }

  @Test
  public void testMark() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.read(new byte[7]);
    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[100];
    int expectedBytesRead = stream.read(expected);

    long end = stream.getPos();

    stream.reset();

    Assertions.assertThat(stream.getPos()).as("Position should return to the mark").isEqualTo(mark);

    byte[] afterReset = new byte[100];
    int bytesReadAfterReset = stream.read(afterReset);

    Assertions.assertThat(bytesReadAfterReset)
        .as("Should read the same number of bytes")
        .isEqualTo(expectedBytesRead);

    Assertions.assertThat(stream.getPos())
        .as("Read should end at the same position")
        .isEqualTo(end);

    Assertions.assertThat(afterReset).as("Content should be equal").isEqualTo(expected);
  }

  @Test
  public void testMarkTwice() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.read(new byte[7]);
    stream.mark(1);
    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[100];
    int expectedBytesRead = stream.read(expected);

    long end = stream.getPos();

    stream.reset();

    Assertions.assertThat(stream.getPos()).as("Position should return to the mark").isEqualTo(mark);

    byte[] afterReset = new byte[100];
    int bytesReadAfterReset = stream.read(afterReset);
    Assertions.assertThat(bytesReadAfterReset)
        .as("Should read the same number of bytes")
        .isEqualTo(expectedBytesRead);

    Assertions.assertThat(stream.getPos())
        .as("Read should end at the same position")
        .isEqualTo(end);

    Assertions.assertThat(afterReset).as("Content should be equal").isEqualTo(expected);
  }

  @Test
  public void testMarkAtStart() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[10];
    Assertions.assertThat(stream.read(expected)).as("Should read 10 bytes").isEqualTo(10);

    long end = stream.getPos();

    stream.reset();

    Assertions.assertThat(stream.getPos()).as("Position should return to the mark").isEqualTo(mark);

    byte[] afterReset = new byte[10];
    Assertions.assertThat(stream.read(afterReset)).as("Should read 10 bytes").isEqualTo(10);

    Assertions.assertThat(stream.getPos())
        .as("Read should end at the same position")
        .isEqualTo(end);

    Assertions.assertThat(afterReset).as("Content should be equal").isEqualTo(expected);
  }

  @Test
  public void testMarkAtEnd() throws Exception {
    ByteBufferInputStream stream = newStream();

    int bytesRead = stream.read(new byte[100]);
    Assertions.assertThat(bytesRead < 100).as("Should read to end of stream").isTrue();

    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[10];
    Assertions.assertThat(stream.read(expected)).as("Should read 0 bytes").isEqualTo(-1);

    long end = stream.getPos();

    stream.reset();

    Assertions.assertThat(stream.getPos()).as("Position should return to the mark").isEqualTo(mark);

    byte[] afterReset = new byte[10];
    Assertions.assertThat(stream.read(afterReset)).as("Should read 0 bytes").isEqualTo(-1);

    Assertions.assertThat(stream.getPos())
        .as("Read should end at the same position")
        .isEqualTo(end);

    Assertions.assertThat(afterReset).as("Content should be equal").isEqualTo(expected);
  }

  @Test
  public void testMarkUnset() {
    ByteBufferInputStream stream = newStream();

    Assertions.assertThatThrownBy(stream::reset)
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No mark defined");
  }

  @Test
  public void testMarkAndResetTwiceOverSameRange() throws Exception {
    ByteBufferInputStream stream = newStream();

    byte[] expected = new byte[6];
    stream.mark(10);
    Assertions.assertThat(stream.read(expected))
        .as("Should read expected bytes")
        .isEqualTo(expected.length);

    stream.reset();
    stream.mark(10);

    byte[] firstRead = new byte[6];
    Assertions.assertThat(stream.read(firstRead))
        .as("Should read firstRead bytes")
        .isEqualTo(firstRead.length);

    stream.reset();

    byte[] secondRead = new byte[6];
    Assertions.assertThat(stream.read(secondRead))
        .as("Should read secondRead bytes")
        .isEqualTo(secondRead.length);

    Assertions.assertThat(firstRead).as("First read should be correct").isEqualTo(expected);

    Assertions.assertThat(secondRead).as("Second read should be correct").isEqualTo(expected);
  }

  @Test
  public void testMarkLimit() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(5);
    Assertions.assertThat(stream.read(new byte[5])).as("Should read 5 bytes").isEqualTo(5);

    stream.reset();

    Assertions.assertThat(stream.read(new byte[6])).as("Should read 6 bytes").isEqualTo(6);

    Assertions.assertThatThrownBy(stream::reset)
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No mark defined");
  }

  @Test
  public void testMarkDoubleReset() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(5);
    Assertions.assertThat(stream.read(new byte[5])).as("Should read 5 bytes").isEqualTo(5);

    stream.reset();

    Assertions.assertThatThrownBy(stream::reset)
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No mark defined");
  }
}
