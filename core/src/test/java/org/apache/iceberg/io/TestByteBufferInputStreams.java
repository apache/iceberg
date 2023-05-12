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
import org.junit.Assert;
import org.junit.Test;

public abstract class TestByteBufferInputStreams {

  protected abstract ByteBufferInputStream newStream();

  protected abstract void checkOriginalData();

  @Test
  public void testRead0() throws Exception {
    byte[] bytes = new byte[0];

    ByteBufferInputStream stream = newStream();

    Assert.assertEquals("Should read 0 bytes", 0, stream.read(bytes));

    int bytesRead = stream.read(new byte[100]);
    Assert.assertTrue("Should read to end of stream", bytesRead < 100);

    Assert.assertEquals("Should read 0 bytes at end of stream", 0, stream.read(bytes));
  }

  @Test
  public void testReadAll() throws Exception {
    byte[] bytes = new byte[35];

    ByteBufferInputStream stream = newStream();

    int bytesRead = stream.read(bytes);
    Assert.assertEquals("Should read the entire buffer", bytes.length, bytesRead);

    for (int i = 0; i < bytes.length; i += 1) {
      Assert.assertEquals("Byte i should be i", i, bytes[i]);
      Assert.assertEquals("Should advance position", 35, stream.getPos());
    }

    Assert.assertEquals("Should have no more remaining content", 0, stream.available());

    Assert.assertEquals("Should return -1 at end of stream", -1, stream.read(bytes));

    Assert.assertEquals("Should have no more remaining content", 0, stream.available());

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
        Assert.assertEquals("Should read requested len", bytes.length, lastBytesRead);

        lastBytesRead = stream.read(bytes, 0, bytes.length);

        Assert.assertEquals("Should advance position", offset + lastBytesRead, stream.getPos());

        // validate the bytes that were read
        for (int i = 0; i < lastBytesRead; i += 1) {
          Assert.assertEquals("Byte i should be i", offset + i, bytes[i]);
        }
      }

      Assert.assertEquals(
          "Should read fewer bytes at end of buffer",
          length % bytes.length,
          lastBytesRead % bytes.length);

      Assert.assertEquals("Should have no more remaining content", 0, stream.available());

      Assert.assertEquals("Should return -1 at end of stream", -1, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content", 0, stream.available());
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
        Assert.assertEquals("Should read requested len", size, lastBytesRead);

        lastBytesRead = stream.read(bytes, offset, Math.min(size, bytes.length - offset));

        Assert.assertEquals(
            "Should advance position",
            lastBytesRead > 0 ? offset + lastBytesRead : offset,
            stream.getPos());
      }

      Assert.assertEquals(
          "Should read fewer bytes at end of buffer", bytes.length % size, lastBytesRead % size);

      for (int i = 0; i < bytes.length; i += 1) {
        Assert.assertEquals("Byte i should be i", i, bytes[i]);
      }

      Assert.assertEquals("Should have no more remaining content", 2, stream.available());

      Assert.assertEquals("Should return 2 more bytes", 2, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content", 0, stream.available());

      Assert.assertEquals("Should return -1 at end of stream", -1, stream.read(bytes));

      Assert.assertEquals("Should have no more remaining content", 0, stream.available());
    }

    checkOriginalData();
  }

  @Test
  public void testReadByte() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    for (int i = 0; i < length; i += 1) {
      Assert.assertEquals("Position should increment", i, stream.getPos());
      Assert.assertEquals(i, stream.read());
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
    Assert.assertNotNull("slice(0) should produce a non-null buffer", empty);
    Assert.assertEquals("slice(0) should produce an empty buffer", 0, empty.remaining());

    Assert.assertEquals("Position should be at start", 0, stream.getPos());

    int i = 0;
    while (stream.available() > 0) {
      int bytesToSlice = Math.min(stream.available(), 10);
      ByteBuffer buffer = stream.slice(bytesToSlice);

      for (int j = 0; j < bytesToSlice; j += 1) {
        Assert.assertEquals("Data should be correct", i + j, buffer.get());
      }

      i += bytesToSlice;
    }

    Assert.assertEquals("Position should be at end", length, stream.getPos());

    checkOriginalData();
  }

  @Test
  public void testSliceBuffers0() throws Exception {
    ByteBufferInputStream stream = newStream();

    Assert.assertEquals(
        "Should return an empty list", Collections.emptyList(), stream.sliceBuffers(0));
  }

  @Test
  public void testWholeSliceBuffers() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    List<ByteBuffer> buffers = stream.sliceBuffers(stream.available());

    Assert.assertEquals("Should consume all buffers", length, stream.getPos());

    Assertions.assertThatThrownBy(() -> stream.sliceBuffers(length))
        .isInstanceOf(EOFException.class)
        .hasMessage(null);

    ByteBufferInputStream copy = ByteBufferInputStream.wrap(buffers);
    for (int i = 0; i < length; i += 1) {
      Assert.assertEquals("Slice should have identical data", i, copy.read());
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

      Assert.assertEquals("Should consume all content", length, stream.getPos());

      ByteBufferInputStream newStream = new MultiBufferInputStream(buffers);

      for (int i = 0; i < length; i += 1) {
        Assert.assertEquals("Data should be correct", i, newStream.read());
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
    Assert.assertEquals(
        "Should advance the original stream", length - sliceLength, stream.available());
    Assert.assertEquals(
        "Should advance the original stream position", sliceLength, stream.getPos());

    Assert.assertEquals("Should return a slice of the first buffer", 1, buffers.size());

    ByteBuffer buffer = buffers.get(0);
    Assert.assertEquals("Should have requested bytes", sliceLength, buffer.remaining());

    // read the buffer one past the returned limit. this should not change the
    // next value in the original stream
    buffer.limit(sliceLength + 1);
    for (int i = 0; i < sliceLength + 1; i += 1) {
      Assert.assertEquals("Should have correct data", i, buffer.get());
    }

    Assert.assertEquals(
        "Reading a slice shouldn't advance the original stream", sliceLength, stream.getPos());
    Assert.assertEquals(
        "Reading a slice shouldn't change the underlying data", sliceLength, stream.read());

    // change the underlying data buffer
    buffer.limit(sliceLength + 2);
    int originalValue = buffer.duplicate().get();
    ByteBuffer undoBuffer = buffer.duplicate();

    try {
      buffer.put((byte) 255);

      Assert.assertEquals(
          "Writing to a slice shouldn't advance the original stream",
          sliceLength + 1,
          stream.getPos());
      Assert.assertEquals(
          "Writing to a slice should change the underlying data", 255, stream.read());

    } finally {
      undoBuffer.put((byte) originalValue);
    }
  }

  @Test
  public void testSkip() throws Exception {
    ByteBufferInputStream stream = newStream();

    while (stream.available() > 0) {
      int bytesToSkip = Math.min(stream.available(), 10);
      Assert.assertEquals(
          "Should skip all, regardless of backing buffers", bytesToSkip, stream.skip(bytesToSkip));
    }

    stream = newStream();
    Assert.assertEquals(0, stream.skip(0));

    int length = stream.available();
    Assert.assertEquals("Should stop at end when out of bytes", length, stream.skip(length + 10));
    Assert.assertEquals("Should return -1 when at end", -1, stream.skip(10));
  }

  @Test
  public void testSkipFully() throws Exception {
    ByteBufferInputStream stream = newStream();

    long lastPosition = 0;
    while (stream.available() > 0) {
      int bytesToSkip = Math.min(stream.available(), 10);

      stream.skipFully(bytesToSkip);

      Assert.assertEquals(
          "Should skip all, regardless of backing buffers",
          bytesToSkip,
          stream.getPos() - lastPosition);

      lastPosition = stream.getPos();
    }

    ByteBufferInputStream stream2 = newStream();
    stream2.skipFully(0);
    Assert.assertEquals(0, stream2.getPos());

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

    Assert.assertEquals("Position should return to the mark", mark, stream.getPos());

    byte[] afterReset = new byte[100];
    int bytesReadAfterReset = stream.read(afterReset);

    Assert.assertEquals(
        "Should read the same number of bytes", expectedBytesRead, bytesReadAfterReset);

    Assert.assertEquals("Read should end at the same position", end, stream.getPos());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
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

    Assert.assertEquals("Position should return to the mark", mark, stream.getPos());

    byte[] afterReset = new byte[100];
    int bytesReadAfterReset = stream.read(afterReset);

    Assert.assertEquals(
        "Should read the same number of bytes", expectedBytesRead, bytesReadAfterReset);

    Assert.assertEquals("Read should end at the same position", end, stream.getPos());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
  }

  @Test
  public void testMarkAtStart() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[10];
    Assert.assertEquals("Should read 10 bytes", 10, stream.read(expected));

    long end = stream.getPos();

    stream.reset();

    Assert.assertEquals("Position should return to the mark", mark, stream.getPos());

    byte[] afterReset = new byte[10];
    Assert.assertEquals("Should read 10 bytes", 10, stream.read(afterReset));

    Assert.assertEquals("Read should end at the same position", end, stream.getPos());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
  }

  @Test
  public void testMarkAtEnd() throws Exception {
    ByteBufferInputStream stream = newStream();

    int bytesRead = stream.read(new byte[100]);
    Assert.assertTrue("Should read to end of stream", bytesRead < 100);

    stream.mark(100);

    long mark = stream.getPos();

    byte[] expected = new byte[10];
    Assert.assertEquals("Should read 0 bytes", -1, stream.read(expected));

    long end = stream.getPos();

    stream.reset();

    Assert.assertEquals("Position should return to the mark", mark, stream.getPos());

    byte[] afterReset = new byte[10];
    Assert.assertEquals("Should read 0 bytes", -1, stream.read(afterReset));

    Assert.assertEquals("Read should end at the same position", end, stream.getPos());

    Assert.assertArrayEquals("Content should be equal", expected, afterReset);
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
    Assert.assertEquals("Should read expected bytes", expected.length, stream.read(expected));

    stream.reset();
    stream.mark(10);

    byte[] firstRead = new byte[6];
    Assert.assertEquals("Should read firstRead bytes", firstRead.length, stream.read(firstRead));

    stream.reset();

    byte[] secondRead = new byte[6];
    Assert.assertEquals("Should read secondRead bytes", secondRead.length, stream.read(secondRead));

    Assert.assertArrayEquals("First read should be correct", expected, firstRead);

    Assert.assertArrayEquals("Second read should be correct", expected, secondRead);
  }

  @Test
  public void testMarkLimit() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(5);
    Assert.assertEquals("Should read 5 bytes", 5, stream.read(new byte[5]));

    stream.reset();

    Assert.assertEquals("Should read 6 bytes", 6, stream.read(new byte[6]));

    Assertions.assertThatThrownBy(stream::reset)
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No mark defined");
  }

  @Test
  public void testMarkDoubleReset() throws Exception {
    ByteBufferInputStream stream = newStream();

    stream.mark(5);
    Assert.assertEquals("Should read 5 bytes", 5, stream.read(new byte[5]));

    stream.reset();

    Assertions.assertThatThrownBy(stream::reset)
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No mark defined");
  }
}
