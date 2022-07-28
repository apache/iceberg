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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestMultiBufferInputStream extends TestByteBufferInputStreams {
  private static final List<ByteBuffer> DATA =
      Arrays.asList(
          ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}),
          ByteBuffer.wrap(new byte[] {9, 10, 11, 12}),
          ByteBuffer.wrap(new byte[] {}),
          ByteBuffer.wrap(new byte[] {13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}),
          ByteBuffer.wrap(new byte[] {25}),
          ByteBuffer.wrap(new byte[] {26, 27, 28, 29, 30, 31, 32}),
          ByteBuffer.wrap(new byte[] {33, 34}));

  @Override
  protected ByteBufferInputStream newStream() {
    return new MultiBufferInputStream(DATA);
  }

  @Override
  protected void checkOriginalData() {
    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change", buffer.array().length, buffer.limit());
    }
  }

  @Test
  @SuppressWarnings("LocalVariableName")
  public void testSliceData() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    List<ByteBuffer> buffers = Lists.newArrayList();
    // slice the stream into 3 8-byte buffers and 1 2-byte buffer
    while (stream.available() > 0) {
      int bytesToSlice = Math.min(stream.available(), 8);
      buffers.add(stream.slice(bytesToSlice));
    }

    Assert.assertEquals("Position should be at end", length, stream.getPos());
    Assert.assertEquals("Should produce 5 buffers", 5, buffers.size());

    int i = 0;

    // one is a view of the first buffer because it is smaller
    ByteBuffer one = buffers.get(0);
    Assert.assertSame("Should be a duplicate of the first array", one.array(), DATA.get(0).array());
    Assert.assertEquals(8, one.remaining());
    Assert.assertEquals(0, one.position());
    Assert.assertEquals(8, one.limit());
    Assert.assertEquals(9, one.capacity());
    for (; i < 8; i += 1) {
      Assert.assertEquals("Should produce correct values", i, one.get());
    }

    // two should be a copy of the next 8 bytes
    ByteBuffer two = buffers.get(1);
    Assert.assertEquals(8, two.remaining());
    Assert.assertEquals(0, two.position());
    Assert.assertEquals(8, two.limit());
    Assert.assertEquals(8, two.capacity());
    for (; i < 16; i += 1) {
      Assert.assertEquals("Should produce correct values", i, two.get());
    }

    // three is a copy of part of the 4th buffer
    ByteBuffer three = buffers.get(2);
    Assert.assertSame(
        "Should be a duplicate of the fourth array", three.array(), DATA.get(3).array());
    Assert.assertEquals(8, three.remaining());
    Assert.assertEquals(3, three.position());
    Assert.assertEquals(11, three.limit());
    Assert.assertEquals(12, three.capacity());
    for (; i < 24; i += 1) {
      Assert.assertEquals("Should produce correct values", i, three.get());
    }

    // four should be a copy of the next 8 bytes
    ByteBuffer four = buffers.get(3);
    Assert.assertEquals(8, four.remaining());
    Assert.assertEquals(0, four.position());
    Assert.assertEquals(8, four.limit());
    Assert.assertEquals(8, four.capacity());
    for (; i < 32; i += 1) {
      Assert.assertEquals("Should produce correct values", i, four.get());
    }

    // five should be a copy of the next 8 bytes
    ByteBuffer five = buffers.get(4);
    Assert.assertEquals(3, five.remaining());
    Assert.assertEquals(0, five.position());
    Assert.assertEquals(3, five.limit());
    Assert.assertEquals(3, five.capacity());
    for (; i < 35; i += 1) {
      Assert.assertEquals("Should produce correct values", i, five.get());
    }
  }

  @Test
  public void testSliceBuffersData() throws Exception {
    ByteBufferInputStream stream = newStream();

    List<ByteBuffer> buffers = stream.sliceBuffers(stream.available());
    List<ByteBuffer> nonEmptyBuffers = Lists.newArrayList();
    for (ByteBuffer buffer : DATA) {
      if (buffer.remaining() > 0) {
        nonEmptyBuffers.add(buffer);
      }
    }

    Assert.assertEquals(
        "Should return duplicates of all non-empty buffers", nonEmptyBuffers, buffers);
  }
}
