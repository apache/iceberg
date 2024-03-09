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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

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
      assertThat(buffer.position()).as("Position should not change").isEqualTo(0);
      assertThat(buffer.limit()).as("Limit should not change").isEqualTo(buffer.array().length);
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

    assertThat(stream.getPos()).as("Position should be at end").isEqualTo(length);
    assertThat(buffers.size()).as("Should produce 5 buffers").isEqualTo(5);

    int i = 0;

    // one is a view of the first buffer because it is smaller
    ByteBuffer one = buffers.get(0);
    assertThat(DATA.get(0).array())
        .as("Should be a duplicate of the first array")
        .isSameAs(one.array());
    assertThat(one.remaining()).isEqualTo(8);
    assertThat(one.position()).isEqualTo(0);
    assertThat(one.limit()).isEqualTo(8);
    assertThat(one.capacity()).isEqualTo(9);
    for (; i < 8; i += 1) {
      assertThat(one.get()).as("Should produce correct values").isEqualTo((byte) i);
    }

    // two should be a copy of the next 8 bytes
    ByteBuffer two = buffers.get(1);
    assertThat(two.remaining()).isEqualTo(8);
    assertThat(two.position()).isEqualTo(0);
    assertThat(two.limit()).isEqualTo(8);
    assertThat(two.capacity()).isEqualTo(8);
    for (; i < 16; i += 1) {
      assertThat(two.get()).as("Should produce correct values").isEqualTo((byte) i);
    }

    // three is a copy of part of the 4th buffer
    ByteBuffer three = buffers.get(2);
    assertThat(DATA.get(3).array())
        .as("Should be a duplicate of the fourth array")
        .isSameAs(three.array());
    assertThat(three.remaining()).isEqualTo(8);
    assertThat(three.position()).isEqualTo(3);
    assertThat(three.limit()).isEqualTo(11);
    assertThat(three.capacity()).isEqualTo(12);
    for (; i < 24; i += 1) {
      assertThat(three.get()).as("Should produce correct values").isEqualTo((byte) i);
    }

    // four should be a copy of the next 8 bytes
    ByteBuffer four = buffers.get(3);
    assertThat(four.remaining()).isEqualTo(8);
    assertThat(four.position()).isEqualTo(0);
    assertThat(four.limit()).isEqualTo(8);
    assertThat(four.capacity()).isEqualTo(8);
    for (; i < 32; i += 1) {
      assertThat(four.get()).as("Should produce correct values").isEqualTo((byte) i);
    }

    // five should be a copy of the next 8 bytes
    ByteBuffer five = buffers.get(4);
    assertThat(five.remaining()).isEqualTo(3);
    assertThat(five.position()).isEqualTo(0);
    assertThat(five.limit()).isEqualTo(3);
    assertThat(five.capacity()).isEqualTo(3);
    for (; i < 35; i += 1) {
      assertThat(five.get()).as("Should produce correct values").isEqualTo((byte) i);
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

    assertThat(buffers)
        .as("Should return duplicates of all non-empty buffers")
        .isEqualTo(nonEmptyBuffers);
  }
}
