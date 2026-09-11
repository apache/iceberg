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
package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class ByteBuffers {

  private ByteBuffers() {}

  public static byte[] toByteArray(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      if (buffer.arrayOffset() == 0
          && buffer.position() == 0
          && array.length == buffer.remaining()) {
        return array;
      } else {
        int start = buffer.arrayOffset() + buffer.position();
        int end = start + buffer.remaining();
        return Arrays.copyOfRange(array, start, end);
      }
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.asReadOnlyBuffer().get(bytes);
      return bytes;
    }
  }

  public static ByteBuffer reuse(ByteBuffer reuse, int length) {
    Preconditions.checkArgument(reuse.hasArray(), "Cannot reuse a buffer not backed by an array");
    Preconditions.checkArgument(
        reuse.arrayOffset() == 0, "Cannot reuse a buffer whose array offset is not 0");
    Preconditions.checkArgument(
        reuse.capacity() == length,
        "Cannot use a buffer whose capacity (%s) is not equal to the requested length (%s)",
        length,
        reuse.capacity());
    reuse.position(0);
    reuse.limit(length);
    return reuse;
  }

  public static ByteBuffer copy(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }

    byte[] copyArray = new byte[buffer.remaining()];
    ByteBuffer readerBuffer = buffer.asReadOnlyBuffer();
    readerBuffer.get(copyArray);

    return ByteBuffer.wrap(copyArray);
  }

  public static void writeByte(ByteBuffer buffer, int value, int offset) {
    buffer.put(buffer.position() + offset, (byte) (value & 0xFF));
  }

  public static void writeLittleEndianUnsigned(ByteBuffer buffer, int value, int offset, int size) {
    int base = buffer.position() + offset;
    switch (size) {
      case 4:
        buffer.putInt(base, value);
        return;
      case 3:
        buffer.putShort(base, (short) (value & 0xFFFF));
        buffer.put(base + 2, (byte) ((value >> 16) & 0xFF));
        return;
      case 2:
        buffer.putShort(base, (short) (value & 0xFFFF));
        return;
      case 1:
        buffer.put(base, (byte) (value & 0xFF));
        return;
    }

    throw new IllegalArgumentException("Invalid size: " + size);
  }

  public static byte readLittleEndianInt8(ByteBuffer buffer, int offset) {
    return buffer.get(buffer.position() + offset);
  }

  public static short readLittleEndianInt16(ByteBuffer buffer, int offset) {
    return buffer.getShort(buffer.position() + offset);
  }

  public static int readByte(ByteBuffer buffer, int offset) {
    return buffer.get(buffer.position() + offset) & 0xFF;
  }

  public static int readLittleEndianUnsigned(ByteBuffer buffer, int offset, int size) {
    int base = buffer.position() + offset;
    switch (size) {
      case 4:
        return buffer.getInt(base);
      case 3:
        return (((int) buffer.getShort(base)) & 0xFFFF) | ((buffer.get(base + 2) & 0xFF) << 16);
      case 2:
        return ((int) buffer.getShort(base)) & 0xFFFF;
      case 1:
        return buffer.get(base) & 0xFF;
    }

    throw new IllegalArgumentException("Invalid size: " + size);
  }

  public static int readLittleEndianInt32(ByteBuffer buffer, int offset) {
    return buffer.getInt(buffer.position() + offset);
  }

  public static long readLittleEndianInt64(ByteBuffer buffer, int offset) {
    return buffer.getLong(buffer.position() + offset);
  }
}
