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
package org.apache.iceberg.variants;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

class VariantUtil {
  private static final int BASIC_TYPE_MASK = 0b11;
  private static final int BASIC_TYPE_PRIMITIVE = 0;
  private static final int BASIC_TYPE_SHORT_STRING = 1;
  private static final int BASIC_TYPE_OBJECT = 2;
  private static final int BASIC_TYPE_ARRAY = 3;

  private VariantUtil() {}

  static float readFloat(ByteBuffer buffer, int offset) {
    return buffer.getFloat(buffer.position() + offset);
  }

  static double readDouble(ByteBuffer buffer, int offset) {
    return buffer.getDouble(buffer.position() + offset);
  }

  static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
    ByteBuffer slice = buffer.duplicate();
    slice.order(ByteOrder.LITTLE_ENDIAN);
    slice.position(buffer.position() + offset);
    slice.limit(buffer.position() + offset + length);
    return slice;
  }

  static String readString(ByteBuffer buffer, int offset, int length) {
    if (buffer.hasArray()) {
      return new String(
          buffer.array(),
          buffer.arrayOffset() + buffer.position() + offset,
          length,
          StandardCharsets.UTF_8);
    } else {
      return StandardCharsets.UTF_8.decode(slice(buffer, offset, length)).toString();
    }
  }

  static <T extends Comparable<T>> int find(int size, T key, Function<Integer, T> resolve) {
    int low = 0;
    int high = size - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      T value = resolve.apply(mid);
      int cmp = key.compareTo(value);
      if (cmp == 0) {
        return mid;
      } else if (cmp < 0) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }

    return -1;
  }

  static int sizeOf(int maxValue) {
    if (maxValue <= 0xFF) {
      return 1;
    } else if (maxValue <= 0xFFFF) {
      return 2;
    } else if (maxValue <= 0xFFFFFF) {
      return 3;
    } else {
      return 4;
    }
  }

  static byte metadataHeader(boolean isSorted, int offsetSize) {
    return (byte) (((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001);
  }

  static byte primitiveHeader(int primitiveType) {
    return (byte) (primitiveType << Primitives.PRIMITIVE_TYPE_SHIFT);
  }

  static byte objectHeader(boolean isLarge, int fieldIdSize, int offsetSize) {
    return (byte)
        ((isLarge ? 0b1000000 : 0) | ((fieldIdSize - 1) << 4) | ((offsetSize - 1) << 2) | 0b10);
  }

  static byte arrayHeader(boolean isLarge, int offsetSize) {
    return (byte) ((isLarge ? 0b10000 : 0) | (offsetSize - 1) << 2 | 0b11);
  }

  static byte shortStringHeader(int length) {
    return (byte) ((length << 2) | BASIC_TYPE_SHORT_STRING);
  }

  static BasicType basicType(int header) {
    int basicType = header & BASIC_TYPE_MASK;
    switch (basicType) {
      case BASIC_TYPE_PRIMITIVE:
        return BasicType.PRIMITIVE;
      case BASIC_TYPE_SHORT_STRING:
        return BasicType.SHORT_STRING;
      case BASIC_TYPE_OBJECT:
        return BasicType.OBJECT;
      case BASIC_TYPE_ARRAY:
        return BasicType.ARRAY;
    }

    throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
  }
}
