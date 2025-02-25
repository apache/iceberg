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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class VariantUtil {
  private static final int BASIC_TYPE_MASK = 0b11;
  private static final int BASIC_TYPE_PRIMITIVE = 0;
  private static final int BASIC_TYPE_SHORT_STRING = 1;
  private static final int BASIC_TYPE_OBJECT = 2;
  private static final int BASIC_TYPE_ARRAY = 3;

  // TODO: Implement PhysicalType.TIME
  // TODO: Implement PhysicalType.TIMESTAMPNTZ_NANO and PhysicalType.TIMESTAMPTZ_NANO
  // TODO: Implement PhysicalType.UUID
  private static final Map<Type, PhysicalType> NO_CONVERSION_NEEDED =
      ImmutableMap.<Type, PhysicalType>builder()
          .put(Types.IntegerType.get(), PhysicalType.INT32)
          .put(Types.LongType.get(), PhysicalType.INT64)
          .put(Types.FloatType.get(), PhysicalType.FLOAT)
          .put(Types.DoubleType.get(), PhysicalType.DOUBLE)
          .put(Types.DateType.get(), PhysicalType.DATE)
          .put(Types.TimestampType.withoutZone(), PhysicalType.TIMESTAMPNTZ)
          .put(Types.TimestampType.withZone(), PhysicalType.TIMESTAMPTZ)
          .put(Types.StringType.get(), PhysicalType.STRING)
          .put(Types.BinaryType.get(), PhysicalType.BINARY)
          .put(Types.UnknownType.get(), PhysicalType.NULL)
          .build();

  private VariantUtil() {}

  @SuppressWarnings("unchecked")
  public static <T> T castTo(VariantValue value, Type type) {
    if (value == null) {
      return null;
    } else if (NO_CONVERSION_NEEDED.get(type) == value.type()) {
      return (T) value.asPrimitive().get();
    }

    switch (type.typeId()) {
      case INTEGER:
        switch (value.type()) {
          case INT8:
          case INT16:
            return (T) (Integer) ((Number) value.asPrimitive().get()).intValue();
        }

        break;
      case LONG:
        switch (value.type()) {
          case INT8:
          case INT16:
          case INT32:
            return (T) (Long) ((Number) value.asPrimitive().get()).longValue();
        }

        break;
      case DOUBLE:
        if (value.type() == PhysicalType.FLOAT) {
          return (T) (Double) ((Number) value.asPrimitive().get()).doubleValue();
        }

        break;
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) type;
        if (value.type() == PhysicalType.BINARY) {
          ByteBuffer buffer = (ByteBuffer) value.asPrimitive().get();
          if (buffer.remaining() == fixedType.length()) {
            return (T) buffer;
          }
        }

        break;
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) type;
        switch (value.type()) {
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            BigDecimal decimalValue = (BigDecimal) value.asPrimitive().get();
            if (decimalValue.scale() == decimalType.scale()) {
              return (T) decimalValue;
            }
        }

        break;
      case BOOLEAN:
        switch (value.type()) {
          case BOOLEAN_FALSE:
            return (T) Boolean.FALSE;
          case BOOLEAN_TRUE:
            return (T) Boolean.TRUE;
        }

        break;
    }

    return null;
  }

  /** A hacky absolute put for ByteBuffer */
  static int writeBufferAbsolute(ByteBuffer buffer, int offset, ByteBuffer toCopy) {
    int originalPosition = buffer.position();
    buffer.position(offset);
    ByteBuffer copy = toCopy.duplicate();
    buffer.put(copy); // duplicate so toCopy is not modified
    buffer.position(originalPosition);
    Preconditions.checkArgument(copy.remaining() <= 0, "Not fully written");
    return toCopy.remaining();
  }

  static void writeByte(ByteBuffer buffer, int value, int offset) {
    buffer.put(buffer.position() + offset, (byte) (value & 0xFF));
  }

  static void writeLittleEndianUnsigned(ByteBuffer buffer, int value, int offset, int size) {
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

  static byte readLittleEndianInt8(ByteBuffer buffer, int offset) {
    return buffer.get(buffer.position() + offset);
  }

  static short readLittleEndianInt16(ByteBuffer buffer, int offset) {
    return buffer.getShort(buffer.position() + offset);
  }

  static int readByte(ByteBuffer buffer, int offset) {
    return buffer.get(buffer.position() + offset) & 0xFF;
  }

  static int readLittleEndianUnsigned(ByteBuffer buffer, int offset, int size) {
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

  static int readLittleEndianInt32(ByteBuffer buffer, int offset) {
    return buffer.getInt(buffer.position() + offset);
  }

  static long readLittleEndianInt64(ByteBuffer buffer, int offset) {
    return buffer.getLong(buffer.position() + offset);
  }

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
