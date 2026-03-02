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
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.UUIDUtil;

class PrimitiveWrapper<T> implements VariantPrimitive<T> {
  private static final byte NULL_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_NULL);
  private static final byte TRUE_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_TRUE);
  private static final byte FALSE_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_FALSE);
  private static final byte INT8_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_INT8);
  private static final byte INT16_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_INT16);
  private static final byte INT32_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_INT32);
  private static final byte INT64_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_INT64);
  private static final byte FLOAT_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_FLOAT);
  private static final byte DOUBLE_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_DOUBLE);
  private static final byte DATE_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_DATE);
  private static final byte TIMESTAMPTZ_HEADER =
      VariantUtil.primitiveHeader(Primitives.TYPE_TIMESTAMPTZ);
  private static final byte TIMESTAMPNTZ_HEADER =
      VariantUtil.primitiveHeader(Primitives.TYPE_TIMESTAMPNTZ);
  private static final byte DECIMAL4_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_DECIMAL4);
  private static final byte DECIMAL8_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_DECIMAL8);
  private static final byte DECIMAL16_HEADER =
      VariantUtil.primitiveHeader(Primitives.TYPE_DECIMAL16);
  private static final byte BINARY_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_BINARY);
  private static final byte STRING_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_STRING);
  private static final byte TIME_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_TIME);
  private static final byte TIMESTAMPTZ_NANOS_HEADER =
      VariantUtil.primitiveHeader(Primitives.TYPE_TIMESTAMPTZ_NANOS);
  private static final byte TIMESTAMPNTZ_NANOS_HEADER =
      VariantUtil.primitiveHeader(Primitives.TYPE_TIMESTAMPNTZ_NANOS);
  private static final byte UUID_HEADER = VariantUtil.primitiveHeader(Primitives.TYPE_UUID);
  private static final int MAX_SHORT_STRING_LENGTH = 63;

  private final PhysicalType type;
  private final T value;
  private ByteBuffer buffer = null;

  PrimitiveWrapper(PhysicalType type, T value) {
    if (value instanceof Boolean boolValue
        && (type == PhysicalType.BOOLEAN_TRUE || type == PhysicalType.BOOLEAN_FALSE)) {
      // set the boolean type from the value so that callers can use BOOLEAN_* interchangeably
      this.type = boolValue ? PhysicalType.BOOLEAN_TRUE : PhysicalType.BOOLEAN_FALSE;
    } else {
      this.type = type;
    }
    this.value = value;
  }

  @Override
  public PhysicalType type() {
    return type;
  }

  @Override
  public T get() {
    return value;
  }

  @Override
  public int sizeInBytes() {
    return switch (type()) {
      case NULL, BOOLEAN_TRUE, BOOLEAN_FALSE -> 1; // 1 header only
      case INT8 -> 2; // 1 header + 1 value
      case INT16 -> 3; // 1 header + 2 value
      case INT32, DATE, FLOAT -> 5; // 1 header + 4 value
      case INT64, DOUBLE, TIMESTAMPTZ, TIMESTAMPNTZ, TIMESTAMPTZ_NANOS, TIMESTAMPNTZ_NANOS, TIME ->
          9; // 1 header + 8 value
      case DECIMAL4 -> 6; // 1 header + 1 scale + 4 unscaled value
      case DECIMAL8 -> 10; // 1 header + 1 scale + 8 unscaled value
      case DECIMAL16 -> 18; // 1 header + 1 scale + 16 unscaled value
      case BINARY -> 5 + ((ByteBuffer) value).remaining(); // 1 header + 4 length + value length
      case STRING -> {
        if (null == buffer) {
          this.buffer = ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
        }

        if (buffer.remaining() <= MAX_SHORT_STRING_LENGTH) {
          yield 1 + buffer.remaining(); // 1 header + value length
        }

        yield 5 + buffer.remaining(); // 1 header + 4 length + value length
      }
      case UUID -> 1 + 16; // 1 header + 16 length
      default -> throw new UnsupportedOperationException("Unsupported primitive type: " + type());
    };
  }

  @Override
  public int writeTo(ByteBuffer outBuffer, int offset) {
    Preconditions.checkArgument(
        outBuffer.order() == ByteOrder.LITTLE_ENDIAN, "Invalid byte order: big endian");
    return switch (type()) {
      case NULL -> {
        outBuffer.put(offset, NULL_HEADER);
        yield 1;
      }
      case BOOLEAN_TRUE -> {
        outBuffer.put(offset, TRUE_HEADER);
        yield 1;
      }
      case BOOLEAN_FALSE -> {
        outBuffer.put(offset, FALSE_HEADER);
        yield 1;
      }
      case INT8 -> {
        outBuffer.put(offset, INT8_HEADER);
        outBuffer.put(offset + 1, (Byte) value);
        yield 2;
      }
      case INT16 -> {
        outBuffer.put(offset, INT16_HEADER);
        outBuffer.putShort(offset + 1, (Short) value);
        yield 3;
      }
      case INT32 -> {
        outBuffer.put(offset, INT32_HEADER);
        outBuffer.putInt(offset + 1, (Integer) value);
        yield 5;
      }
      case INT64 -> {
        outBuffer.put(offset, INT64_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case FLOAT -> {
        outBuffer.put(offset, FLOAT_HEADER);
        outBuffer.putFloat(offset + 1, (Float) value);
        yield 5;
      }
      case DOUBLE -> {
        outBuffer.put(offset, DOUBLE_HEADER);
        outBuffer.putDouble(offset + 1, (Double) value);
        yield 9;
      }
      case DATE -> {
        outBuffer.put(offset, DATE_HEADER);
        outBuffer.putInt(offset + 1, (Integer) value);
        yield 5;
      }
      case TIMESTAMPTZ -> {
        outBuffer.put(offset, TIMESTAMPTZ_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case TIMESTAMPNTZ -> {
        outBuffer.put(offset, TIMESTAMPNTZ_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case DECIMAL4 -> {
        BigDecimal decimal4 = (BigDecimal) value;
        outBuffer.put(offset, DECIMAL4_HEADER);
        outBuffer.put(offset + 1, (byte) decimal4.scale());
        outBuffer.putInt(offset + 2, decimal4.unscaledValue().intValueExact());
        yield 6;
      }
      case DECIMAL8 -> {
        BigDecimal decimal8 = (BigDecimal) value;
        outBuffer.put(offset, DECIMAL8_HEADER);
        outBuffer.put(offset + 1, (byte) decimal8.scale());
        outBuffer.putLong(offset + 2, decimal8.unscaledValue().longValueExact());
        yield 10;
      }
      case DECIMAL16 -> {
        BigDecimal decimal16 = (BigDecimal) value;
        byte padding = (byte) (decimal16.signum() < 0 ? 0xFF : 0x00);
        byte[] bytes = decimal16.unscaledValue().toByteArray();
        outBuffer.put(offset, DECIMAL16_HEADER);
        outBuffer.put(offset + 1, (byte) decimal16.scale());
        for (int i = 0; i < 16; i += 1) {
          if (i < bytes.length) {
            // copy the big endian value and convert to little endian
            outBuffer.put(offset + 2 + i, bytes[bytes.length - i - 1]);
          } else {
            // pad with 0x00 or 0xFF depending on the sign
            outBuffer.put(offset + 2 + i, padding);
          }
        }

        yield 18;
      }
      case BINARY -> {
        ByteBuffer binary = (ByteBuffer) value;
        outBuffer.put(offset, BINARY_HEADER);
        outBuffer.putInt(offset + 1, binary.remaining());
        VariantUtil.writeBufferAbsolute(outBuffer, offset + 5, binary);
        yield 5 + binary.remaining();
      }
      case STRING -> {
        if (null == buffer) {
          this.buffer = ByteBuffer.wrap(((String) value).getBytes(StandardCharsets.UTF_8));
        }

        if (buffer.remaining() <= MAX_SHORT_STRING_LENGTH) {
          outBuffer.put(offset, VariantUtil.shortStringHeader(buffer.remaining()));
          VariantUtil.writeBufferAbsolute(outBuffer, offset + 1, buffer);
          yield 1 + buffer.remaining();
        } else {
          outBuffer.put(offset, STRING_HEADER);
          outBuffer.putInt(offset + 1, buffer.remaining());
          VariantUtil.writeBufferAbsolute(outBuffer, offset + 5, buffer);
          yield 5 + buffer.remaining();
        }
      }
      case TIME -> {
        outBuffer.put(offset, TIME_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case TIMESTAMPTZ_NANOS -> {
        outBuffer.put(offset, TIMESTAMPTZ_NANOS_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case TIMESTAMPNTZ_NANOS -> {
        outBuffer.put(offset, TIMESTAMPNTZ_NANOS_HEADER);
        outBuffer.putLong(offset + 1, (Long) value);
        yield 9;
      }
      case UUID -> {
        outBuffer.put(offset, UUID_HEADER);
        VariantUtil.writeBufferAbsolute(
            outBuffer, offset + 1, UUIDUtil.convertToByteBuffer((UUID) value));
        yield 17;
      }
      default -> throw new UnsupportedOperationException("Unsupported primitive type: " + type());
    };
  }

  @Override
  public int hashCode() {
    return VariantPrimitive.hash(this);
  }

  @Override
  public boolean equals(Object other) {
    return VariantPrimitive.equals(this, other);
  }

  @Override
  public String toString() {
    return VariantPrimitive.asString(this);
  }
}
