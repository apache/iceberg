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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.UUIDUtil;

class SerializedPrimitive implements VariantPrimitive<Object>, SerializedValue, Serializable {
  private static final int PRIMITIVE_TYPE_SHIFT = 2;
  private static final int PRIMITIVE_OFFSET = 1;

  static SerializedPrimitive from(byte[] bytes) {
    return from(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static SerializedPrimitive from(ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    BasicType basicType = VariantUtil.basicType(header);
    Preconditions.checkArgument(
        basicType == BasicType.PRIMITIVE,
        "Invalid primitive, basic type != PRIMITIVE: " + basicType);
    return new SerializedPrimitive(value, header);
  }

  private final ByteBuffer value;
  private final PhysicalType type;
  private Object primitive = null;

  private SerializedPrimitive(ByteBuffer value, int header) {
    this.value = value;
    this.type = PhysicalType.from(header >> PRIMITIVE_TYPE_SHIFT);
    long requiredBytes = PRIMITIVE_OFFSET + payloadSize(type, value);
    Preconditions.checkArgument(
        requiredBytes <= value.remaining(),
        "Invalid variant primitive: %s payload extends past buffer",
        type);
  }

  private static long payloadSize(PhysicalType type, ByteBuffer value) {
    return switch (type) {
      case NULL, BOOLEAN_TRUE, BOOLEAN_FALSE -> 0;
      case INT8 -> 1;
      case INT16 -> 2;
      case INT32, DATE, FLOAT -> 4;
      case INT64, TIMESTAMPTZ, TIMESTAMPNTZ, TIME, TIMESTAMPTZ_NANOS, TIMESTAMPNTZ_NANOS, DOUBLE ->
          8;
      case DECIMAL4 -> 5;
      case DECIMAL8 -> 9;
      case DECIMAL16 -> 17;
      case UUID -> 16;
      case BINARY, STRING -> {
        Preconditions.checkArgument(
            PRIMITIVE_OFFSET + 4 <= value.remaining(),
            "Invalid variant primitive: %s size field extends past buffer",
            type);
        int size = ByteBuffers.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
        Preconditions.checkArgument(
            size >= 0, "Invalid variant primitive: negative %s size %s", type, size);
        yield 4L + size;
      }
      default -> throw new UnsupportedOperationException("Unsupported primitive type: " + type);
    };
  }

  private Object read() {
    return switch (type) {
      case NULL -> null;
      case BOOLEAN_TRUE -> true;
      case BOOLEAN_FALSE -> false;
      case INT8 -> ByteBuffers.readLittleEndianInt8(value, PRIMITIVE_OFFSET);
      case INT16 -> ByteBuffers.readLittleEndianInt16(value, PRIMITIVE_OFFSET);
      case INT32, DATE -> ByteBuffers.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
      case INT64, TIMESTAMPTZ, TIMESTAMPNTZ, TIME, TIMESTAMPTZ_NANOS, TIMESTAMPNTZ_NANOS ->
          ByteBuffers.readLittleEndianInt64(value, PRIMITIVE_OFFSET);
      case FLOAT -> VariantUtil.readFloat(value, PRIMITIVE_OFFSET);
      case DOUBLE -> VariantUtil.readDouble(value, PRIMITIVE_OFFSET);
      case DECIMAL4 -> {
        int scale = ByteBuffers.readByte(value, PRIMITIVE_OFFSET);
        int unscaled = ByteBuffers.readLittleEndianInt32(value, PRIMITIVE_OFFSET + 1);
        yield new BigDecimal(BigInteger.valueOf(unscaled), scale);
      }
      case DECIMAL8 -> {
        int scale = ByteBuffers.readByte(value, PRIMITIVE_OFFSET);
        long unscaled = ByteBuffers.readLittleEndianInt64(value, PRIMITIVE_OFFSET + 1);
        yield new BigDecimal(BigInteger.valueOf(unscaled), scale);
      }
      case DECIMAL16 -> {
        int scale = ByteBuffers.readByte(value, PRIMITIVE_OFFSET);
        byte[] unscaled = new byte[16];
        for (int i = 0; i < 16; i += 1) {
          unscaled[i] = (byte) ByteBuffers.readByte(value, PRIMITIVE_OFFSET + 16 - i);
        }
        yield new BigDecimal(new BigInteger(unscaled), scale);
      }
      case BINARY -> {
        int size = ByteBuffers.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
        yield VariantUtil.slice(value, PRIMITIVE_OFFSET + 4, size);
      }
      case STRING -> {
        int size = ByteBuffers.readLittleEndianInt32(value, PRIMITIVE_OFFSET);
        yield VariantUtil.readString(value, PRIMITIVE_OFFSET + 4, size);
      }
      case UUID ->
          UUIDUtil.convert(
              VariantUtil.slice(value, PRIMITIVE_OFFSET, 16).order(ByteOrder.BIG_ENDIAN));
      default -> throw new UnsupportedOperationException("Unsupported primitive type: " + type);
    };
  }

  @Override
  public PhysicalType type() {
    return type;
  }

  @Override
  public Object get() {
    if (null == primitive) {
      this.primitive = read();
    }
    return primitive;
  }

  @Override
  public ByteBuffer buffer() {
    return value;
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

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Serializable {
    private final byte[] valueBytes;

    private SerializationProxy(SerializedPrimitive primitive) {
      this.valueBytes = ByteBuffers.toByteArray(primitive.buffer());
    }

    private Object readResolve() {
      return SerializedPrimitive.from(valueBytes);
    }
  }
}
