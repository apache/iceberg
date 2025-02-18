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
import org.apache.iceberg.util.DateTimeUtil;

public class Variants {
  private Variants() {}

  interface Serialized {
    ByteBuffer buffer();
  }

  abstract static class SerializedValue implements VariantValue, Serialized {
    @Override
    public int sizeInBytes() {
      return buffer().remaining();
    }

    @Override
    public int writeTo(ByteBuffer buffer, int offset) {
      ByteBuffer value = buffer();
      VariantUtil.writeBufferAbsolute(buffer, offset, value);
      return value.remaining();
    }
  }

  static final int HEADER_SIZE = 1;

  enum BasicType {
    PRIMITIVE,
    SHORT_STRING,
    OBJECT,
    ARRAY
  }

  public static VariantMetadata emptyMetadata() {
    return SerializedMetadata.EMPTY_V1_METADATA;
  }

  public static VariantMetadata metadata(ByteBuffer metadata) {
    return SerializedMetadata.from(metadata);
  }

  public static VariantValue value(VariantMetadata metadata, ByteBuffer value) {
    int header = VariantUtil.readByte(value, 0);
    BasicType basicType = VariantUtil.basicType(header);
    switch (basicType) {
      case PRIMITIVE:
        return SerializedPrimitive.from(value, header);
      case SHORT_STRING:
        return SerializedShortString.from(value, header);
      case OBJECT:
        return SerializedObject.from(metadata, value, header);
      case ARRAY:
        return SerializedArray.from(metadata, value, header);
    }

    throw new UnsupportedOperationException("Unsupported basic type: " + basicType);
  }

  public static ShreddedObject object(VariantMetadata metadata, VariantObject object) {
    return new ShreddedObject(metadata, object);
  }

  public static ShreddedObject object(VariantMetadata metadata) {
    return new ShreddedObject(metadata);
  }

  public static <T> VariantPrimitive<T> of(PhysicalType type, T value) {
    return new PrimitiveWrapper<>(type, value);
  }

  public static VariantPrimitive<Void> ofNull() {
    return new PrimitiveWrapper<>(PhysicalType.NULL, null);
  }

  public static VariantPrimitive<Boolean> of(boolean value) {
    return new PrimitiveWrapper<>(PhysicalType.BOOLEAN_TRUE, value);
  }

  public static VariantPrimitive<Byte> of(byte value) {
    return new PrimitiveWrapper<>(PhysicalType.INT8, value);
  }

  public static VariantPrimitive<Short> of(short value) {
    return new PrimitiveWrapper<>(PhysicalType.INT16, value);
  }

  public static VariantPrimitive<Integer> of(int value) {
    return new PrimitiveWrapper<>(PhysicalType.INT32, value);
  }

  public static VariantPrimitive<Long> of(long value) {
    return new PrimitiveWrapper<>(PhysicalType.INT64, value);
  }

  public static VariantPrimitive<Float> of(float value) {
    return new PrimitiveWrapper<>(PhysicalType.FLOAT, value);
  }

  public static VariantPrimitive<Double> of(double value) {
    return new PrimitiveWrapper<>(PhysicalType.DOUBLE, value);
  }

  public static VariantPrimitive<Integer> ofDate(int value) {
    return new PrimitiveWrapper<>(PhysicalType.DATE, value);
  }

  public static VariantPrimitive<Integer> ofIsoDate(String value) {
    return ofDate(DateTimeUtil.isoDateToDays(value));
  }

  public static VariantPrimitive<Long> ofTimestamptz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPTZ, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestamptz(String value) {
    return ofTimestamptz(DateTimeUtil.isoTimestamptzToMicros(value));
  }

  public static VariantPrimitive<Long> ofTimestampntz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPNTZ, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestampntz(String value) {
    return ofTimestampntz(DateTimeUtil.isoTimestampToMicros(value));
  }

  public static VariantPrimitive<BigDecimal> of(BigDecimal value) {
    int bitLength = value.unscaledValue().bitLength();
    if (bitLength < 32) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL4, value);
    } else if (bitLength < 64) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL8, value);
    } else if (bitLength < 128) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL16, value);
    }

    throw new UnsupportedOperationException("Unsupported decimal precision: " + value.precision());
  }

  public static VariantPrimitive<ByteBuffer> of(ByteBuffer value) {
    return new PrimitiveWrapper<>(PhysicalType.BINARY, value);
  }

  public static VariantPrimitive<String> of(String value) {
    return new PrimitiveWrapper<>(PhysicalType.STRING, value);
  }
}
