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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.util.DateTimeUtil;

public class Variants {
  private Variants() {}

  enum LogicalType {
    NULL,
    BOOLEAN,
    EXACT_NUMERIC,
    FLOAT,
    DOUBLE,
    DATE,
    TIMESTAMPTZ,
    TIMESTAMPNTZ,
    BINARY,
    STRING,
    ARRAY,
    OBJECT
  }

  public enum PhysicalType {
    NULL(LogicalType.NULL, Void.class),
    BOOLEAN_TRUE(LogicalType.BOOLEAN, Boolean.class),
    BOOLEAN_FALSE(LogicalType.BOOLEAN, Boolean.class),
    INT8(LogicalType.EXACT_NUMERIC, Byte.class),
    INT16(LogicalType.EXACT_NUMERIC, Short.class),
    INT32(LogicalType.EXACT_NUMERIC, Integer.class),
    INT64(LogicalType.EXACT_NUMERIC, Long.class),
    DOUBLE(LogicalType.DOUBLE, Double.class),
    DECIMAL4(LogicalType.EXACT_NUMERIC, BigDecimal.class),
    DECIMAL8(LogicalType.EXACT_NUMERIC, BigDecimal.class),
    DECIMAL16(LogicalType.EXACT_NUMERIC, BigDecimal.class),
    DATE(LogicalType.DATE, Integer.class),
    TIMESTAMPTZ(LogicalType.TIMESTAMPTZ, Long.class),
    TIMESTAMPNTZ(LogicalType.TIMESTAMPNTZ, Long.class),
    FLOAT(LogicalType.FLOAT, Float.class),
    BINARY(LogicalType.BINARY, ByteBuffer.class),
    STRING(LogicalType.STRING, String.class),
    ARRAY(LogicalType.ARRAY, List.class),
    OBJECT(LogicalType.OBJECT, Map.class);

    private final LogicalType logicalType;
    private final Class<?> javaClass;

    PhysicalType(LogicalType logicalType, Class<?> javaClass) {
      this.logicalType = logicalType;
      this.javaClass = javaClass;
    }

    LogicalType toLogicalType() {
      return logicalType;
    }

    public Class<?> javaClass() {
      return javaClass;
    }

    public static PhysicalType from(int primitiveType) {
      switch (primitiveType) {
        case Primitives.TYPE_NULL:
          return NULL;
        case Primitives.TYPE_TRUE:
          return BOOLEAN_TRUE;
        case Primitives.TYPE_FALSE:
          return BOOLEAN_FALSE;
        case Primitives.TYPE_INT8:
          return INT8;
        case Primitives.TYPE_INT16:
          return INT16;
        case Primitives.TYPE_INT32:
          return INT32;
        case Primitives.TYPE_INT64:
          return INT64;
        case Primitives.TYPE_DATE:
          return DATE;
        case Primitives.TYPE_TIMESTAMPTZ:
          return TIMESTAMPTZ;
        case Primitives.TYPE_TIMESTAMPNTZ:
          return TIMESTAMPNTZ;
        case Primitives.TYPE_FLOAT:
          return FLOAT;
        case Primitives.TYPE_DOUBLE:
          return DOUBLE;
        case Primitives.TYPE_DECIMAL4:
          return DECIMAL4;
        case Primitives.TYPE_DECIMAL8:
          return DECIMAL8;
        case Primitives.TYPE_DECIMAL16:
          return DECIMAL16;
        case Primitives.TYPE_BINARY:
          return BINARY;
        case Primitives.TYPE_STRING:
          return STRING;
      }

      throw new UnsupportedOperationException("Unknown primitive physical type: " + primitiveType);
    }
  }

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

  static class Primitives {
    static final int TYPE_NULL = 0;
    static final int TYPE_TRUE = 1;
    static final int TYPE_FALSE = 2;
    static final int TYPE_INT8 = 3;
    static final int TYPE_INT16 = 4;
    static final int TYPE_INT32 = 5;
    static final int TYPE_INT64 = 6;
    static final int TYPE_DOUBLE = 7;
    static final int TYPE_DECIMAL4 = 8;
    static final int TYPE_DECIMAL8 = 9;
    static final int TYPE_DECIMAL16 = 10;
    static final int TYPE_DATE = 11;
    static final int TYPE_TIMESTAMPTZ = 12; // equivalent to timestamptz
    static final int TYPE_TIMESTAMPNTZ = 13; // equivalent to timestamp
    static final int TYPE_FLOAT = 14;
    static final int TYPE_BINARY = 15;
    static final int TYPE_STRING = 16;

    static final int PRIMITIVE_TYPE_SHIFT = 2;

    private Primitives() {}
  }

  static final int HEADER_SIZE = 1;

  enum BasicType {
    PRIMITIVE,
    SHORT_STRING,
    OBJECT,
    ARRAY
  }

  public static VariantValue from(ByteBuffer metadata, ByteBuffer value) {
    return from(SerializedMetadata.from(metadata), value);
  }

  static VariantValue from(SerializedMetadata metadata, ByteBuffer value) {
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

  static VariantPrimitive<Void> ofNull() {
    return new PrimitiveWrapper<>(PhysicalType.NULL, null);
  }

  static VariantPrimitive<Boolean> of(boolean value) {
    if (value) {
      return new PrimitiveWrapper<>(PhysicalType.BOOLEAN_TRUE, true);
    } else {
      return new PrimitiveWrapper<>(PhysicalType.BOOLEAN_FALSE, false);
    }
  }

  static VariantPrimitive<Byte> of(byte value) {
    return new PrimitiveWrapper<>(PhysicalType.INT8, value);
  }

  static VariantPrimitive<Short> of(short value) {
    return new PrimitiveWrapper<>(PhysicalType.INT16, value);
  }

  static VariantPrimitive<Integer> of(int value) {
    return new PrimitiveWrapper<>(PhysicalType.INT32, value);
  }

  static VariantPrimitive<Long> of(long value) {
    return new PrimitiveWrapper<>(PhysicalType.INT64, value);
  }

  static VariantPrimitive<Float> of(float value) {
    return new PrimitiveWrapper<>(PhysicalType.FLOAT, value);
  }

  static VariantPrimitive<Double> of(double value) {
    return new PrimitiveWrapper<>(PhysicalType.DOUBLE, value);
  }

  static VariantPrimitive<Integer> ofDate(int value) {
    return new PrimitiveWrapper<>(PhysicalType.DATE, value);
  }

  static VariantPrimitive<Integer> ofIsoDate(String value) {
    return ofDate(DateTimeUtil.isoDateToDays(value));
  }

  static VariantPrimitive<Long> ofTimestamptz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPTZ, value);
  }

  static VariantPrimitive<Long> ofIsoTimestamptz(String value) {
    return ofTimestamptz(DateTimeUtil.isoTimestamptzToMicros(value));
  }

  static VariantPrimitive<Long> ofTimestampntz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPNTZ, value);
  }

  static VariantPrimitive<Long> ofIsoTimestampntz(String value) {
    return ofTimestampntz(DateTimeUtil.isoTimestampToMicros(value));
  }

  static VariantPrimitive<BigDecimal> of(BigDecimal value) {
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

  static VariantPrimitive<ByteBuffer> of(ByteBuffer value) {
    return new PrimitiveWrapper<>(PhysicalType.BINARY, value);
  }

  static VariantPrimitive<String> of(String value) {
    return new PrimitiveWrapper<>(PhysicalType.STRING, value);
  }
}
