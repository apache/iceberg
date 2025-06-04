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
package org.apache.iceberg.expressions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantValue;

class VariantExpressionUtil {
  private static final Map<Type, PhysicalType> NO_CONVERSION_NEEDED =
      ImmutableMap.<Type, PhysicalType>builder()
          .put(Types.IntegerType.get(), PhysicalType.INT32)
          .put(Types.LongType.get(), PhysicalType.INT64)
          .put(Types.FloatType.get(), PhysicalType.FLOAT)
          .put(Types.DoubleType.get(), PhysicalType.DOUBLE)
          .put(Types.DateType.get(), PhysicalType.DATE)
          .put(Types.TimestampType.withoutZone(), PhysicalType.TIMESTAMPNTZ)
          .put(Types.TimestampType.withZone(), PhysicalType.TIMESTAMPTZ)
          .put(Types.TimestampNanoType.withoutZone(), PhysicalType.TIMESTAMPNTZ_NANOS)
          .put(Types.TimestampNanoType.withZone(), PhysicalType.TIMESTAMPTZ_NANOS)
          .put(Types.TimeType.get(), PhysicalType.TIME)
          .put(Types.UUIDType.get(), PhysicalType.UUID)
          .put(Types.StringType.get(), PhysicalType.STRING)
          .put(Types.BinaryType.get(), PhysicalType.BINARY)
          .put(Types.UnknownType.get(), PhysicalType.NULL)
          .build();

  // return (T) (Integer) ((Number) value.asPrimitive().get()).intValue();

  private static final Map<Type, List<PhysicalType>> INT_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.IntegerType.get(),
              ImmutableList.<PhysicalType>builder()
                  .add(PhysicalType.INT8)
                  .add(PhysicalType.INT16)
                  .build())
          .build();

  private static final Map<Type, List<PhysicalType>> LONG_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.LongType.get(),
              ImmutableList.<PhysicalType>builder()
                  .add(PhysicalType.INT8)
                  .add(PhysicalType.INT16)
                  .add(PhysicalType.INT32)
                  .build())
          .put(
              Types.TimeType.get(),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.INT64).build())
          .put(
              Types.TimestampType.withZone(),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.INT64).build())
          .put(
              Types.TimestampNanoType.withZone(),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.INT64).build())
          .build();

  private static final Map<Type, List<PhysicalType>> DOUBLE_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.DoubleType.get(),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.FLOAT).build())
          .build();

  private static final Map<Type, List<PhysicalType>> DECIMAL_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.DecimalType.of(9, 0),
              ImmutableList.<PhysicalType>builder()
                  .add(PhysicalType.DECIMAL4)
                  .add(PhysicalType.DECIMAL8)
                  .add(PhysicalType.DECIMAL16)
                  .build())
          .build();

  private static final Map<Type, List<PhysicalType>> FIXED_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.FixedType.ofLength(1),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.BINARY).build())
          .build();

  private static final Map<Type, Map<PhysicalType, Boolean>> BOOLEAN_CONVERSION_NEEDED =
      ImmutableMap.<Type, Map<PhysicalType, Boolean>>builder()
          .put(
              Types.BooleanType.get(),
              ImmutableMap.<PhysicalType, Boolean>builder()
                  .put(PhysicalType.BOOLEAN_TRUE, Boolean.TRUE)
                  .put(PhysicalType.BOOLEAN_FALSE, Boolean.FALSE)
                  .build())
          .build();

  private static final Map<Type, List<PhysicalType>> UUID_CONVERSION_NEEDED =
      ImmutableMap.<Type, List<PhysicalType>>builder()
          .put(
              Types.UUIDType.get(),
              ImmutableList.<PhysicalType>builder().add(PhysicalType.STRING).build())
          .build();

  private VariantExpressionUtil() {}

  @SuppressWarnings("unchecked")
  static <T> T castTo(VariantValue value, Type type) {
    if (value == null) {
      return null;
    } else if (NO_CONVERSION_NEEDED.get(type) == value.type()) {
      return (T) value.asPrimitive().get();
    }
    if (INT_CONVERSION_NEEDED.get(type).contains(value.type())) {
      return (T) (Integer) ((Number) value.asPrimitive().get()).intValue();
    } else if (LONG_CONVERSION_NEEDED.get(type).contains(value.type())) {
      return (T) (Long) ((Number) value.asPrimitive().get()).longValue();
    } else if (DOUBLE_CONVERSION_NEEDED.get(type).contains(value.type())) {
      return (T) (Double) ((Number) value.asPrimitive().get()).doubleValue();
    } else if (DECIMAL_CONVERSION_NEEDED.get(type).contains(value.type())) {
      Types.DecimalType decimalType = (Types.DecimalType) type;
      BigDecimal decimalValue = (BigDecimal) value.asPrimitive().get();
      if (decimalValue.scale() == decimalType.scale()) {
        return (T) decimalValue;
      }
    } else if (FIXED_CONVERSION_NEEDED.get(type).contains(value.type())) {
      Types.FixedType fixedType = (Types.FixedType) type;
      ByteBuffer buffer = (ByteBuffer) value.asPrimitive().get();
      if (buffer.remaining() == fixedType.length()) {
        return (T) buffer;
      }
    } else if (UUID_CONVERSION_NEEDED.get(type).contains(value.type())) {
      return (T) UUID.fromString((String) value.asPrimitive().get());
    } else if (BOOLEAN_CONVERSION_NEEDED.get(type).containsKey(value.type())) {
      return (T) BOOLEAN_CONVERSION_NEEDED.get(type).get(value.type());
    }
    return null;
  }
}
