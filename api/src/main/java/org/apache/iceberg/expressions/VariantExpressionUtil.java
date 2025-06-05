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
import java.util.Map;
import java.util.UUID;
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

  private VariantExpressionUtil() {}

  // Todo: Fix cyclomatic complexity checkstyle error using appropriate casting functions
  @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
  static <T> T castTo(VariantValue value, Type type) {
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
      case TIMESTAMP:
      case TIMESTAMP_NANO:
      case TIME:
        if (value.type() == PhysicalType.INT64) {
          return (T) (Long) ((Number) value.asPrimitive().get()).longValue();
        }
        break;
      case UUID:
        if (value.type() == PhysicalType.STRING) {
          return (T) UUID.fromString((String) value.asPrimitive().get());
        }
    }
    return null;
  }
}
