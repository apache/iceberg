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
  TIME(LogicalType.TIME, Long.class),
  TIMESTAMPTZ_NANOS(LogicalType.TIMESTAMPTZ, Long.class),
  TIMESTAMPNTZ_NANOS(LogicalType.TIMESTAMPNTZ, Long.class),
  UUID(LogicalType.UUID, String.class),
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
    return switch (primitiveType) {
      case Primitives.TYPE_NULL -> NULL;
      case Primitives.TYPE_TRUE -> BOOLEAN_TRUE;
      case Primitives.TYPE_FALSE -> BOOLEAN_FALSE;
      case Primitives.TYPE_INT8 -> INT8;
      case Primitives.TYPE_INT16 -> INT16;
      case Primitives.TYPE_INT32 -> INT32;
      case Primitives.TYPE_INT64 -> INT64;
      case Primitives.TYPE_DATE -> DATE;
      case Primitives.TYPE_TIMESTAMPTZ -> TIMESTAMPTZ;
      case Primitives.TYPE_TIMESTAMPNTZ -> TIMESTAMPNTZ;
      case Primitives.TYPE_FLOAT -> FLOAT;
      case Primitives.TYPE_DOUBLE -> DOUBLE;
      case Primitives.TYPE_DECIMAL4 -> DECIMAL4;
      case Primitives.TYPE_DECIMAL8 -> DECIMAL8;
      case Primitives.TYPE_DECIMAL16 -> DECIMAL16;
      case Primitives.TYPE_BINARY -> BINARY;
      case Primitives.TYPE_STRING -> STRING;
      case Primitives.TYPE_TIME -> TIME;
      case Primitives.TYPE_TIMESTAMPTZ_NANOS -> TIMESTAMPTZ_NANOS;
      case Primitives.TYPE_TIMESTAMPNTZ_NANOS -> TIMESTAMPNTZ_NANOS;
      case Primitives.TYPE_UUID -> UUID;
      default ->
          throw new UnsupportedOperationException(
              "Unknown primitive physical type: " + primitiveType);
    };
  }
}
