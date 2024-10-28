/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class Variants {
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
    INT8(LogicalType.EXACT_NUMERIC, Integer.class),
    INT16(LogicalType.EXACT_NUMERIC, Integer.class),
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

  public interface Serialized {
    ByteBuffer buffer();
  }

  public interface Metadata extends Serialized {
    int id(String name);

    String get(int id);
  }

  public interface Value {
    PhysicalType type();
  }

  public interface Primitive<T> extends Value {
    T get();
  }

  public interface Object extends Value {
    Value get(String field);

    default PhysicalType type() {
      return PhysicalType.OBJECT;
    }
  }

  public interface Array extends Value {
    Value get(int index);

    default PhysicalType type() {
      return PhysicalType.ARRAY;
    }
  }

  static class Primitives {
    private static final int TYPE_NULL = 0;
    private static final int TYPE_TRUE = 1;
    private static final int TYPE_FALSE = 2;
    private static final int TYPE_INT8 = 3;
    private static final int TYPE_INT16 = 4;
    private static final int TYPE_INT32 = 5;
    private static final int TYPE_INT64 = 6;
    private static final int TYPE_DOUBLE = 7;
    private static final int TYPE_DECIMAL4 = 8;
    private static final int TYPE_DECIMAL8 = 9;
    private static final int TYPE_DECIMAL16 = 10;
    private static final int TYPE_DATE = 11;
    private static final int TYPE_TIMESTAMPTZ = 12; // equivalent to timestamptz
    private static final int TYPE_TIMESTAMPNTZ = 13; // equivalent to timestamp
    private static final int TYPE_FLOAT = 14;
    private static final int TYPE_BINARY = 15;
    private static final int TYPE_STRING = 16;

    private Primitives() {}
  }

  static final int HEADER_SIZE = 1;
  static final int BASIC_TYPE_MASK = 0b11;
  static final int BASIC_TYPE_PRIMITIVE = 0;
  static final int BASIC_TYPE_SHORT_STRING = 1;
  static final int BASIC_TYPE_OBJECT = 2;
  static final int BASIC_TYPE_ARRAY = 3;

  public static Value from(ByteBuffer metadata, ByteBuffer value) {
    return from(VariantMetadata.from(metadata), value);
  }

  static Value from(VariantMetadata metadata, ByteBuffer value) {
    int header = VariantUtil.readByte(value, 0);
    int basicType = header & BASIC_TYPE_MASK;
    switch (basicType) {
      case BASIC_TYPE_PRIMITIVE:
        return VariantPrimitive.from(value, header);
      case BASIC_TYPE_SHORT_STRING:
        return VariantShortString.from(value, header);
      case BASIC_TYPE_OBJECT:
        return VariantObject.from(metadata, value, header);
      case BASIC_TYPE_ARRAY:
        return VariantArray.from(metadata, value, header);
      default:
        throw new UnsupportedOperationException("Unsupported basic type: %s" + basicType);
    }
  }
}
