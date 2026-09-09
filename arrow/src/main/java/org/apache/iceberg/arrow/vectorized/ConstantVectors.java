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
package org.apache.iceberg.arrow.vectorized;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.IntConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.UUIDUtil;

/**
 * Builds an Arrow vector whose rows all hold the same value.
 *
 * <p>A column that is missing from a data file, either because it was added by a schema change or
 * because it has a default value, is read as a constant instead of from the file. Spark represents
 * such a column with a virtual vector that never allocates memory, but the Arrow reader hands out
 * {@link org.apache.arrow.vector.VectorSchemaRoot}, so it needs a real vector holding the value
 * repeated once per row.
 */
class ConstantVectors {

  private ConstantVectors() {}

  /**
   * Creates a holder for a vector of {@code numRows} rows that all hold {@code constant}.
   *
   * @param field the Iceberg field the vector is read for, used to derive the Arrow type
   * @param constant the value every row holds, or null to produce an all-null vector
   * @param numRows the number of rows in the vector
   * @param allocator the allocator that owns the vector's memory
   * @return a holder whose vector the caller is responsible for closing
   */
  static VectorHolder holder(
      Types.NestedField field, Object constant, int numRows, BufferAllocator allocator) {
    FieldVector vector = createVector(field, constant, numRows, allocator);

    NullabilityHolder nullabilityHolder = new NullabilityHolder(numRows);
    if (constant == null) {
      nullabilityHolder.setNulls(0, numRows);
    } else {
      nullabilityHolder.setNotNulls(0, numRows);
    }

    return VectorHolder.vectorHolder(vector, field, nullabilityHolder);
  }

  private static FieldVector createVector(
      Types.NestedField field, Object constant, int numRows, BufferAllocator allocator) {
    FieldVector vector = ArrowSchemaUtil.convert(field).createVector(allocator);
    try {
      vector.setInitialCapacity(numRows);
      vector.allocateNew();

      if (constant != null) {
        IntConsumer setter = setter(vector, field.type(), constant);
        for (int row = 0; row < numRows; row += 1) {
          setter.accept(row);
        }
      }

      // rows that were never set keep the validity bit allocateNew cleared, so they read as null
      vector.setValueCount(numRows);
      return vector;
    } catch (RuntimeException e) {
      vector.close();
      throw e;
    }
  }

  private static IntConsumer setter(FieldVector vector, Type type, Object constant) {
    switch (type.typeId()) {
      case BOOLEAN:
        {
          BitVector bits = (BitVector) vector;
          int bit = (Boolean) constant ? 1 : 0;
          return row -> bits.setSafe(row, bit);
        }
      case INTEGER:
        {
          IntVector ints = (IntVector) vector;
          int intValue = (Integer) constant;
          return row -> ints.setSafe(row, intValue);
        }
      case LONG:
        {
          BigIntVector longs = (BigIntVector) vector;
          long longValue = (Long) constant;
          return row -> longs.setSafe(row, longValue);
        }
      case FLOAT:
        {
          Float4Vector floats = (Float4Vector) vector;
          float floatValue = (Float) constant;
          return row -> floats.setSafe(row, floatValue);
        }
      case DOUBLE:
        {
          Float8Vector doubles = (Float8Vector) vector;
          double doubleValue = (Double) constant;
          return row -> doubles.setSafe(row, doubleValue);
        }
      case DATE:
        {
          DateDayVector days = (DateDayVector) vector;
          int dayValue = (Integer) constant;
          return row -> days.setSafe(row, dayValue);
        }
      case TIME:
        {
          TimeMicroVector times = (TimeMicroVector) vector;
          long timeValue = (Long) constant;
          return row -> times.setSafe(row, timeValue);
        }
      case TIMESTAMP:
        {
          long micros = (Long) constant;
          if (vector instanceof TimeStampMicroTZVector) {
            TimeStampMicroTZVector timestamps = (TimeStampMicroTZVector) vector;
            return row -> timestamps.setSafe(row, micros);
          } else {
            TimeStampMicroVector timestamps = (TimeStampMicroVector) vector;
            return row -> timestamps.setSafe(row, micros);
          }
        }
      case TIMESTAMP_NANO:
        {
          long nanos = (Long) constant;
          if (vector instanceof TimeStampNanoTZVector) {
            TimeStampNanoTZVector timestamps = (TimeStampNanoTZVector) vector;
            return row -> timestamps.setSafe(row, nanos);
          } else {
            TimeStampNanoVector timestamps = (TimeStampNanoVector) vector;
            return row -> timestamps.setSafe(row, nanos);
          }
        }
      case STRING:
        {
          VarCharVector strings = (VarCharVector) vector;
          byte[] utf8 = constant.toString().getBytes(StandardCharsets.UTF_8);
          return row -> strings.setSafe(row, utf8);
        }
      case UUID:
        {
          FixedSizeBinaryVector uuids = (FixedSizeBinaryVector) vector;
          byte[] uuidBytes = UUIDUtil.convert((UUID) constant);
          return row -> uuids.setSafe(row, uuidBytes);
        }
      case FIXED:
        {
          FixedSizeBinaryVector fixed = (FixedSizeBinaryVector) vector;
          byte[] fixedBytes = toByteArray(constant);
          return row -> fixed.setSafe(row, fixedBytes);
        }
      case BINARY:
        {
          VarBinaryVector binary = (VarBinaryVector) vector;
          byte[] bytes = toByteArray(constant);
          return row -> binary.setSafe(row, bytes);
        }
      case DECIMAL:
        {
          DecimalVector decimals = (DecimalVector) vector;
          BigDecimal decimal = (BigDecimal) constant;
          return row -> decimals.setSafe(row, decimal);
        }
      default:
        throw new UnsupportedOperationException("Unsupported constant type: " + type);
    }
  }

  private static byte[] toByteArray(Object constant) {
    if (constant instanceof ByteBuffer) {
      return ByteBuffers.toByteArray((ByteBuffer) constant);
    }
    return (byte[]) constant;
  }
}
