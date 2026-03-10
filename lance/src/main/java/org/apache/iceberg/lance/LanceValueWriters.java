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
package org.apache.iceberg.lance;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Writer functions for converting Iceberg data values to intermediate representations suitable for
 * Lance (Arrow-based) output.
 *
 * <p>Each writer converts an Iceberg typed value into a Java object suitable for serialization into
 * Arrow vectors. In a production implementation these would write directly to Arrow ValueVectors.
 */
public class LanceValueWriters {

  private LanceValueWriters() {}

  /** Functional interface for a single-value writer. */
  @FunctionalInterface
  public interface ValueWriter<T> {
    /**
     * Write the given value to the output at the specified index.
     *
     * @param value the value to write
     * @param index the row index
     * @param output the output buffer (e.g., an Arrow vector or list buffer)
     */
    void write(T value, int index, Object output);
  }

  /** Returns a writer for boolean values. */
  public static ValueWriter<Boolean> booleans() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Boolean value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /** Returns a writer for integer values. */
  public static ValueWriter<Integer> ints() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Integer value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /** Returns a writer for long values. */
  public static ValueWriter<Long> longs() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Long value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /** Returns a writer for float values. */
  public static ValueWriter<Float> floats() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Float value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /** Returns a writer for double values. */
  public static ValueWriter<Double> doubles() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Double value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /** Returns a writer for string values. */
  public static ValueWriter<CharSequence> strings() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "String value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value.toString());
    };
  }

  /** Returns a writer for binary values (ByteBuffer). */
  public static ValueWriter<ByteBuffer> byteBuffers() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Binary value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      byte[] bytes = new byte[value.remaining()];
      value.duplicate().get(bytes);
      buffer.add(bytes);
    };
  }

  /** Returns a writer for UUID values. */
  public static ValueWriter<UUID> uuids() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "UUID value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(value.getMostSignificantBits());
      bb.putLong(value.getLeastSignificantBits());
      buffer.add(bb.array());
    };
  }

  /** Returns a writer for date values (days since epoch). */
  public static ValueWriter<LocalDate> dates() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Date value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add((int) value.toEpochDay());
    };
  }

  /** Returns a writer for time values (microseconds since midnight). */
  public static ValueWriter<LocalTime> times() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Time value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value.toNanoOfDay() / 1000);
    };
  }

  /** Returns a writer for timestamp without timezone (microseconds since epoch). */
  public static ValueWriter<LocalDateTime> timestamps() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Timestamp value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      long epochMicros = ChronoUnit.MICROS.between(LocalDateTime.of(1970, 1, 1, 0, 0), value);
      buffer.add(epochMicros);
    };
  }

  /** Returns a writer for timestamp with timezone (microseconds since epoch UTC). */
  public static ValueWriter<OffsetDateTime> timestampTzs() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "TimestampTz value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      long epochMicros =
          ChronoUnit.MICROS.between(
              OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), value);
      buffer.add(epochMicros);
    };
  }

  /** Returns a writer for decimal values. */
  public static ValueWriter<BigDecimal> decimals() {
    return (value, index, output) -> {
      Preconditions.checkNotNull(value, "Decimal value cannot be null at index %s", index);
      @SuppressWarnings("unchecked")
      List<Object> buffer = (List<Object>) output;
      buffer.add(value);
    };
  }

  /**
   * Create a writer for a given Iceberg type.
   *
   * @param type the Iceberg type
   * @return a value writer for the type
   */
  @SuppressWarnings("unchecked")
  public static ValueWriter<Object> forType(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return (ValueWriter<Object>) (ValueWriter<?>) booleans();
      case INTEGER:
        return (ValueWriter<Object>) (ValueWriter<?>) ints();
      case LONG:
        return (ValueWriter<Object>) (ValueWriter<?>) longs();
      case FLOAT:
        return (ValueWriter<Object>) (ValueWriter<?>) floats();
      case DOUBLE:
        return (ValueWriter<Object>) (ValueWriter<?>) doubles();
      case STRING:
        return (ValueWriter<Object>) (ValueWriter<?>) strings();
      case BINARY:
      case FIXED:
        return (ValueWriter<Object>) (ValueWriter<?>) byteBuffers();
      case UUID:
        return (ValueWriter<Object>) (ValueWriter<?>) uuids();
      case DATE:
        return (ValueWriter<Object>) (ValueWriter<?>) dates();
      case TIME:
        return (ValueWriter<Object>) (ValueWriter<?>) times();
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          return (ValueWriter<Object>) (ValueWriter<?>) timestampTzs();
        } else {
          return (ValueWriter<Object>) (ValueWriter<?>) timestamps();
        }
      case DECIMAL:
        return (ValueWriter<Object>) (ValueWriter<?>) decimals();
      default:
        throw new UnsupportedOperationException("Unsupported type for Lance writer: " + type);
    }
  }

  /**
   * Convert a GenericRecord to a simple Object array suitable for testing.
   *
   * @param record the record to convert
   * @return an array of values
   */
  public static Object[] recordToArray(GenericRecord record) {
    int size = record.struct().fields().size();
    Object[] result = new Object[size];
    for (int i = 0; i < size; i++) {
      result[i] = record.get(i);
    }
    return result;
  }
}
