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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Reader functions for converting Arrow-based (Lance) values back to Iceberg typed Java objects.
 *
 * <p>Each reader converts a raw value from an Arrow vector into the appropriate Java type expected
 * by Iceberg's GenericRecord.
 */
public class LanceValueReaders {

  private LanceValueReaders() {}

  /** Functional interface for a single-value reader. */
  @FunctionalInterface
  public interface ValueReader<T> {
    /**
     * Read a value from the source.
     *
     * @param rawValue the raw value from the Arrow vector
     * @return the converted value
     */
    T read(Object rawValue);
  }

  /** Returns a reader for boolean values. */
  public static ValueReader<Boolean> booleans() {
    return rawValue -> {
      if (rawValue instanceof Boolean) {
        return (Boolean) rawValue;
      }
      throw new IllegalArgumentException("Expected Boolean, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for integer values. */
  public static ValueReader<Integer> ints() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        return ((Number) rawValue).intValue();
      }
      throw new IllegalArgumentException("Expected Integer, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for long values. */
  public static ValueReader<Long> longs() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        return ((Number) rawValue).longValue();
      }
      throw new IllegalArgumentException("Expected Long, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for float values. */
  public static ValueReader<Float> floats() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        return ((Number) rawValue).floatValue();
      }
      throw new IllegalArgumentException("Expected Float, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for double values. */
  public static ValueReader<Double> doubles() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        return ((Number) rawValue).doubleValue();
      }
      throw new IllegalArgumentException("Expected Double, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for string values. */
  public static ValueReader<String> strings() {
    return rawValue -> {
      if (rawValue instanceof CharSequence) {
        return rawValue.toString();
      }
      throw new IllegalArgumentException("Expected String, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for binary values. */
  public static ValueReader<ByteBuffer> byteBuffers() {
    return rawValue -> {
      if (rawValue instanceof byte[]) {
        return ByteBuffer.wrap((byte[]) rawValue);
      } else if (rawValue instanceof ByteBuffer) {
        return (ByteBuffer) rawValue;
      }
      throw new IllegalArgumentException("Expected byte[] or ByteBuffer, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for UUID values (from 16-byte fixed binary). */
  public static ValueReader<UUID> uuids() {
    return rawValue -> {
      ByteBuffer bb;
      if (rawValue instanceof byte[]) {
        bb = ByteBuffer.wrap((byte[]) rawValue);
      } else if (rawValue instanceof ByteBuffer) {
        bb = ((ByteBuffer) rawValue).duplicate();
      } else {
        throw new IllegalArgumentException("Expected byte[] or ByteBuffer for UUID, got: " + rawValue.getClass());
      }
      long msb = bb.getLong();
      long lsb = bb.getLong();
      return new UUID(msb, lsb);
    };
  }

  /** Returns a reader for date values (days since epoch -> LocalDate). */
  public static ValueReader<LocalDate> dates() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        return LocalDate.ofEpochDay(((Number) rawValue).longValue());
      }
      throw new IllegalArgumentException("Expected Number for date, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for time values (microseconds since midnight -> LocalTime). */
  public static ValueReader<LocalTime> times() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        long micros = ((Number) rawValue).longValue();
        return LocalTime.ofNanoOfDay(micros * 1000);
      }
      throw new IllegalArgumentException("Expected Number for time, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for timestamp without timezone (microseconds since epoch -> LocalDateTime). */
  public static ValueReader<LocalDateTime> timestamps() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        long micros = ((Number) rawValue).longValue();
        long epochSecond = Math.floorDiv(micros, 1_000_000L);
        long nanoAdj = Math.floorMod(micros, 1_000_000L) * 1000;
        return LocalDateTime.ofEpochSecond(epochSecond, (int) nanoAdj, ZoneOffset.UTC);
      }
      throw new IllegalArgumentException(
          "Expected Number for timestamp, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for timestamp with timezone (microseconds since epoch -> OffsetDateTime). */
  public static ValueReader<OffsetDateTime> timestampTzs() {
    return rawValue -> {
      if (rawValue instanceof Number) {
        long micros = ((Number) rawValue).longValue();
        long epochSecond = Math.floorDiv(micros, 1_000_000L);
        long nanoAdj = Math.floorMod(micros, 1_000_000L) * 1000;
        Instant instant = Instant.ofEpochSecond(epochSecond, nanoAdj);
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
      }
      throw new IllegalArgumentException(
          "Expected Number for timestamptz, got: " + rawValue.getClass());
    };
  }

  /** Returns a reader for decimal values. */
  public static ValueReader<BigDecimal> decimals() {
    return rawValue -> {
      if (rawValue instanceof BigDecimal) {
        return (BigDecimal) rawValue;
      } else if (rawValue instanceof Number) {
        return BigDecimal.valueOf(((Number) rawValue).doubleValue());
      }
      throw new IllegalArgumentException("Expected BigDecimal, got: " + rawValue.getClass());
    };
  }

  /**
   * Create a reader for a given Iceberg type.
   *
   * @param type the Iceberg type
   * @return a value reader for the type
   */
  @SuppressWarnings("unchecked")
  public static ValueReader<Object> forType(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return (ValueReader<Object>) (ValueReader<?>) booleans();
      case INTEGER:
        return (ValueReader<Object>) (ValueReader<?>) ints();
      case LONG:
        return (ValueReader<Object>) (ValueReader<?>) longs();
      case FLOAT:
        return (ValueReader<Object>) (ValueReader<?>) floats();
      case DOUBLE:
        return (ValueReader<Object>) (ValueReader<?>) doubles();
      case STRING:
        return (ValueReader<Object>) (ValueReader<?>) strings();
      case BINARY:
      case FIXED:
        return (ValueReader<Object>) (ValueReader<?>) byteBuffers();
      case UUID:
        return (ValueReader<Object>) (ValueReader<?>) uuids();
      case DATE:
        return (ValueReader<Object>) (ValueReader<?>) dates();
      case TIME:
        return (ValueReader<Object>) (ValueReader<?>) times();
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          return (ValueReader<Object>) (ValueReader<?>) timestampTzs();
        } else {
          return (ValueReader<Object>) (ValueReader<?>) timestamps();
        }
      case DECIMAL:
        return (ValueReader<Object>) (ValueReader<?>) decimals();
      default:
        throw new UnsupportedOperationException("Unsupported type for Lance reader: " + type);
    }
  }
}
