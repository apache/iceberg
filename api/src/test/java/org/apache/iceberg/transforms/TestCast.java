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
package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestCast {

  // ========== Boolean Casting Tests ==========

  @Test
  public void testBooleanToString() {
    Transform<Boolean, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Boolean, String> func = cast.bind(Types.BooleanType.get());

    assertThat(func.apply(true)).isEqualTo("true");
    assertThat(func.apply(false)).isEqualTo("false");
    assertThat(func.apply(null)).isNull();
  }

  @Test
  public void testBooleanCannotCastToNumeric() {
    Transform<Boolean, Integer> cast = Transforms.cast(Types.IntegerType.get());
    assertThat(cast.canTransform(Types.BooleanType.get())).isFalse();
  }

  // ========== Integer Casting Tests ==========

  @Test
  public void testIntegerToLong() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Integer, Long> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo(42L);
    assertThat(func.apply(Integer.MAX_VALUE)).isEqualTo((long) Integer.MAX_VALUE);
    assertThat(func.apply(Integer.MIN_VALUE)).isEqualTo((long) Integer.MIN_VALUE);
    assertThat(func.apply(null)).isNull();
  }

  @Test
  public void testIntegerToFloat() {
    Transform<Integer, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<Integer, Float> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo(42.0f);
    assertThat(func.apply(0)).isEqualTo(0.0f);
    assertThat(func.apply(-100)).isEqualTo(-100.0f);
  }

  @Test
  public void testIntegerToDouble() {
    Transform<Integer, Double> cast = Transforms.cast(Types.DoubleType.get());
    SerializableFunction<Integer, Double> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo(42.0);
    assertThat(func.apply(Integer.MAX_VALUE)).isEqualTo((double) Integer.MAX_VALUE);
  }

  @Test
  public void testIntegerToDate() {
    Transform<Integer, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<Integer, Integer> func = cast.bind(Types.IntegerType.get());

    // Integer days since epoch can be used as date
    assertThat(func.apply(17532)).isEqualTo(17532); // 2018-01-01
    assertThat(func.apply(0)).isEqualTo(0); // 1970-01-01
  }

  @Test
  public void testIntegerToDecimal() {
    Transform<Integer, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(10, 2));
    SerializableFunction<Integer, BigDecimal> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo(new BigDecimal("42.00"));
    assertThat(func.apply(100)).isEqualTo(new BigDecimal("100.00"));
  }

  @Test
  public void testIntegerToString() {
    Transform<Integer, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Integer, String> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo("42");
    assertThat(func.apply(-100)).isEqualTo("-100");
    assertThat(func.apply(0)).isEqualTo("0");
  }

  // ========== Long Casting Tests ==========

  @Test
  public void testLongToInteger() {
    Transform<Long, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(42L)).isEqualTo(42);
    assertThat(func.apply(100L)).isEqualTo(100);
    assertThat(func.apply(null)).isNull();
  }

  @Test
  public void testLongToIntegerOverflow() {
    Transform<Long, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.LongType.get());

    // Overflow cases should return null
    assertThat(func.apply(Long.MAX_VALUE)).isNull();
    assertThat(func.apply(Long.MIN_VALUE)).isNull();
    assertThat(func.apply((long) Integer.MAX_VALUE + 1)).isNull();
  }

  @Test
  public void testLongToFloat() {
    Transform<Long, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<Long, Float> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(42L)).isEqualTo(42.0f);
    assertThat(func.apply(1000L)).isEqualTo(1000.0f);
  }

  @Test
  public void testLongToDouble() {
    Transform<Long, Double> cast = Transforms.cast(Types.DoubleType.get());
    SerializableFunction<Long, Double> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(42L)).isEqualTo(42.0);
    assertThat(func.apply(Long.MAX_VALUE)).isEqualTo((double) Long.MAX_VALUE);
  }

  @Test
  public void testLongToTime() {
    Transform<Long, Long> cast = Transforms.cast(Types.TimeType.get());
    SerializableFunction<Long, Long> func = cast.bind(Types.LongType.get());

    // Long microseconds can be used as time
    long timeMicros = 36775038194L; // 10:12:55.038194
    assertThat(func.apply(timeMicros)).isEqualTo(timeMicros);
  }

  @Test
  public void testLongToTimestamp() {
    Transform<Long, Long> cast = Transforms.cast(Types.TimestampType.withoutZone());
    SerializableFunction<Long, Long> func = cast.bind(Types.LongType.get());

    // Long microseconds can be used as timestamp
    long timestampMicros = 1514764800000000L; // 2018-01-01T00:00:00
    assertThat(func.apply(timestampMicros)).isEqualTo(timestampMicros);
  }

  @Test
  public void testLongToTimestampNano() {
    Transform<Long, Long> cast = Transforms.cast(Types.TimestampNanoType.withoutZone());
    SerializableFunction<Long, Long> func = cast.bind(Types.LongType.get());

    long timestampMicros = 1514764800000000L; // 2018-01-01T00:00:00 in micros
    long timestampNanos = timestampMicros * 1000L; // Convert to nanos
    assertThat(func.apply(timestampMicros)).isEqualTo(timestampNanos);
  }

  @Test
  public void testLongToDate() {
    Transform<Long, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(17532L)).isEqualTo(17532); // 2018-01-01
  }

  @Test
  public void testLongToDateOverflow() {
    Transform<Long, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(Long.MAX_VALUE)).isNull();
    assertThat(func.apply((long) Integer.MAX_VALUE + 1)).isNull();
  }

  @Test
  public void testLongToDecimal() {
    Transform<Long, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(20, 2));
    SerializableFunction<Long, BigDecimal> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(42L)).isEqualTo(new BigDecimal("42.00"));
    assertThat(func.apply(100000L)).isEqualTo(new BigDecimal("100000.00"));
  }

  @Test
  public void testLongToString() {
    Transform<Long, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Long, String> func = cast.bind(Types.LongType.get());

    assertThat(func.apply(42L)).isEqualTo("42");
    assertThat(func.apply(Long.MAX_VALUE)).isEqualTo(String.valueOf(Long.MAX_VALUE));
  }

  // ========== Float Casting Tests ==========

  @Test
  public void testFloatToDouble() {
    Transform<Float, Double> cast = Transforms.cast(Types.DoubleType.get());
    SerializableFunction<Float, Double> func = cast.bind(Types.FloatType.get());

    assertThat(func.apply(3.14f)).isEqualTo((double) 3.14f);
    assertThat(func.apply(Float.MAX_VALUE)).isEqualTo((double) Float.MAX_VALUE);
  }

  @Test
  public void testFloatToDecimal() {
    Transform<Float, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(10, 2));
    SerializableFunction<Float, BigDecimal> func = cast.bind(Types.FloatType.get());

    assertThat(func.apply(3.14f)).isEqualTo(new BigDecimal("3.14"));
    assertThat(func.apply(100.5f)).isEqualTo(new BigDecimal("100.50"));
  }

  @Test
  public void testFloatToString() {
    Transform<Float, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Float, String> func = cast.bind(Types.FloatType.get());

    assertThat(func.apply(3.14f)).isEqualTo("3.14");
    assertThat(func.apply(-100.5f)).isEqualTo("-100.5");
  }

  // ========== Double Casting Tests ==========

  @Test
  public void testDoubleToFloat() {
    Transform<Double, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<Double, Float> func = cast.bind(Types.DoubleType.get());

    assertThat(func.apply(3.14)).isEqualTo(3.14f);
    assertThat(func.apply(100.5)).isEqualTo(100.5f);
  }

  @Test
  public void testDoubleToFloatOverflow() {
    Transform<Double, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<Double, Float> func = cast.bind(Types.DoubleType.get());

    // Double too large for float should return null
    assertThat(func.apply(Double.MAX_VALUE)).isNull();
    assertThat(func.apply(-Double.MAX_VALUE)).isNull();
  }

  @Test
  public void testDoubleToDecimal() {
    Transform<Double, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(10, 2));
    SerializableFunction<Double, BigDecimal> func = cast.bind(Types.DoubleType.get());

    assertThat(func.apply(3.14)).isEqualTo(new BigDecimal("3.14"));
    assertThat(func.apply(100.5)).isEqualTo(new BigDecimal("100.50"));
  }

  @Test
  public void testDoubleToString() {
    Transform<Double, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Double, String> func = cast.bind(Types.DoubleType.get());

    assertThat(func.apply(3.14)).isEqualTo("3.14");
    assertThat(func.apply(-100.5)).isEqualTo("-100.5");
  }

  // ========== Decimal Casting Tests ==========

  @Test
  public void testDecimalToInteger() {
    Transform<BigDecimal, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<BigDecimal, Integer> func = cast.bind(Types.DecimalType.of(10, 2));

    assertThat(func.apply(new BigDecimal("42.99"))).isEqualTo(42);
    assertThat(func.apply(new BigDecimal("100.01"))).isEqualTo(100);
    assertThat(func.apply(new BigDecimal("-50.5"))).isEqualTo(-50);
  }

  @Test
  public void testDecimalToLong() {
    Transform<BigDecimal, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<BigDecimal, Long> func = cast.bind(Types.DecimalType.of(20, 2));

    assertThat(func.apply(new BigDecimal("12345.67"))).isEqualTo(12345L);
    assertThat(func.apply(new BigDecimal("999999999999.99"))).isEqualTo(999999999999L);
  }

  @Test
  public void testDecimalToFloat() {
    Transform<BigDecimal, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<BigDecimal, Float> func = cast.bind(Types.DecimalType.of(10, 2));

    assertThat(func.apply(new BigDecimal("3.14"))).isEqualTo(3.14f);
    assertThat(func.apply(new BigDecimal("100.5"))).isEqualTo(100.5f);
  }

  @Test
  public void testDecimalToDouble() {
    Transform<BigDecimal, Double> cast = Transforms.cast(Types.DoubleType.get());
    SerializableFunction<BigDecimal, Double> func = cast.bind(Types.DecimalType.of(10, 2));

    assertThat(func.apply(new BigDecimal("3.14"))).isEqualTo(3.14);
    assertThat(func.apply(new BigDecimal("12345.67"))).isEqualTo(12345.67);
  }

  @Test
  public void testDecimalToDecimalDifferentScale() {
    Transform<BigDecimal, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(10, 4));
    SerializableFunction<BigDecimal, BigDecimal> func = cast.bind(Types.DecimalType.of(10, 2));

    assertThat(func.apply(new BigDecimal("42.99"))).isEqualTo(new BigDecimal("42.9900"));
    assertThat(func.apply(new BigDecimal("100.01"))).isEqualTo(new BigDecimal("100.0100"));
  }

  @Test
  public void testDecimalToString() {
    Transform<BigDecimal, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<BigDecimal, String> func = cast.bind(Types.DecimalType.of(10, 2));

    assertThat(func.apply(new BigDecimal("42.99"))).isEqualTo("42.99");
    assertThat(func.apply(new BigDecimal("100.00"))).isEqualTo("100.00");
  }

  // ========== Date Casting Tests ==========

  @Test
  public void testDateToString() {
    Transform<Integer, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Integer, String> func = cast.bind(Types.DateType.get());

    int days = DateTimeUtil.isoDateToDays("2018-01-01");
    assertThat(func.apply(days)).isEqualTo("2018-01-01");

    int epochDay = 0; // 1970-01-01
    assertThat(func.apply(epochDay)).isEqualTo("1970-01-01");
  }

  @Test
  public void testDateToTimestamp() {
    Transform<Integer, Long> cast = Transforms.cast(Types.TimestampType.withoutZone());
    SerializableFunction<Integer, Long> func = cast.bind(Types.DateType.get());

    int days = 1; // 1970-01-02
    long expectedMicros = 1L * 86400L * 1_000_000L;
    assertThat(func.apply(days)).isEqualTo(expectedMicros);
  }

  @Test
  public void testDateToTimestampNano() {
    Transform<Integer, Long> cast = Transforms.cast(Types.TimestampNanoType.withoutZone());
    SerializableFunction<Integer, Long> func = cast.bind(Types.DateType.get());

    int days = 1; // 1970-01-02
    long expectedNanos = 1L * 86400L * 1_000_000_000L;
    assertThat(func.apply(days)).isEqualTo(expectedNanos);
  }

  // ========== Time Casting Tests ==========

  @Test
  public void testTimeToString() {
    Transform<Long, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Long, String> func = cast.bind(Types.TimeType.get());

    long micros = DateTimeUtil.isoTimeToMicros("10:12:55.038194");
    assertThat(func.apply(micros)).isEqualTo("10:12:55.038194");
  }

  @Test
  public void testTimeToLong() {
    Transform<Long, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Long, Long> func = cast.bind(Types.TimeType.get());

    long micros = 36775038194L;
    assertThat(func.apply(micros)).isEqualTo(micros);
  }

  // ========== Timestamp Casting Tests ==========

  @Test
  public void testTimestampToDate() {
    Transform<Long, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.TimestampType.withoutZone());

    // Midnight timestamp should convert to date
    long midnightMicros = 17532L * 86400L * 1_000_000L; // 2018-01-01T00:00:00
    assertThat(func.apply(midnightMicros)).isEqualTo(17532);

    // Non-midnight timestamp should also convert
    long timestampMicros = 1514764800123456L;
    int expectedDays = (int) (timestampMicros / (86400L * 1_000_000L));
    assertThat(func.apply(timestampMicros)).isEqualTo(expectedDays);
  }

  @Test
  public void testTimestampToTimestampNano() {
    Transform<Long, Long> cast = Transforms.cast(Types.TimestampNanoType.withoutZone());
    SerializableFunction<Long, Long> func = cast.bind(Types.TimestampType.withoutZone());

    long timestampMicros = 1514764800000000L;
    long expectedNanos = timestampMicros * 1000L;
    assertThat(func.apply(timestampMicros)).isEqualTo(expectedNanos);
  }

  @Test
  public void testTimestampToLong() {
    Transform<Long, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Long, Long> func = cast.bind(Types.TimestampType.withoutZone());

    long timestampMicros = 1514764800000000L;
    assertThat(func.apply(timestampMicros)).isEqualTo(timestampMicros);
  }

  @Test
  public void testTimestampToString() {
    Transform<Long, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Long, String> func = cast.bind(Types.TimestampType.withoutZone());

    long timestampMicros = DateTimeUtil.isoTimestampToMicros("2018-01-01T10:12:55.038194");
    assertThat(func.apply(timestampMicros)).isEqualTo("2018-01-01T10:12:55.038194");
  }

  @Test
  public void testTimestampWithZoneToString() {
    Transform<Long, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Long, String> func = cast.bind(Types.TimestampType.withZone());

    long timestampMicros = DateTimeUtil.isoTimestamptzToMicros("2018-01-01T10:12:55.038194+00:00");
    assertThat(func.apply(timestampMicros)).isEqualTo("2018-01-01T10:12:55.038194+00:00");
  }

  // ========== TimestampNano Casting Tests ==========

  @Test
  public void testTimestampNanoToDate() {
    Transform<Long, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<Long, Integer> func = cast.bind(Types.TimestampNanoType.withoutZone());

    long midnightNanos = 17532L * 86400L * 1_000_000_000L;
    assertThat(func.apply(midnightNanos)).isEqualTo(17532);
  }

  @Test
  public void testTimestampNanoToTimestamp() {
    Transform<Long, Long> cast = Transforms.cast(Types.TimestampType.withoutZone());
    SerializableFunction<Long, Long> func = cast.bind(Types.TimestampNanoType.withoutZone());

    long timestampNanos = 1514764800000000000L;
    long expectedMicros = timestampNanos / 1000L;
    assertThat(func.apply(timestampNanos)).isEqualTo(expectedMicros);
  }

  @Test
  public void testTimestampNanoToLong() {
    Transform<Long, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Long, Long> func = cast.bind(Types.TimestampNanoType.withoutZone());

    long timestampNanos = 1514764800000000000L;
    assertThat(func.apply(timestampNanos)).isEqualTo(timestampNanos);
  }

  @Test
  public void testTimestampNanoToString() {
    Transform<Long, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<Long, String> func = cast.bind(Types.TimestampNanoType.withoutZone());

    long timestampNanos = DateTimeUtil.isoTimestampToNanos("2018-01-01T10:12:55.038194123");
    assertThat(func.apply(timestampNanos)).isEqualTo("2018-01-01T10:12:55.038194123");
  }

  // ========== String Casting Tests ==========

  @Test
  public void testStringToBoolean() {
    Transform<String, Boolean> cast = Transforms.cast(Types.BooleanType.get());
    SerializableFunction<String, Boolean> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("true")).isTrue();
    assertThat(func.apply("false")).isFalse();
    assertThat(func.apply("  true  ")).isTrue();
  }

  @Test
  public void testStringToInteger() {
    Transform<String, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<String, Integer> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("42")).isEqualTo(42);
    assertThat(func.apply("-100")).isEqualTo(-100);
    assertThat(func.apply("  123  ")).isEqualTo(123);
  }

  @Test
  public void testStringToIntegerInvalid() {
    Transform<String, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<String, Integer> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("not_a_number")).isNull();
    assertThat(func.apply("12.34")).isNull();
  }

  @Test
  public void testStringToLong() {
    Transform<String, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<String, Long> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("42")).isEqualTo(42L);
    assertThat(func.apply("9223372036854775807")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testStringToFloat() {
    Transform<String, Float> cast = Transforms.cast(Types.FloatType.get());
    SerializableFunction<String, Float> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("3.14")).isEqualTo(3.14f);
    assertThat(func.apply("-100.5")).isEqualTo(-100.5f);
  }

  @Test
  public void testStringToDouble() {
    Transform<String, Double> cast = Transforms.cast(Types.DoubleType.get());
    SerializableFunction<String, Double> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("3.14")).isEqualTo(3.14);
    assertThat(func.apply("-100.5")).isEqualTo(-100.5);
  }

  @Test
  public void testStringToDecimal() {
    Transform<String, BigDecimal> cast = Transforms.cast(Types.DecimalType.of(10, 2));
    SerializableFunction<String, BigDecimal> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("42.99")).isEqualTo(new BigDecimal("42.99"));
    assertThat(func.apply("100")).isEqualTo(new BigDecimal("100"));
  }

  @Test
  public void testStringToDate() {
    Transform<String, Integer> cast = Transforms.cast(Types.DateType.get());
    SerializableFunction<String, Integer> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("2018-01-01")).isEqualTo(DateTimeUtil.isoDateToDays("2018-01-01"));
    assertThat(func.apply("1970-01-01")).isEqualTo(0);
  }

  @Test
  public void testStringToTime() {
    Transform<String, Long> cast = Transforms.cast(Types.TimeType.get());
    SerializableFunction<String, Long> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("10:12:55.038194"))
        .isEqualTo(DateTimeUtil.isoTimeToMicros("10:12:55.038194"));
  }

  @Test
  public void testStringToTimestamp() {
    Transform<String, Long> cast = Transforms.cast(Types.TimestampType.withoutZone());
    SerializableFunction<String, Long> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("2018-01-01T10:12:55.038194"))
        .isEqualTo(DateTimeUtil.isoTimestampToMicros("2018-01-01T10:12:55.038194"));
  }

  @Test
  public void testStringToTimestampWithZone() {
    Transform<String, Long> cast = Transforms.cast(Types.TimestampType.withZone());
    SerializableFunction<String, Long> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("2018-01-01T10:12:55.038194+00:00"))
        .isEqualTo(DateTimeUtil.isoTimestamptzToMicros("2018-01-01T10:12:55.038194+00:00"));
  }

  @Test
  public void testStringToTimestampNano() {
    Transform<String, Long> cast = Transforms.cast(Types.TimestampNanoType.withoutZone());
    SerializableFunction<String, Long> func = cast.bind(Types.StringType.get());

    assertThat(func.apply("2018-01-01T10:12:55.038194123"))
        .isEqualTo(DateTimeUtil.isoTimestampToNanos("2018-01-01T10:12:55.038194123"));
  }

  @Test
  public void testStringToUUID() {
    Transform<String, UUID> cast = Transforms.cast(Types.UUIDType.get());
    SerializableFunction<String, UUID> func = cast.bind(Types.StringType.get());

    UUID uuid = UUID.randomUUID();
    assertThat(func.apply(uuid.toString())).isEqualTo(uuid);
  }

  // ========== UUID Casting Tests ==========

  @Test
  public void testUUIDToString() {
    Transform<UUID, String> cast = Transforms.cast(Types.StringType.get());
    SerializableFunction<UUID, String> func = cast.bind(Types.UUIDType.get());

    UUID uuid = UUID.randomUUID();
    assertThat(func.apply(uuid)).isEqualTo(uuid.toString());
  }

  // ========== Binary/Fixed Casting Tests ==========

  @Test
  public void testBinaryToFixed() {
    Transform<ByteBuffer, ByteBuffer> cast = Transforms.cast(Types.FixedType.ofLength(3));
    SerializableFunction<ByteBuffer, ByteBuffer> func = cast.bind(Types.BinaryType.get());

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThat(func.apply(buffer)).isEqualTo(buffer);
  }

  @Test
  public void testBinaryToFixedWrongLength() {
    Transform<ByteBuffer, ByteBuffer> cast = Transforms.cast(Types.FixedType.ofLength(3));
    SerializableFunction<ByteBuffer, ByteBuffer> func = cast.bind(Types.BinaryType.get());

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    assertThat(func.apply(buffer)).isNull();
  }

  @Test
  public void testFixedToBinary() {
    Transform<ByteBuffer, ByteBuffer> cast = Transforms.cast(Types.BinaryType.get());
    SerializableFunction<ByteBuffer, ByteBuffer> func = cast.bind(Types.FixedType.ofLength(3));

    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThat(func.apply(buffer)).isEqualTo(buffer);
  }

  // ========== Null Handling Tests ==========

  @Test
  public void testNullValues() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Integer, Long> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(null)).isNull();
  }

  // ========== Transform Properties Tests ==========

  @Test
  public void testCanTransform() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());

    assertThat(cast.canTransform(Types.IntegerType.get())).isTrue();
    assertThat(cast.canTransform(Types.BooleanType.get())).isFalse();
  }

  @Test
  public void testGetResultType() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());

    assertThat(cast.getResultType(Types.IntegerType.get())).isEqualTo(Types.LongType.get());
  }

  @Test
  public void testPreservesOrderNumeric() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());

    assertThat(cast.preservesOrder(Types.IntegerType.get())).isTrue();
  }

  @Test
  public void testPreservesOrderDateToTimestamp() {
    Transform<Integer, Long> cast = Transforms.cast(Types.TimestampType.withoutZone());

    assertThat(cast.preservesOrder(Types.DateType.get())).isTrue();
  }

  @Test
  public void testPreservesOrderTimestampToDate() {
    Transform<Long, Integer> cast = Transforms.cast(Types.DateType.get());

    assertThat(cast.preservesOrder(Types.TimestampType.withoutZone())).isTrue();
  }

  @Test
  public void testNotPreservesOrderStringCasts() {
    Transform<String, Integer> cast = Transforms.cast(Types.IntegerType.get());

    // String to numeric doesn't preserve order
    assertThat(cast.preservesOrder(Types.StringType.get())).isFalse();
  }

  @Test
  public void testToString() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());

    assertThat(cast.toString()).contains("cast");
    assertThat(cast.toString()).contains("long");
  }

  @Test
  public void testEquals() {
    Transform<Integer, Long> cast1 = Transforms.cast(Types.LongType.get());
    Transform<Integer, Long> cast2 = Transforms.cast(Types.LongType.get());
    Transform<Integer, String> cast3 = Transforms.cast(Types.StringType.get());

    assertThat(cast1).isEqualTo(cast2);
    assertThat(cast1).isNotEqualTo(cast3);
  }

  @Test
  public void testHashCode() {
    Transform<Integer, Long> cast1 = Transforms.cast(Types.LongType.get());
    Transform<Integer, Long> cast2 = Transforms.cast(Types.LongType.get());

    assertThat(cast1.hashCode()).isEqualTo(cast2.hashCode());
  }

  @Test
  public void testDedupName() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());

    assertThat(cast.dedupName()).contains("cast");
    assertThat(cast.dedupName()).contains("long");
  }

  // ========== Edge Cases and Special Values ==========

  @Test
  public void testZeroValues() {
    Transform<Integer, Long> intToLong = Transforms.cast(Types.LongType.get());
    assertThat(intToLong.bind(Types.IntegerType.get()).apply(0)).isEqualTo(0L);

    Transform<Long, Integer> longToInt = Transforms.cast(Types.IntegerType.get());
    assertThat(longToInt.bind(Types.LongType.get()).apply(0L)).isEqualTo(0);
  }

  @Test
  public void testNegativeValues() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Integer, Long> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(-1)).isEqualTo(-1L);
    assertThat(func.apply(Integer.MIN_VALUE)).isEqualTo((long) Integer.MIN_VALUE);
  }

  @Test
  public void testMaxMinValues() {
    Transform<Integer, Long> cast = Transforms.cast(Types.LongType.get());
    SerializableFunction<Integer, Long> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(Integer.MAX_VALUE)).isEqualTo((long) Integer.MAX_VALUE);
    assertThat(func.apply(Integer.MIN_VALUE)).isEqualTo((long) Integer.MIN_VALUE);
  }

  // ========== Invalid Cast Tests ==========

  @Test
  public void testInvalidCastAttempts() {
    // Boolean cannot cast to numeric
    Transform<Boolean, Integer> boolToInt = Transforms.cast(Types.IntegerType.get());
    assertThat(boolToInt.canTransform(Types.BooleanType.get())).isFalse();

    // Float cannot cast to integer (lossy)
    Transform<Float, Integer> floatToInt = Transforms.cast(Types.IntegerType.get());
    assertThat(floatToInt.canTransform(Types.FloatType.get())).isFalse();

    // Date cannot cast to integer directly
    Transform<Integer, Integer> dateToInt = Transforms.cast(Types.IntegerType.get());
    assertThat(dateToInt.canTransform(Types.DateType.get())).isFalse();
  }

  // ========== Same Type Cast Tests ==========

  @Test
  public void testSameTypeCast() {
    Transform<Integer, Integer> cast = Transforms.cast(Types.IntegerType.get());
    SerializableFunction<Integer, Integer> func = cast.bind(Types.IntegerType.get());

    assertThat(func.apply(42)).isEqualTo(42);
    assertThat(cast.canTransform(Types.IntegerType.get())).isTrue();
    assertThat(cast.preservesOrder(Types.IntegerType.get())).isTrue();
  }
}
