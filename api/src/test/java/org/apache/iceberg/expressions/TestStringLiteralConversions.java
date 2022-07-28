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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestStringLiteralConversions {
  @Test
  public void testStringToStringLiteral() {
    Literal<CharSequence> string = Literal.of("abc");
    Assert.assertSame("Should return same instance", string, string.to(Types.StringType.get()));
  }

  @Test
  public void testStringToDateLiteral() {
    Literal<CharSequence> dateStr = Literal.of("2017-08-18");
    Literal<Integer> date = dateStr.to(Types.DateType.get());

    // use Avro's date conversion to validate the result
    Schema avroSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    TimeConversions.DateConversion avroConversion = new TimeConversions.DateConversion();
    int avroValue =
        avroConversion.toInt(LocalDate.of(2017, 8, 18), avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Date should match", avroValue, (int) date.value());
  }

  @Test
  public void testNegativeStringToDateLiteral() {
    Literal<CharSequence> dateStr = Literal.of("1969-12-30");
    Literal<Integer> date = dateStr.to(Types.DateType.get());

    // use Avro's date conversion to validate the result
    Schema avroSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    TimeConversions.DateConversion avroConversion = new TimeConversions.DateConversion();
    int avroValue =
        avroConversion.toInt(LocalDate.of(1969, 12, 30), avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Date should be -2", -2, (int) date.value());
    Assert.assertEquals("Date should match", avroValue, (int) date.value());
  }

  @Test
  public void testStringToTimeLiteral() {
    // use Avro's time conversion to validate the result
    Schema avroSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));

    Literal<CharSequence> timeStr = Literal.of("14:21:01.919");
    Literal<Long> time = timeStr.to(Types.TimeType.get());

    long avroValue =
        new TimeConversions.TimeMicrosConversion()
            .toLong(
                LocalTime.of(14, 21, 1, 919 * 1000000), avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Time should match", avroValue, (long) time.value());
  }

  @Test
  public void testStringToTimestampLiteral() {
    // use Avro's timestamp conversion to validate the result
    Schema avroSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    TimeConversions.TimestampMicrosConversion avroConversion =
        new TimeConversions.TimestampMicrosConversion();

    // Timestamp with explicit UTC offset, +00:00
    Literal<CharSequence> timestampStr = Literal.of("2017-08-18T14:21:01.919+00:00");
    Literal<Long> timestamp = timestampStr.to(Types.TimestampType.withZone());
    long avroValue =
        avroConversion.toLong(
            LocalDateTime.of(2017, 8, 18, 14, 21, 1, 919 * 1000000).toInstant(ZoneOffset.UTC),
            avroSchema,
            avroSchema.getLogicalType());

    Assert.assertEquals("Timestamp should match", avroValue, (long) timestamp.value());

    // Timestamp without an explicit zone should be UTC (equal to the previous converted value)
    timestampStr = Literal.of("2017-08-18T14:21:01.919");
    timestamp = timestampStr.to(Types.TimestampType.withoutZone());

    Assert.assertEquals(
        "Timestamp without zone should match UTC", avroValue, (long) timestamp.value());

    // Timestamp with an explicit offset should be adjusted to UTC
    timestampStr = Literal.of("2017-08-18T14:21:01.919-07:00");
    timestamp = timestampStr.to(Types.TimestampType.withZone());
    avroValue =
        avroConversion.toLong(
            LocalDateTime.of(2017, 8, 18, 21, 21, 1, 919 * 1000000).toInstant(ZoneOffset.UTC),
            avroSchema,
            avroSchema.getLogicalType());

    Assert.assertEquals(
        "Timestamp without zone should match UTC", avroValue, (long) timestamp.value());
  }

  @Test
  public void testNegativeStringToTimestampLiteral() {
    // use Avro's timestamp conversion to validate the result
    Schema avroSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    TimeConversions.TimestampMicrosConversion avroConversion =
        new TimeConversions.TimestampMicrosConversion();

    // Timestamp with explicit UTC offset, +00:00
    Literal<CharSequence> timestampStr = Literal.of("1969-12-31T23:59:58.999999+00:00");
    Literal<Long> timestamp = timestampStr.to(Types.TimestampType.withZone());
    long avroValue =
        avroConversion.toLong(
            LocalDateTime.of(1969, 12, 31, 23, 59, 58, 999999 * 1_000).toInstant(ZoneOffset.UTC),
            avroSchema,
            avroSchema.getLogicalType());

    Assert.assertEquals("Timestamp should match", avroValue, (long) timestamp.value());
    Assert.assertEquals("Timestamp should be -1_000_001", -1_000_001, (long) timestamp.value());

    // Timestamp without an explicit zone should be UTC (equal to the previous converted value)
    timestampStr = Literal.of("1969-12-31T23:59:58.999999");
    timestamp = timestampStr.to(Types.TimestampType.withoutZone());

    Assert.assertEquals(
        "Timestamp without zone should match UTC", avroValue, (long) timestamp.value());

    // Timestamp with an explicit offset should be adjusted to UTC
    timestampStr = Literal.of("1969-12-31T16:59:58.999999-07:00");
    timestamp = timestampStr.to(Types.TimestampType.withZone());
    avroValue =
        avroConversion.toLong(
            LocalDateTime.of(1969, 12, 31, 23, 59, 58, 999999 * 1_000).toInstant(ZoneOffset.UTC),
            avroSchema,
            avroSchema.getLogicalType());

    Assert.assertEquals(
        "Timestamp without zone should match UTC", avroValue, (long) timestamp.value());
    Assert.assertEquals(
        "Timestamp without zone should be -1_000_001", -1_000_001, (long) timestamp.value());
  }

  @Test
  public void testTimestampWithZoneWithoutZoneInLiteral() {
    // Zone must be present in literals when converting to timestamp with zone
    Literal<CharSequence> timestampStr = Literal.of("2017-08-18T14:21:01.919");
    Assertions.assertThatThrownBy(() -> timestampStr.to(Types.TimestampType.withZone()))
        .isInstanceOf(DateTimeException.class)
        .hasMessageContaining("could not be parsed");
  }

  @Test
  public void testTimestampWithoutZoneWithZoneInLiteral() {
    // Zone must not be present in literals when converting to timestamp without zone
    Literal<CharSequence> timestampStr = Literal.of("2017-08-18T14:21:01.919+07:00");
    Assertions.assertThatThrownBy(() -> timestampStr.to(Types.TimestampType.withoutZone()))
        .isInstanceOf(DateTimeException.class)
        .hasMessageContaining("could not be parsed");
  }

  @Test
  public void testStringToUUIDLiteral() {
    UUID expected = UUID.randomUUID();
    Literal<CharSequence> uuidStr = Literal.of(expected.toString());
    Literal<UUID> uuid = uuidStr.to(Types.UUIDType.get());

    Assert.assertEquals("UUID should match", expected, uuid.value());
  }

  @Test
  public void testStringToDecimalLiteral() {
    BigDecimal expected = new BigDecimal("34.560");
    Literal<CharSequence> decimalStr = Literal.of("34.560");

    IntStream.range(0, 10)
        .forEach(
            scale -> {
              Literal<BigDecimal> decimal = decimalStr.to(Types.DecimalType.of(9, scale));
              Assert.assertEquals("Decimal should have scale 3", 3, decimal.value().scale());
              Assert.assertEquals("Decimal should match", expected, decimal.value());
            });
  }
}
