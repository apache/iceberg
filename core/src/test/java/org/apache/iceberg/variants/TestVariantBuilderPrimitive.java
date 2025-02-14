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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.stream.Stream;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestVariantBuilderPrimitive {
  private static Stream<Arguments> primitiveInputs() {
    return Stream.of(
        Arguments.of("null", Variants.PhysicalType.NULL, null),
        Arguments.of("true", Variants.PhysicalType.BOOLEAN_TRUE, true),
        Arguments.of("false", Variants.PhysicalType.BOOLEAN_FALSE, false),
        Arguments.of("34", Variants.PhysicalType.INT8, (byte) 34),
        Arguments.of("1234", Variants.PhysicalType.INT16, (short) 1234),
        Arguments.of("1234567890", Variants.PhysicalType.INT32, 1234567890),
        Arguments.of("1234567890987654321", Variants.PhysicalType.INT64, 1234567890987654321L),
        Arguments.of("1234e-2", Variants.PhysicalType.DECIMAL4, new BigDecimal("12.34")),
        Arguments.of("123456.789", Variants.PhysicalType.DECIMAL4, new BigDecimal("123456.789")),
        Arguments.of(
            "123456789.987654321",
            Variants.PhysicalType.DECIMAL8,
            new BigDecimal("123456789.987654321")),
        Arguments.of(
            "12345678901234567890.987654321",
            Variants.PhysicalType.DECIMAL16,
            new BigDecimal("12345678901234567890.987654321")),
        Arguments.of(
            "\"This test string is used to generate a primitive string type of variant\"",
            Variants.PhysicalType.STRING,
            "This test string is used to generate a primitive string type of variant"));
  }

  @ParameterizedTest
  @MethodSource("primitiveInputs")
  public void testPrimitiveJson(
      String input, Variants.PhysicalType expectedType, Object expectedValue) {
    Variant variant = VariantBuilder.parseJson(input);
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(expectedType);
    assertThat(primitive.get()).isEqualTo(expectedValue);
  }

  @Test
  public void testShortStringJson() throws IOException {
    Variant variant = VariantBuilder.parseJson("\"iceberg\"");
    VariantPrimitive shortString = variant.value().asPrimitive();

    assertThat(shortString.type()).isEqualTo(Variants.PhysicalType.STRING);
    assertThat(shortString.get()).isEqualTo("iceberg");
  }

  @Test
  public void testPrimitiveNull() {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeNull();
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.NULL);
    assertThat(primitive.get()).isEqualTo(null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPrimitiveBoolean(boolean value) {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeBoolean(value);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type())
        .isEqualTo(
            value ? Variants.PhysicalType.BOOLEAN_TRUE : Variants.PhysicalType.BOOLEAN_FALSE);
    assertThat(primitive.get()).isEqualTo(value);
  }

  private static Stream<Arguments> testPrimitiveNumericInputs() {
    return Stream.of(
        Arguments.of(34, Variants.PhysicalType.INT8, (byte) 34),
        Arguments.of(1234, Variants.PhysicalType.INT16, (short) 1234),
        Arguments.of(1234567890, Variants.PhysicalType.INT32, 1234567890),
        Arguments.of(1234567890987654321L, Variants.PhysicalType.INT64, 1234567890987654321L));
  }

  @ParameterizedTest
  @MethodSource("testPrimitiveNumericInputs")
  public void testPrimitiveNumeric(long value, Variants.PhysicalType type, Object expectedValue) {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeIntegral(value);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(type);
    assertThat(primitive.get()).isEqualTo(expectedValue);
  }

  @Test
  public void testPrimitiveDouble() {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeDouble(1234e-2);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.DOUBLE);
    assertThat(primitive.get()).isEqualTo(12.34);
  }

  private static Stream<Arguments> testPrimitiveDecimalInputs() {
    return Stream.of(
        Arguments.of(new BigDecimal("123456.789"), Variants.PhysicalType.DECIMAL4),
        Arguments.of(new BigDecimal("123456789.987654321"), Variants.PhysicalType.DECIMAL8),
        Arguments.of(
            new BigDecimal("12345678901234567890.987654321"), Variants.PhysicalType.DECIMAL16));
  }

  @ParameterizedTest
  @MethodSource("testPrimitiveDecimalInputs")
  public void testPrimitiveDecimal(BigDecimal value, Variants.PhysicalType type) {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeDecimal(value);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(type);
    assertThat(primitive.get()).isEqualTo(value);
  }

  @Test
  public void testPrimitiveDate() {
    String dateString = "2017-08-18";
    LocalDate date = LocalDate.parse(dateString);

    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeDate(date);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.DATE);
    assertThat(primitive.get()).isEqualTo(DateTimeUtil.daysFromDate(date));
  }

  @Test
  public void testPrimitiveTimestampTZ() {
    String tzString = "2017-08-18T14:21:01.919+00:00";
    OffsetDateTime ts = OffsetDateTime.parse(tzString);

    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeTimestampTz(ts);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.TIMESTAMPTZ);
    assertThat(primitive.get()).isEqualTo(DateTimeUtil.microsFromTimestamptz(ts));
  }

  @Test
  public void testPrimitiveTimestampNTZ() {
    String ntzString = "2017-08-18T14:21:01.919";
    LocalDateTime ts = LocalDateTime.parse(ntzString);

    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeTimestampNtz(ts);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.TIMESTAMPNTZ);
    assertThat(primitive.get()).isEqualTo(DateTimeUtil.microsFromTimestamp(ts));
  }

  @Test
  public void testPrimitiveFloat() {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeFloat(12.34f);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.FLOAT);
    assertThat(primitive.get()).isEqualTo(12.34f);
  }

  @Test
  public void testPrimitiveBinary() {
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeBinary("iceberg".getBytes());
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.BINARY);
    assertThat(primitive.get()).isEqualTo(ByteBuffer.wrap("iceberg".getBytes()));
  }

  @Test
  public void testPrimitiveString() {
    String value = "This test string is used to generate a primitive string type of variant";
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeString(value);
    Variant variant = builder.build();
    VariantPrimitive primitive = variant.value().asPrimitive();

    assertThat(primitive.type()).isEqualTo(Variants.PhysicalType.STRING);
    assertThat(primitive.get()).isEqualTo(value);
  }

  @Test
  public void testPrimitiveShortString() {
    String value = "iceberg";
    VariantPrimitiveBuilder builder = new VariantBuilder().createPrimitive();
    builder.writeString(value);
    Variant variant = builder.build();
    VariantPrimitive shortString = variant.value().asPrimitive();

    assertThat(shortString.type()).isEqualTo(Variants.PhysicalType.STRING);
    assertThat(shortString.get()).isEqualTo("iceberg");
  }
}
