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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSchemaParser extends DataTestBase {
  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected boolean supportsGeospatial() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  public void testSchemaId() {
    Schema schema = new Schema(34, required(1, "id", Types.LongType.get()));

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.schemaId()).isEqualTo(schema.schemaId());
  }

  @Test
  public void testIdentifierColumns() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(1, "id-1", Types.LongType.get()),
                required(2, "id-2", Types.LongType.get()),
                optional(3, "data", Types.StringType.get())),
            Sets.newHashSet(1, 2));

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.identifierFieldIds()).isEqualTo(Sets.newHashSet(1, 2));
  }

  @Test
  public void testDocStrings() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get(), "unique identifier"),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withDoc("payload")
                .build());

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.findField("id").doc()).isEqualTo("unique identifier");
    assertThat(serialized.findField("data").doc()).isEqualTo("payload");
  }

  private static Stream<Arguments> primitiveTypesAndDefaults() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), Literal.of(false)),
        Arguments.of(Types.IntegerType.get(), Literal.of(34)),
        Arguments.of(Types.LongType.get(), Literal.of(4900000000L)),
        Arguments.of(Types.FloatType.get(), Literal.of(12.21F)),
        Arguments.of(Types.DoubleType.get(), Literal.of(-0.0D)),
        Arguments.of(Types.DateType.get(), Literal.of(DateTimeUtil.isoDateToDays("2024-12-17"))),
        // Arguments.of(Types.TimeType.get(), DateTimeUtil.isoTimeToMicros("23:59:59.999999")),
        Arguments.of(
            Types.TimestampType.withZone(),
            Literal.of(DateTimeUtil.isoTimestamptzToMicros("2024-12-17T23:59:59.999999+00:00"))),
        Arguments.of(
            Types.TimestampNanoType.withZone(),
            Expressions.nanos(
                DateTimeUtil.isoTimestamptzToNanos("2024-12-17T23:59:59.123456789+00:00"))),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            Literal.of(DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.999999"))),
        Arguments.of(
            Types.TimestampNanoType.withoutZone(),
            Expressions.nanos(DateTimeUtil.isoTimestampToNanos("2024-12-17T23:59:59.123456789"))),
        Arguments.of(Types.StringType.get(), Literal.of("iceberg")),
        Arguments.of(Types.UUIDType.get(), Literal.of(UUID.randomUUID())),
        Arguments.of(
            Types.FixedType.ofLength(4),
            Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))),
        Arguments.of(Types.BinaryType.get(), Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b}))),
        Arguments.of(Types.DecimalType.of(9, 2), Literal.of(new BigDecimal("12.34"))));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypesAndDefaults")
  public void testPrimitiveTypeDefaultValues(Type.PrimitiveType type, Literal<?> defaultValue) {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.required("col_with_default")
                .withId(2)
                .ofType(type)
                .withInitialDefault(defaultValue)
                .withWriteDefault(defaultValue)
                .build());

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.findField("col_with_default").initialDefault())
        .isEqualTo(defaultValue.value());
    assertThat(serialized.findField("col_with_default").writeDefault())
        .isEqualTo(defaultValue.value());
  }

  private static Stream<Arguments> nonFiniteDefaults() {
    return Stream.of(
        Arguments.of(Types.FloatType.get(), "NaN", Float.NaN, "Infinity", Float.POSITIVE_INFINITY),
        Arguments.of(
            Types.FloatType.get(),
            "Infinity",
            Float.POSITIVE_INFINITY,
            "-Infinity",
            Float.NEGATIVE_INFINITY),
        Arguments.of(Types.FloatType.get(), "-Infinity", Float.NEGATIVE_INFINITY, "NaN", Float.NaN),
        Arguments.of(
            Types.DoubleType.get(), "NaN", Double.NaN, "Infinity", Double.POSITIVE_INFINITY),
        Arguments.of(
            Types.DoubleType.get(),
            "Infinity",
            Double.POSITIVE_INFINITY,
            "-Infinity",
            Double.NEGATIVE_INFINITY),
        Arguments.of(
            Types.DoubleType.get(), "-Infinity", Double.NEGATIVE_INFINITY, "NaN", Double.NaN));
  }

  @ParameterizedTest
  @MethodSource("nonFiniteDefaults")
  public void nonFiniteDefaultValues(
      Type.PrimitiveType type,
      String initialDefaultJson,
      Object initialDefault,
      String writeDefaultJson,
      Object writeDefault) {
    String json =
        schemaJsonWithDefaults(
            type,
            String.format(
                "\"initial-default\":\"%s\",\"write-default\":\"%s\"",
                initialDefaultJson, writeDefaultJson));

    Types.NestedField parsed = SchemaParser.fromJson(json).findField("col");
    assertThat(parsed.type()).isEqualTo(type);
    assertThat(parsed.initialDefault()).isEqualTo(initialDefault);
    assertThat(parsed.writeDefault()).isEqualTo(writeDefault);
  }

  @ParameterizedTest
  @ValueSource(strings = {"initial-default", "write-default"})
  public void nonFiniteDefaultWithSingleAttribute(String attribute) {
    String json =
        schemaJsonWithDefaults(Types.FloatType.get(), String.format("\"%s\":\"NaN\"", attribute));

    Types.NestedField parsed = SchemaParser.fromJson(json).findField("col");
    Object presentDefault =
        attribute.equals("initial-default") ? parsed.initialDefault() : parsed.writeDefault();
    Object absentDefault =
        attribute.equals("initial-default") ? parsed.writeDefault() : parsed.initialDefault();
    assertThat((Float) presentDefault).isNaN();
    assertThat(absentDefault).isNull();
  }

  @Test
  public void nonFiniteStringDefaultForStringField() {
    String json =
        schemaJsonWithDefaults(
            Types.StringType.get(), "\"initial-default\":\"NaN\",\"write-default\":\"Infinity\"");

    Types.NestedField parsed = SchemaParser.fromJson(json).findField("col");
    assertThat(parsed.initialDefault()).isEqualTo("NaN");
    assertThat(parsed.writeDefault()).isEqualTo("Infinity");
  }

  private static Stream<Arguments> invalidFloatingPointDefaults() {
    return Stream.of("initial-default", "write-default")
        .flatMap(
            attribute ->
                Stream.of(Types.FloatType.get(), Types.DoubleType.get())
                    .flatMap(
                        type ->
                            Stream.of(
                                    "nan",
                                    "NAN",
                                    "infinity",
                                    "+Infinity",
                                    "-infinity",
                                    "Inf",
                                    "1.25",
                                    "")
                                .map(value -> Arguments.of(attribute, type, value))));
  }

  @ParameterizedTest
  @MethodSource("invalidFloatingPointDefaults")
  public void invalidFloatingPointDefaultStrings(
      String attribute, Type.PrimitiveType type, String value) {
    String json = schemaJsonWithDefaults(type, String.format("\"%s\":\"%s\"", attribute, value));

    assertThatThrownBy(() -> SchemaParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse default as a %s value: \"%s\"", type, value);
  }

  @Test
  public void nonFiniteDefaultStringForIntegerField() {
    String json = schemaJsonWithDefaults(Types.IntegerType.get(), "\"initial-default\":\"NaN\"");

    assertThatThrownBy(() -> SchemaParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse default as a int value: \"NaN\"");
  }

  private static String schemaJsonWithDefaults(Type type, String defaults) {
    return String.format(
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"col\","
            + "\"required\":false,\"type\":\"%s\",%s}]}",
        type, defaults);
  }
}
