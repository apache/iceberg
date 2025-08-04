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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariantReaders {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", IntegerType.get()),
          NestedField.required(2, "var", VariantType.get()));

  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();

  private static final ByteBuffer TEST_METADATA_BUFFER =
      VariantTestUtil.createMetadata(ImmutableList.of("a", "b", "c", "d", "e"), true);
  private static final ByteBuffer TEST_OBJECT_BUFFER =
      VariantTestUtil.createObject(
          TEST_METADATA_BUFFER,
          ImmutableMap.of(
              "a", Variants.ofNull(),
              "d", Variants.of("iceberg")));

  private static final VariantMetadata EMPTY_METADATA =
      Variants.metadata(VariantTestUtil.emptyMetadata());
  private static final VariantMetadata TEST_METADATA = Variants.metadata(TEST_METADATA_BUFFER);
  private static final VariantObject TEST_OBJECT =
      (VariantObject) Variants.value(TEST_METADATA, TEST_OBJECT_BUFFER);

  private static final VariantPrimitive<?>[] PRIMITIVES =
      new VariantPrimitive[] {
        Variants.ofNull(),
        Variants.of(true),
        Variants.of(false),
        Variants.of((byte) 34),
        Variants.of((byte) -34),
        Variants.of((short) 1234),
        Variants.of((short) -1234),
        Variants.of(12345),
        Variants.of(-12345),
        Variants.of(9876543210L),
        Variants.of(-9876543210L),
        Variants.of(10.11F),
        Variants.of(-10.11F),
        Variants.of(14.3D),
        Variants.of(-14.3D),
        Variants.ofIsoDate("2024-11-07"),
        Variants.ofIsoDate("1957-11-07"),
        Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00"),
        Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456"),
        Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456"),
        Variants.of(new BigDecimal("12345.6789")), // decimal4
        Variants.of(new BigDecimal("-12345.6789")), // decimal4
        Variants.of(new BigDecimal("123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("-123456789.987654321")), // decimal8
        Variants.of(new BigDecimal("9876543210.123456789")), // decimal16
        Variants.of(new BigDecimal("-9876543210.123456789")), // decimal16
        Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d})),
        Variants.of("iceberg"),
        Variants.ofIsoTime("12:33:54.123456"),
        Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00"),
        Variants.ofIsoTimestamptzNanos("1957-11-07T12:33:54.123456789+00:00"),
        Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789"),
        Variants.ofIsoTimestampntzNanos("1957-11-07T12:33:54.123456789"),
        Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
      };

  // Required configuration to convert between Avro and Parquet schemas with 3-level list structure
  private static final ParquetConfiguration CONF =
      new PlainParquetConfiguration(
          Map.of(
              AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
              "false",
              AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS,
              "false"));

  private static Stream<Arguments> metadataAndValues() {
    Stream<Arguments> primitives =
        Stream.of(PRIMITIVES).map(variant -> Arguments.of(EMPTY_METADATA, variant));
    Stream<Arguments> object = Stream.of(Arguments.of(TEST_METADATA, TEST_OBJECT));
    return Streams.concat(primitives, object);
  }

  @ParameterizedTest
  @MethodSource("metadataAndValues")
  public void testUnshreddedVariants(VariantMetadata metadata, VariantValue expected)
      throws IOException {
    GroupType variantType = variant("var", 2);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", serialize(metadata), "value", serialize(expected)));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(metadata, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @ParameterizedTest
  @MethodSource("metadataAndValues")
  public void testUnshreddedVariantsWithShreddedSchema(
      VariantMetadata metadata, VariantValue expected) throws IOException {
    // the variant's Parquet schema has a shredded field that is unused by all data values
    GroupType variantType = variant("var", 2, shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", serialize(metadata), "value", serialize(expected)));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(metadata, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testShreddedVariantPrimitives(VariantPrimitive<?> primitive) throws IOException {
    assumeThat(primitive.type()).as("Null is not a shredded type").isNotEqualTo(PhysicalType.NULL);

    GroupType variantType = variant("var", 2, shreddedType(primitive));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                VariantTestUtil.emptyMetadata(),
                "typed_value",
                toAvroValue(primitive)));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(primitive, actualVariant.value());
  }

  @Test
  public void testNullValueAndNullTypedValue() throws IOException {
    GroupType variantType = variant("var", 2, shreddedPrimitive(PrimitiveTypeName.INT32));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", VariantTestUtil.emptyMetadata()));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(Variants.ofNull(), actualVariant.value());
  }

  @Test
  public void testMissingValueColumn() throws IOException {
    GroupType variantType =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .id(2)
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("var");
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", 34));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(Variants.of(34), actualVariant.value());
  }

  @Test
  public void testValueAndTypedValueConflict() {
    GroupType variantType = variant("var", 2, shreddedPrimitive(PrimitiveTypeName.INT32));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                VariantTestUtil.emptyMetadata(),
                "value",
                serialize(Variants.of("str")),
                "typed_value",
                34));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid variant, conflicting value and typed_value");
  }

  @Test
  public void testUnsignedInteger() {
    GroupType variantType =
        variant(
            "var",
            2,
            shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", VariantTestUtil.emptyMetadata()));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported shredded value type: INTEGER(32,false)");
  }

  @Test
  public void testFixedLengthByteArray() {
    GroupType variantType =
        variant(
            "var",
            2,
            Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4).named("typed_value"));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(variantType, Map.of("metadata", VariantTestUtil.emptyMetadata()));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Unsupported shredded value type: optional fixed_len_byte_array(4) typed_value");
  }

  @Test
  public void testShreddedObject() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.ofNull())));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", ""));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.ofNull());
    expected.put("b", Variants.of(""));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testShreddedObjectMissingValueColumn() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .id(2)
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .addField(objectFields)
            .named("var");

    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.of((short) 1234))));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.of((short) 1234));
    expected.put("b", Variants.of("iceberg"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testShreddedObjectMissingField() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.of(false))));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = record(fieldB, Map.of());
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.of(false));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testEmptyShreddedObject() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of()); // missing
    GenericRecord recordB = record(fieldB, Map.of()); // missing
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testShreddedObjectMissingFieldValueColumn() throws IOException {
    // field groups do not have value
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
            .named("b");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fieldA, fieldB).named("typed_value");
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of()); // typed_value=null
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("b", Variants.of("iceberg"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testShreddedObjectMissingTypedValue() throws IOException {
    // field groups do not have typed_value
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .named("b");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fieldA, fieldB).named("typed_value");
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of()); // value=null
    GenericRecord recordB = record(fieldB, Map.of("value", serialize(Variants.of("iceberg"))));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("b", Variants.of("iceberg"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testShreddedObjectWithinShreddedObject() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType innerFields = objectFields(fieldA, fieldB);
    GroupType fieldC = field("c", innerFields);
    GroupType fieldD = field("d", shreddedPrimitive(PrimitiveTypeName.DOUBLE));
    GroupType outerFields = objectFields(fieldC, fieldD);
    GroupType variantType = variant("var", 2, outerFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("typed_value", 34));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord inner = record(innerFields, Map.of("a", recordA, "b", recordB));
    GenericRecord recordC = record(fieldC, Map.of("typed_value", inner));
    GenericRecord recordD = record(fieldD, Map.of("typed_value", -0.0D));
    GenericRecord outer = record(outerFields, Map.of("c", recordC, "d", recordD));
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", outer));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expectedInner = Variants.object(TEST_METADATA);
    expectedInner.put("a", Variants.of(34));
    expectedInner.put("b", Variants.of("iceberg"));
    ShreddedObject expectedOuter = Variants.object(TEST_METADATA);
    expectedOuter.put("c", expectedInner);
    expectedOuter.put("d", Variants.of(-0.0D));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedOuter, actualVariant.value());
  }

  @Test
  public void testShreddedObjectWithOptionalFieldStructs() throws IOException {
    // fields use an incorrect OPTIONAL struct of value and typed_value to test definition levels
    GroupType fieldA =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.INT32))
            .named("a");
    GroupType fieldB =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.BINARY, STRING))
            .named("b");
    GroupType fieldC =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.DOUBLE))
            .named("c");
    GroupType fieldD =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .optional(PrimitiveTypeName.BINARY)
            .named("value")
            .addField(shreddedPrimitive(PrimitiveTypeName.BOOLEAN))
            .named("d");
    GroupType objectFields =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .addFields(fieldA, fieldB, fieldC, fieldD)
            .named("typed_value");
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.of(34))));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord recordC = record(fieldC, Map.of()); // c.value and c.typed_value are missing
    GenericRecord fields =
        record(objectFields, Map.of("a", recordA, "b", recordB, "c", recordC)); // d is missing
    GenericRecord variant =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    // the expected value is the shredded field value
    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.of(34));
    expected.put("b", Variants.of("iceberg"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testPartiallyShreddedObject() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    ShreddedObject baseObject = Variants.object(TEST_METADATA);
    baseObject.put("d", Variants.ofIsoDate("2024-01-30"));

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.ofNull())));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                TEST_METADATA_BUFFER,
                "value",
                serialize(baseObject),
                "typed_value",
                fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.ofNull());
    expected.put("b", Variants.of("iceberg"));
    expected.put("d", Variants.ofIsoDate("2024-01-30"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testPartiallyShreddedObjectFieldConflict() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    ByteBuffer baseObjectBuffer =
        VariantTestUtil.createObject(
            TEST_METADATA_BUFFER, Map.of("b", Variants.ofIsoDate("2024-01-30"))); // conflict

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.ofNull())));
    GenericRecord recordB = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                TEST_METADATA_BUFFER,
                "value",
                baseObjectBuffer,
                "typed_value",
                fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    // the expected value is the shredded field value
    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.ofNull());
    expected.put("b", Variants.of("iceberg"));

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testPartiallyShreddedObjectMissingFieldConflict() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    ByteBuffer baseObjectBuffer =
        VariantTestUtil.createObject(
            TEST_METADATA_BUFFER, Map.of("b", Variants.ofIsoDate("2024-01-30"))); // conflict

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.ofNull())));
    // value and typed_value are null, but a struct for b is required
    GenericRecord recordB = record(fieldB, Map.of());
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                TEST_METADATA_BUFFER,
                "value",
                baseObjectBuffer,
                "typed_value",
                fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    // the expected value is the shredded field value
    ShreddedObject expected = Variants.object(TEST_METADATA);
    expected.put("a", Variants.ofNull());

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expected, actualVariant.value());
  }

  @Test
  public void testNonObjectWithNullShreddedFields() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord variant =
        record(
            variantType,
            Map.of("metadata", TEST_METADATA_BUFFER, "value", serialize(Variants.of(34))));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(Variants.of(34), actualVariant.value());
  }

  @Test
  public void testNonObjectWithNonNullShreddedFields() {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of("value", serialize(Variants.ofNull())));
    GenericRecord recordB = record(fieldB, Map.of("value", serialize(Variants.of(9876543210L))));
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                TEST_METADATA_BUFFER,
                "value",
                serialize(Variants.of(34)),
                "typed_value",
                fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid variant, non-object value with shredded fields");
  }

  @Test
  public void testEmptyPartiallyShreddedObjectConflict() {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType objectFields = objectFields(fieldA, fieldB);
    GroupType variantType = variant("var", 2, objectFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord recordA = record(fieldA, Map.of()); // missing
    GenericRecord recordB = record(fieldB, Map.of()); // missing
    GenericRecord fields = record(objectFields, Map.of("a", recordA, "b", recordB));
    GenericRecord variant =
        record(
            variantType,
            Map.of(
                "metadata",
                TEST_METADATA_BUFFER,
                "value",
                serialize(Variants.ofNull()), // conflicting non-object
                "typed_value",
                fields));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid variant, non-object value with shredded fields");
  }

  @Test
  public void testMixedRecords() throws IOException {
    // tests multiple rows to check that Parquet columns are correctly advanced
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType innerFields = objectFields(fieldA, fieldB);
    GroupType fieldC = field("c", innerFields);
    GroupType fieldD = field("d", shreddedPrimitive(PrimitiveTypeName.DOUBLE));
    GroupType outerFields = objectFields(fieldC, fieldD);
    GroupType variantType = variant("var", 2, outerFields);
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord zero = record(parquetSchema, Map.of("id", 0));

    GenericRecord a1 = record(fieldA, Map.of()); // missing
    GenericRecord b1 = record(fieldB, Map.of("typed_value", "iceberg"));
    GenericRecord inner1 = record(innerFields, Map.of("a", a1, "b", b1));
    GenericRecord c1 = record(fieldC, Map.of("typed_value", inner1));
    GenericRecord d1 = record(fieldD, Map.of()); // missing
    GenericRecord outer1 = record(outerFields, Map.of("c", c1, "d", d1));
    GenericRecord variant1 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", outer1));
    GenericRecord one = record(parquetSchema, Map.of("id", 1, "var", variant1));

    ShreddedObject expectedC1 = Variants.object(TEST_METADATA);
    expectedC1.put("b", Variants.of("iceberg"));
    ShreddedObject expectedOne = Variants.object(TEST_METADATA);
    expectedOne.put("c", expectedC1);

    GenericRecord c2 = record(fieldC, Map.of("value", serialize(Variants.of((byte) 8))));
    GenericRecord d2 = record(fieldD, Map.of("typed_value", -0.0D));
    GenericRecord outer2 = record(outerFields, Map.of("c", c2, "d", d2));
    GenericRecord variant2 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", outer2));
    GenericRecord two = record(parquetSchema, Map.of("id", 2, "var", variant2));

    ShreddedObject expectedTwo = Variants.object(TEST_METADATA);
    expectedTwo.put("c", Variants.of((byte) 8));
    expectedTwo.put("d", Variants.of(-0.0D));

    GenericRecord a3 = record(fieldA, Map.of("typed_value", 34));
    GenericRecord b3 = record(fieldB, Map.of("value", serialize(Variants.of(""))));
    GenericRecord inner3 = record(innerFields, Map.of("a", a3, "b", b3));
    GenericRecord c3 = record(fieldC, Map.of("typed_value", inner3));
    GenericRecord d3 = record(fieldD, Map.of("typed_value", 0.0D));
    GenericRecord outer3 = record(outerFields, Map.of("c", c3, "d", d3));
    GenericRecord variant3 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", outer3));
    GenericRecord three = record(parquetSchema, Map.of("id", 3, "var", variant3));

    ShreddedObject expectedC3 = Variants.object(TEST_METADATA);
    expectedC3.put("a", Variants.of(34));
    expectedC3.put("b", Variants.of(""));
    ShreddedObject expectedThree = Variants.object(TEST_METADATA);
    expectedThree.put("c", expectedC3);
    expectedThree.put("d", Variants.of(0.0D));

    List<Record> records = writeAndRead(parquetSchema, List.of(zero, one, two, three));

    Record actualZero = records.get(0);
    assertThat(actualZero.getField("id")).isEqualTo(0);
    assertThat(actualZero.getField("var")).isNull();

    Record actualOne = records.get(1);
    assertThat(actualOne.getField("id")).isEqualTo(1);
    assertThat(actualOne.getField("var")).isInstanceOf(Variant.class);

    Variant actualOneVariant = (Variant) actualOne.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualOneVariant.metadata());
    VariantTestUtil.assertEqual(expectedOne, actualOneVariant.value());

    Record actualTwo = records.get(2);
    assertThat(actualTwo.getField("id")).isEqualTo(2);
    assertThat(actualTwo.getField("var")).isInstanceOf(Variant.class);

    Variant actualTwoVariant = (Variant) actualTwo.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualTwoVariant.metadata());
    VariantTestUtil.assertEqual(expectedTwo, actualTwoVariant.value());

    Record actualThree = records.get(3);
    assertThat(actualThree.getField("id")).isEqualTo(3);
    assertThat(actualThree.getField("var")).isInstanceOf(Variant.class);

    Variant actualThreeVariant = (Variant) actualThree.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualThreeVariant.metadata());
    VariantTestUtil.assertEqual(expectedThree, actualThreeVariant.value());
  }

  @Test
  public void testSimpleArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("typed_value", "drama")));

    GenericRecord var =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    ValueArray expectedArray = Variants.array();
    expectedArray.add(Variants.of("comedy"));
    expectedArray.add(Variants.of("drama"));

    Record actual = writeAndRead(parquetSchema, row);

    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedArray, actualVariant.value());
  }

  @Test
  public void testNullArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType variantType = variant("var", 2, list(element(shreddedType)));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord var =
        record(
            variantType,
            Map.of(
                "metadata",
                VariantTestUtil.emptyMetadata(),
                "value",
                serialize(Variants.ofNull())));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    Record actual = writeAndRead(parquetSchema, row);

    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(Variants.ofNull(), actualVariant.value());
  }

  @Test
  public void testEmptyArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType variantType = variant("var", 2, list(element(shreddedType)));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr = List.of();
    GenericRecord var =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    Record actual = writeAndRead(parquetSchema, row);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    assertThat(actualVariant.value().type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(actualVariant.value().asArray().numElements()).isEqualTo(0);
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
  }

  @Test
  public void testArrayWithNull() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("value", serialize(Variants.ofNull()))),
            record(elementType, Map.of("typed_value", "drama")));

    GenericRecord var =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    ValueArray expectedArray = Variants.array();
    expectedArray.add(Variants.of("comedy"));
    expectedArray.add(Variants.ofNull());
    expectedArray.add(Variants.of("drama"));

    Record actual = writeAndRead(parquetSchema, row);

    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    assertThat(actualVariant.value().type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(actualVariant.value().asArray().numElements()).isEqualTo(3);
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedArray, actualVariant.value());
  }

  @Test
  public void testNestedArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType outerElementType = element(list(elementType));
    GroupType variantType = variant("var", 2, list(outerElementType));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> inner1 =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("typed_value", "drama")));
    List<GenericRecord> outer1 =
        List.of(
            record(outerElementType, Map.of("typed_value", inner1)),
            record(outerElementType, Map.of("typed_value", List.of())));
    GenericRecord var =
        record(
            variantType,
            Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", outer1));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    ValueArray expectedArray = Variants.array();
    ValueArray expectedInner1 = Variants.array();
    expectedInner1.add(Variants.of("comedy"));
    expectedInner1.add(Variants.of("drama"));
    ValueArray expectedInner2 = Variants.array();
    expectedArray.add(expectedInner1);
    expectedArray.add(expectedInner2);

    Record actual = writeAndRead(parquetSchema, row);

    // Verify
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedArray, actualVariant.value());
  }

  @Test
  public void testArrayWithNestedObject() throws IOException {
    GroupType fieldA = field("a", shreddedPrimitive(PrimitiveTypeName.INT32));
    GroupType fieldB = field("b", shreddedPrimitive(PrimitiveTypeName.BINARY, STRING));
    GroupType shreddedFields = objectFields(fieldA, fieldB);
    GroupType elementType = element(shreddedFields);
    GroupType listType = list(elementType);
    GroupType variantType = variant("var", 2, listType);
    MessageType parquetSchema = parquetSchema(variantType);

    // Row 1 with nested fully shredded object
    GenericRecord shredded1 =
        record(
            shreddedFields,
            Map.of(
                "a",
                record(fieldA, Map.of("typed_value", 1)),
                "b",
                record(fieldB, Map.of("typed_value", "comedy"))));
    GenericRecord shredded2 =
        record(
            shreddedFields,
            Map.of(
                "a",
                record(fieldA, Map.of("typed_value", 2)),
                "b",
                record(fieldB, Map.of("typed_value", "drama"))));
    List<GenericRecord> arr1 =
        List.of(
            record(elementType, Map.of("typed_value", shredded1)),
            record(elementType, Map.of("typed_value", shredded2)));
    GenericRecord var1 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", arr1));
    GenericRecord row1 = record(parquetSchema, Map.of("id", 1, "var", var1));

    ValueArray expected1 = Variants.array();
    ShreddedObject expectedElement1 = Variants.object(TEST_METADATA);
    expectedElement1.put("a", Variants.of(1));
    expectedElement1.put("b", Variants.of("comedy"));
    expected1.add(expectedElement1);
    ShreddedObject expectedElement2 = Variants.object(TEST_METADATA);
    expectedElement2.put("a", Variants.of(2));
    expectedElement2.put("b", Variants.of("drama"));
    expected1.add(expectedElement2);

    // Row 2 with nested partially shredded object
    GenericRecord shredded3 =
        record(
            shreddedFields,
            Map.of(
                "a",
                record(fieldA, Map.of("typed_value", 3)),
                "b",
                record(fieldB, Map.of("typed_value", "action"))));
    ShreddedObject baseObject3 = Variants.object(TEST_METADATA);
    baseObject3.put("c", Variants.of("str"));

    GenericRecord shredded4 =
        record(
            shreddedFields,
            Map.of(
                "a",
                record(fieldA, Map.of("typed_value", 4)),
                "b",
                record(fieldB, Map.of("typed_value", "horror"))));
    ShreddedObject baseObject4 = Variants.object(TEST_METADATA);
    baseObject4.put("d", Variants.ofIsoDate("2024-01-30"));

    List<GenericRecord> arr2 =
        List.of(
            record(elementType, Map.of("value", serialize(baseObject3), "typed_value", shredded3)),
            record(elementType, Map.of("value", serialize(baseObject4), "typed_value", shredded4)));
    GenericRecord var2 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", arr2));
    GenericRecord row2 = record(parquetSchema, Map.of("id", 2, "var", var2));

    ValueArray expected2 = Variants.array();
    ShreddedObject expectedElement3 = Variants.object(TEST_METADATA);
    expectedElement3.put("a", Variants.of(3));
    expectedElement3.put("b", Variants.of("action"));
    expectedElement3.put("c", Variants.of("str"));
    expected2.add(expectedElement3);
    ShreddedObject expectedElement4 = Variants.object(TEST_METADATA);
    expectedElement4.put("a", Variants.of(4));
    expectedElement4.put("b", Variants.of("horror"));
    expectedElement4.put("d", Variants.ofIsoDate("2024-01-30"));
    expected2.add(expectedElement4);

    // verify
    List<Record> actual = writeAndRead(parquetSchema, List.of(row1, row2));
    Record actual1 = actual.get(0);
    assertThat(actual1.getField("id")).isEqualTo(1);
    assertThat(actual1.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant1 = (Variant) actual1.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant1.metadata());
    VariantTestUtil.assertEqual(expected1, actualVariant1.value());

    Record actual2 = actual.get(1);
    assertThat(actual2.getField("id")).isEqualTo(2);
    assertThat(actual2.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant2 = (Variant) actual2.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant2.metadata());
    VariantTestUtil.assertEqual(expected2, actualVariant2.value());
  }

  @Test
  public void testArrayWithNonArray() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr1 =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("typed_value", "drama")));
    GenericRecord var1 =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr1));
    GenericRecord row1 = record(parquetSchema, Map.of("id", 1, "var", var1));

    ValueArray expectedArray1 = Variants.array();
    expectedArray1.add(Variants.of("comedy"));
    expectedArray1.add(Variants.of("drama"));

    GenericRecord var2 =
        record(
            variantType,
            Map.of(
                "metadata", VariantTestUtil.emptyMetadata(), "value", serialize(Variants.of(34))));
    GenericRecord row2 = record(parquetSchema, Map.of("id", 2, "var", var2));

    VariantValue expectedValue2 = Variants.of(PhysicalType.INT32, 34);

    GenericRecord var3 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "value", TEST_OBJECT_BUFFER));
    GenericRecord row3 = record(parquetSchema, Map.of("id", 3, "var", var3));

    ShreddedObject expectedObject3 = Variants.object(TEST_METADATA);
    expectedObject3.put("a", Variants.ofNull());
    expectedObject3.put("d", Variants.of("iceberg"));

    // Test array is read properly after a non-array
    List<GenericRecord> arr4 =
        List.of(
            record(elementType, Map.of("typed_value", "action")),
            record(elementType, Map.of("typed_value", "horror")));
    GenericRecord var4 =
        record(variantType, Map.of("metadata", TEST_METADATA_BUFFER, "typed_value", arr4));
    GenericRecord row4 = record(parquetSchema, Map.of("id", 4, "var", var4));

    ValueArray expectedArray4 = Variants.array();
    expectedArray4.add(Variants.of("action"));
    expectedArray4.add(Variants.of("horror"));

    List<Record> actual = writeAndRead(parquetSchema, List.of(row1, row2, row3, row4));

    // Verify
    Record actual1 = actual.get(0);
    assertThat(actual1.getField("id")).isEqualTo(1);
    assertThat(actual1.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant1 = (Variant) actual1.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant1.metadata());
    VariantTestUtil.assertEqual(expectedArray1, actualVariant1.value());

    Record actual2 = actual.get(1);
    assertThat(actual2.getField("id")).isEqualTo(2);
    assertThat(actual2.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant2 = (Variant) actual2.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant2.metadata());
    VariantTestUtil.assertEqual(expectedValue2, actualVariant2.value());

    Record actual3 = actual.get(2);
    assertThat(actual3.getField("id")).isEqualTo(3);
    assertThat(actual3.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant3 = (Variant) actual3.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant3.metadata());
    VariantTestUtil.assertEqual(expectedObject3, actualVariant3.value());

    Record actual4 = actual.get(3);
    assertThat(actual4.getField("id")).isEqualTo(4);
    assertThat(actual4.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant4 = (Variant) actual4.getField("var");
    VariantTestUtil.assertEqual(TEST_METADATA, actualVariant4.metadata());
    VariantTestUtil.assertEqual(expectedArray4, actualVariant4.value());
  }

  @Test
  public void testArrayMissingValueColumn() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .id(2)
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .addField(list(elementType))
            .named("var");

    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("typed_value", "drama")));
    GenericRecord var =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    ValueArray expectedArray = Variants.array();
    expectedArray.add(Variants.of("comedy"));
    expectedArray.add(Variants.of("drama"));

    Record actual = writeAndRead(parquetSchema, row);

    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedArray, actualVariant.value());
  }

  @Test
  public void testArrayMissingElementValueColumn() throws IOException {
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType =
        Types.buildGroup(Type.Repetition.REQUIRED).addField(shreddedType).named("element");

    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    List<GenericRecord> arr =
        List.of(
            record(elementType, Map.of("typed_value", "comedy")),
            record(elementType, Map.of("typed_value", "drama")));
    GenericRecord var =
        record(
            variantType, Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", arr));
    GenericRecord row = record(parquetSchema, Map.of("id", 1, "var", var));

    ValueArray expectedArray = Variants.array();
    expectedArray.add(Variants.of("comedy"));
    expectedArray.add(Variants.of("drama"));

    Record actual = writeAndRead(parquetSchema, row);

    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);
    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantTestUtil.assertEqual(expectedArray, actualVariant.value());
  }

  @Test
  public void testArrayWithElementNullValueAndNullTypedValue() throws IOException {
    // Test the invalid case that both value and typed_value of an element are null
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord element = record(elementType, Map.of());
    GenericRecord variant =
        record(
            variantType,
            Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", List.of(element)));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    Record actual = writeAndRead(parquetSchema, record);
    assertThat(actual.getField("id")).isEqualTo(1);
    assertThat(actual.getField("var")).isInstanceOf(Variant.class);

    Variant actualVariant = (Variant) actual.getField("var");
    VariantTestUtil.assertEqual(EMPTY_METADATA, actualVariant.metadata());
    VariantValue actualValue = actualVariant.value();
    assertThat(actualValue.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(actualValue.asArray().numElements()).isEqualTo(1);
    VariantTestUtil.assertEqual(Variants.ofNull(), actualValue.asArray().get(0));
  }

  @Test
  public void testArrayWithElementValueTypedValueConflict() {
    // Test the invalid case that both value and typed_value of an element are not null
    Type shreddedType = shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
    GroupType elementType = element(shreddedType);
    GroupType variantType = variant("var", 2, list(elementType));
    MessageType parquetSchema = parquetSchema(variantType);

    GenericRecord element =
        record(elementType, Map.of("value", serialize(Variants.of(3)), "typed_value", "comedy"));
    GenericRecord variant =
        record(
            variantType,
            Map.of("metadata", VariantTestUtil.emptyMetadata(), "typed_value", List.of(element)));
    GenericRecord record = record(parquetSchema, Map.of("id", 1, "var", variant));

    assertThatThrownBy(() -> writeAndRead(parquetSchema, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid variant, conflicting value and typed_value");
  }

  private static ByteBuffer serialize(VariantValue value) {
    ByteBuffer buffer = ByteBuffer.allocate(value.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    value.writeTo(buffer, 0);
    return buffer;
  }

  private static ByteBuffer serialize(VariantMetadata metadata) {
    ByteBuffer buffer = ByteBuffer.allocate(metadata.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(buffer, 0);
    return buffer;
  }

  /** Creates an Avro record from a map of field name to value. */
  private static GenericRecord record(GroupType type, Map<String, Object> fields) {
    GenericRecord record = new GenericData.Record(avroSchema(type));
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    return record;
  }

  /**
   * This is a custom Parquet writer builder that injects a specific Parquet schema and then uses
   * the Avro object model. This ensures that the Parquet file's schema is exactly what was passed.
   */
  private static class TestWriterBuilder
      extends ParquetWriter.Builder<GenericRecord, TestWriterBuilder> {
    private MessageType parquetSchema = null;

    protected TestWriterBuilder(OutputFile outputFile) {
      super(ParquetIO.file(outputFile));
    }

    TestWriterBuilder withFileType(MessageType schema) {
      this.parquetSchema = schema;
      return self();
    }

    @Override
    protected TestWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<GenericRecord> getWriteSupport(Configuration conf) {
      return new AvroWriteSupport<>(parquetSchema, avroSchema(parquetSchema), GenericData.get());
    }
  }

  static Record writeAndRead(MessageType parquetSchema, GenericRecord record) throws IOException {
    return Iterables.getOnlyElement(writeAndRead(parquetSchema, List.of(record)));
  }

  static List<Record> writeAndRead(MessageType parquetSchema, List<GenericRecord> records)
      throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    try (ParquetWriter<GenericRecord> writer =
        new TestWriterBuilder(outputFile).withFileType(parquetSchema).withConf(CONF).build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }

    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static MessageType parquetSchema(Type variantType) {
    return Types.buildMessage()
        .required(PrimitiveTypeName.INT32)
        .id(1)
        .named("id")
        .addField(variantType)
        .named("table");
  }

  private static GroupType variant(String name, int fieldId) {
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static void checkShreddedType(Type shreddedType) {
    Preconditions.checkArgument(
        shreddedType.getName().equals("typed_value"),
        "Invalid shredded type name: %s should be typed_value",
        shreddedType.getName());
    Preconditions.checkArgument(
        shreddedType.isRepetition(Type.Repetition.OPTIONAL),
        "Invalid shredded type repetition: %s should be OPTIONAL",
        shreddedType.getRepetition());
  }

  private static Type shreddedPrimitive(PrimitiveTypeName primitive) {
    return Types.optional(primitive).named("typed_value");
  }

  private static Type shreddedPrimitive(
      PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return Types.optional(primitive).as(annotation).named("typed_value");
  }

  private static Type shreddedType(VariantValue value) {
    switch (value.type()) {
      case BOOLEAN_TRUE:
      case BOOLEAN_FALSE:
        return shreddedPrimitive(PrimitiveTypeName.BOOLEAN);
      case INT8:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8));
      case INT16:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16));
      case INT32:
        return shreddedPrimitive(PrimitiveTypeName.INT32);
      case INT64:
        return shreddedPrimitive(PrimitiveTypeName.INT64);
      case FLOAT:
        return shreddedPrimitive(PrimitiveTypeName.FLOAT);
      case DOUBLE:
        return shreddedPrimitive(PrimitiveTypeName.DOUBLE);
      case DECIMAL4:
        BigDecimal decimal4 = (BigDecimal) value.asPrimitive().get();
        return shreddedPrimitive(
            PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(decimal4.scale(), 9));
      case DECIMAL8:
        BigDecimal decimal8 = (BigDecimal) value.asPrimitive().get();
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.decimalType(decimal8.scale(), 18));
      case DECIMAL16:
        BigDecimal decimal16 = (BigDecimal) value.asPrimitive().get();
        return shreddedPrimitive(
            PrimitiveTypeName.BINARY, LogicalTypeAnnotation.decimalType(decimal16.scale(), 38));
      case DATE:
        return shreddedPrimitive(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
      case TIMESTAMPTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS));
      case TIMESTAMPNTZ:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS));
      case BINARY:
        return shreddedPrimitive(PrimitiveTypeName.BINARY);
      case STRING:
        return shreddedPrimitive(PrimitiveTypeName.BINARY, STRING);
      case TIME:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS));
      case TIMESTAMPTZ_NANOS:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS));
      case TIMESTAMPNTZ_NANOS:
        return shreddedPrimitive(
            PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS));
      case UUID:
        return shreddedPrimitive(
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType());
    }

    throw new UnsupportedOperationException("Unsupported shredding type: " + value.type());
  }

  private static Object toAvroValue(VariantPrimitive<?> variant) {
    switch (variant.type()) {
      case DECIMAL4:
        return ((BigDecimal) variant.get()).unscaledValue().intValueExact();
      case DECIMAL8:
        return ((BigDecimal) variant.get()).unscaledValue().longValueExact();
      case DECIMAL16:
        return ((BigDecimal) variant.get()).unscaledValue().toByteArray();
      default:
        return variant.get();
    }
  }

  private static GroupType variant(String name, int fieldId, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .id(fieldId)
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static void checkField(GroupType fieldType) {
    Preconditions.checkArgument(
        fieldType.isRepetition(Type.Repetition.REQUIRED),
        "Invalid field type repetition: %s should be REQUIRED",
        fieldType.getRepetition());
  }

  private static GroupType objectFields(GroupType... fields) {
    for (GroupType fieldType : fields) {
      checkField(fieldType);
    }

    return Types.buildGroup(Type.Repetition.OPTIONAL).addFields(fields).named("typed_value");
  }

  private static GroupType field(String name, Type shreddedType) {
    checkShreddedType(shreddedType);
    return Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveTypeName.BINARY)
        .named("value")
        .addField(shreddedType)
        .named(name);
  }

  private static GroupType element(Type shreddedType) {
    return field("element", shreddedType);
  }

  private static GroupType list(GroupType elementType) {
    return Types.optionalList().element(elementType).named("typed_value");
  }

  private static void checkListType(GroupType listType) {
    // Check the list is a 3-level structure
    Preconditions.checkArgument(
        listType.getFieldCount() == 1
            && listType.getFields().get(0).isRepetition(Type.Repetition.REPEATED),
        "Invalid list type: does not contain single repeated field: %s",
        listType);

    GroupType repeated = listType.getFields().get(0).asGroupType();
    Preconditions.checkArgument(
        repeated.getFieldCount() == 1
            && repeated.getFields().get(0).isRepetition(Type.Repetition.REQUIRED),
        "Invalid list type: does not contain single required subfield: %s",
        listType);
  }

  private static org.apache.avro.Schema avroSchema(GroupType schema) {
    if (schema instanceof MessageType) {
      return new AvroSchemaConverter(CONF).convert((MessageType) schema);

    } else {
      MessageType wrapped = Types.buildMessage().addField(schema).named("table");
      org.apache.avro.Schema avro =
          new AvroSchemaConverter(CONF).convert(wrapped).getFields().get(0).schema();
      switch (avro.getType()) {
        case RECORD:
          return avro;
        case UNION:
          return avro.getTypes().get(1);
      }

      throw new IllegalArgumentException("Invalid converted type: " + avro);
    }
  }
}
