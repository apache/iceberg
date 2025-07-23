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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.InternalTestHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestVariantWriters {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "var", Types.VariantType.get()));

  private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);

  private static final ByteBuffer TEST_METADATA_BUFFER =
      VariantTestUtil.createMetadata(ImmutableList.of("a", "b", "c", "d", "e"), true);
  private static final ByteBuffer TEST_OBJECT_BUFFER =
      VariantTestUtil.createObject(
          TEST_METADATA_BUFFER,
          ImmutableMap.of(
              "a", Variants.ofNull(),
              "d", Variants.of("iceberg")));
  private static final ByteBuffer SIMILAR_OBJECT_BUFFER =
      VariantTestUtil.createObject(
          TEST_METADATA_BUFFER,
          ImmutableMap.of(
              "a", Variants.of(123456789),
              "c", Variants.of("string")));
  private static final ByteBuffer EMPTY_OBJECT_BUFFER =
      VariantTestUtil.createObject(TEST_METADATA_BUFFER, ImmutableMap.of());
  private static final ByteBuffer ARRAY_IN_OBJECT_BUFFER =
      VariantTestUtil.createObject(
          TEST_METADATA_BUFFER,
          ImmutableMap.of(
              "a", Variants.of(123456789),
              "c", array(Variants.of("string"), Variants.of("iceberg"))));

  private static final VariantMetadata EMPTY_METADATA =
      Variants.metadata(VariantTestUtil.emptyMetadata());
  private static final VariantMetadata TEST_METADATA = Variants.metadata(TEST_METADATA_BUFFER);
  private static final VariantObject TEST_OBJECT =
      (VariantObject) Variants.value(TEST_METADATA, TEST_OBJECT_BUFFER);
  private static final VariantObject SIMILAR_OBJECT =
      (VariantObject) Variants.value(TEST_METADATA, SIMILAR_OBJECT_BUFFER);
  private static final VariantObject EMPTY_OBJECT =
      (VariantObject) Variants.value(TEST_METADATA, EMPTY_OBJECT_BUFFER);
  private static final VariantObject ARRAY_IN_OBJECT =
      (VariantObject) Variants.value(TEST_METADATA, ARRAY_IN_OBJECT_BUFFER);

  private static final ByteBuffer EMPTY_ARRAY_BUFFER = VariantTestUtil.createArray();
  private static final ByteBuffer TEST_ARRAY_BUFFER =
      VariantTestUtil.createArray(Variants.of("iceberg"), Variants.of("string"));
  private static final ByteBuffer MIXED_TYPE_ARRAY_BUFFER =
      VariantTestUtil.createArray(Variants.of("iceberg"), Variants.of("string"), Variants.of(34));
  private static final ByteBuffer NESTED_ARRAY_BUFFER =
      VariantTestUtil.createArray(
          array(Variants.of("string"), Variants.of("iceberg")),
          array(Variants.of("apple"), Variants.of("banana")));
  private static final ByteBuffer MIXED_NESTED_ARRAY_BUFFER =
      VariantTestUtil.createArray(
          array(Variants.of("string"), Variants.of("iceberg"), Variants.of(34)),
          array(Variants.of(34), Variants.ofNull()),
          array(),
          array(Variants.of("string"), Variants.of("iceberg")),
          Variants.of(34));
  private static final ByteBuffer OBJECT_IN_ARRAY_BUFFER =
      VariantTestUtil.createArray(SIMILAR_OBJECT, SIMILAR_OBJECT);
  private static final ByteBuffer MIXED_OBJECT_IN_ARRAY_BUFFER =
      VariantTestUtil.createArray(
          SIMILAR_OBJECT, SIMILAR_OBJECT, Variants.of("iceberg"), Variants.of(34));

  private static final VariantArray EMPTY_ARRAY =
      (VariantArray) Variants.value(EMPTY_METADATA, EMPTY_ARRAY_BUFFER);
  private static final VariantArray TEST_ARRAY =
      (VariantArray) Variants.value(EMPTY_METADATA, TEST_ARRAY_BUFFER);
  private static final VariantArray MIXED_TYPE_ARRAY =
      (VariantArray) Variants.value(EMPTY_METADATA, MIXED_TYPE_ARRAY_BUFFER);
  private static final VariantArray NESTED_ARRAY =
      (VariantArray) Variants.value(EMPTY_METADATA, NESTED_ARRAY_BUFFER);
  private static final VariantArray MIXED_NESTED_ARRAY =
      (VariantArray) Variants.value(EMPTY_METADATA, MIXED_NESTED_ARRAY_BUFFER);
  private static final VariantArray OBJECT_IN_ARRAY =
      (VariantArray) Variants.value(TEST_METADATA, OBJECT_IN_ARRAY_BUFFER);
  private static final VariantArray MIXED_OBJECT_IN_ARRAY =
      (VariantArray) Variants.value(TEST_METADATA, MIXED_OBJECT_IN_ARRAY_BUFFER);

  private static final Variant[] VARIANTS =
      new Variant[] {
        Variant.of(EMPTY_METADATA, Variants.ofNull()),
        Variant.of(EMPTY_METADATA, Variants.of(true)),
        Variant.of(EMPTY_METADATA, Variants.of(false)),
        Variant.of(EMPTY_METADATA, Variants.of((byte) 34)),
        Variant.of(EMPTY_METADATA, Variants.of((byte) -34)),
        Variant.of(EMPTY_METADATA, Variants.of((short) 1234)),
        Variant.of(EMPTY_METADATA, Variants.of((short) -1234)),
        Variant.of(EMPTY_METADATA, Variants.of(12345)),
        Variant.of(EMPTY_METADATA, Variants.of(-12345)),
        Variant.of(EMPTY_METADATA, Variants.of(9876543210L)),
        Variant.of(EMPTY_METADATA, Variants.of(-9876543210L)),
        Variant.of(EMPTY_METADATA, Variants.of(10.11F)),
        Variant.of(EMPTY_METADATA, Variants.of(-10.11F)),
        Variant.of(EMPTY_METADATA, Variants.of(14.3D)),
        Variant.of(EMPTY_METADATA, Variants.of(-14.3D)),
        Variant.of(EMPTY_METADATA, EMPTY_OBJECT),
        Variant.of(TEST_METADATA, TEST_OBJECT),
        Variant.of(TEST_METADATA, SIMILAR_OBJECT),
        Variant.of(TEST_METADATA, ARRAY_IN_OBJECT),
        Variant.of(EMPTY_METADATA, EMPTY_ARRAY),
        Variant.of(EMPTY_METADATA, TEST_ARRAY),
        Variant.of(EMPTY_METADATA, MIXED_TYPE_ARRAY),
        Variant.of(EMPTY_METADATA, NESTED_ARRAY),
        Variant.of(EMPTY_METADATA, MIXED_NESTED_ARRAY),
        Variant.of(TEST_METADATA, OBJECT_IN_ARRAY),
        Variant.of(TEST_METADATA, MIXED_OBJECT_IN_ARRAY),
        Variant.of(EMPTY_METADATA, Variants.ofIsoDate("2024-11-07")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoDate("1957-11-07")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("2024-11-07T12:33:54.123456+00:00")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoTimestamptz("1957-11-07T12:33:54.123456+00:00")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("2024-11-07T12:33:54.123456")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoTimestampntz("1957-11-07T12:33:54.123456")),
        Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("123456.789"))), // decimal4
        Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("-123456.789"))), // decimal4
        Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("123456789.987654321"))), // decimal8
        Variant.of(EMPTY_METADATA, Variants.of(new BigDecimal("-123456789.987654321"))), // decimal8
        Variant.of(
            EMPTY_METADATA, Variants.of(new BigDecimal("9876543210.123456789"))), // decimal16
        Variant.of(
            EMPTY_METADATA, Variants.of(new BigDecimal("-9876543210.123456789"))), // decimal16
        Variant.of(
            EMPTY_METADATA, Variants.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0x0c, 0x0d}))),
        Variant.of(EMPTY_METADATA, Variants.of("iceberg")),
        Variant.of(EMPTY_METADATA, Variants.ofIsoTime("12:33:54.123456")),
        Variant.of(
            EMPTY_METADATA, Variants.ofIsoTimestamptzNanos("2024-11-07T12:33:54.123456789+00:00")),
        Variant.of(
            EMPTY_METADATA, Variants.ofIsoTimestamptzNanos("1957-11-07T12:33:54.123456789+00:00")),
        Variant.of(
            EMPTY_METADATA, Variants.ofIsoTimestampntzNanos("2024-11-07T12:33:54.123456789")),
        Variant.of(
            EMPTY_METADATA, Variants.ofIsoTimestampntzNanos("1957-11-07T12:33:54.123456789")),
        Variant.of(EMPTY_METADATA, Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")),
      };

  @ParameterizedTest
  @FieldSource("VARIANTS")
  public void testUnshreddedValues(Variant variant) throws IOException {
    Record record = RECORD.copy("id", 1, "var", variant);

    Record actual = writeAndRead((id, name) -> null, record);

    InternalTestHelpers.assertEquals(SCHEMA.asStruct(), record, actual);
  }

  @ParameterizedTest
  @FieldSource("VARIANTS")
  public void testShreddedValues(Variant variant) throws IOException {
    Record record = RECORD.copy("id", 1, "var", variant);

    Record actual =
        writeAndRead((id, name) -> ParquetVariantUtil.toParquetSchema(variant.value()), record);

    InternalTestHelpers.assertEquals(SCHEMA.asStruct(), record, actual);
  }

  @ParameterizedTest
  @FieldSource("VARIANTS")
  public void testMixedShredding(Variant variant) throws IOException {
    List<Record> expected =
        IntStream.range(0, VARIANTS.length)
            .mapToObj(i -> RECORD.copy("id", i, "var", VARIANTS[i]))
            .collect(Collectors.toList());

    List<Record> actual =
        writeAndRead((id, name) -> ParquetVariantUtil.toParquetSchema(variant.value()), expected);

    assertThat(actual).hasSameSizeAs(expected);

    for (int i = 0; i < expected.size(); i += 1) {
      InternalTestHelpers.assertEquals(SCHEMA.asStruct(), expected.get(i), actual.get(i));
    }
  }

  private static Record writeAndRead(VariantShreddingFunction shreddingFunc, Record record)
      throws IOException {
    return Iterables.getOnlyElement(writeAndRead(shreddingFunc, List.of(record)));
  }

  private static List<Record> writeAndRead(
      VariantShreddingFunction shreddingFunc, List<Record> records) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .variantShreddingFunc(shreddingFunc)
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build()) {
      for (Record record : records) {
        writer.add(record);
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

  private static ValueArray array(VariantValue... values) {
    ValueArray arr = Variants.array();
    for (VariantValue value : values) {
      arr.add(value);
    }

    return arr;
  }
}
