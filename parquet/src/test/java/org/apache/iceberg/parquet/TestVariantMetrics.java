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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestVariantMetrics {
  private static final VariantMetadata METADATA =
      VariantMetadata.from(VariantTestUtil.createMetadata(Set.of("a", "b", "c", "d", "e"), true));

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "var", Types.VariantType.get()));

  private static final VariantMetadata EMPTY = Variants.emptyMetadata();

  private static final String ROOT_FIELD = "$";

  private static final VariantValue[] PRIMITIVES =
      new VariantValue[] {
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
        Variants.of(new BigDecimal("123456.789")), // decimal4
        Variants.of(new BigDecimal("-123456.789")), // decimal4
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

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testShreddedPrimitiveTypes(VariantValue value) throws IOException {
    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(value),
            Variant.of(EMPTY, value),
            Variant.of(EMPTY, Variants.ofNull()),
            null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 3L, 2, 3L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds()).hasSize(2);
    assertThat(metrics.lowerBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(0L);
    assertThat(metrics.lowerBounds().get(2))
        .extracting(
            bytes -> {
              VariantObject bounds = Variant.from(bytes).value().asObject();
              return bounds.get(ROOT_FIELD);
            })
        .isEqualTo(value);

    assertThat(metrics.upperBounds()).hasSize(2);
    assertThat(metrics.upperBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(2L);
    assertThat(metrics.upperBounds().get(2))
        .extracting(
            bytes -> {
              VariantObject bounds = Variant.from(bytes).value().asObject();
              return bounds.get(ROOT_FIELD);
            })
        .isEqualTo(value);

    assertThat(metrics)
        .extracting("originalTypes")
        .isEqualTo(Map.of(1, Types.LongType.get(), 2, Types.VariantType.get()));
  }

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testShreddedPrimitiveRange(VariantValue value) throws IOException {
    VariantValue largerValue = increment(value);
    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(value),
            Variant.of(EMPTY, largerValue),
            Variant.of(EMPTY, Variants.ofNull()),
            null,
            Variant.of(EMPTY, value));

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 4L, 2, 4L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds()).hasSize(2);
    assertThat(metrics.lowerBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(0L);
    assertThat(metrics.lowerBounds().get(2))
        .extracting(
            bytes -> {
              VariantObject bounds = Variant.from(bytes).value().asObject();
              return bounds.get(ROOT_FIELD);
            })
        .isEqualTo(value);

    assertThat(metrics.upperBounds()).hasSize(2);
    assertThat(metrics.upperBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(3L);
    assertThat(metrics.upperBounds().get(2))
        .extracting(
            bytes -> {
              VariantObject bounds = Variant.from(bytes).value().asObject();
              return bounds.get(ROOT_FIELD);
            })
        .isEqualTo(largerValue);

    assertThat(metrics)
        .extracting("originalTypes")
        .isEqualTo(Map.of(1, Types.LongType.get(), 2, Types.VariantType.get()));
  }

  @ParameterizedTest
  @FieldSource("PRIMITIVES")
  public void testShreddedPrimitiveTypeMismatch(VariantValue value) throws IOException {
    // a value of another type will cause lower/upper bounds to be omitted
    VariantValue otherValue =
        value.type() == PhysicalType.STRING ? Variants.of(34) : Variants.of("iceberg");

    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(value),
            Variant.of(EMPTY, value),
            Variant.of(EMPTY, otherValue));

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 2L, 2, 2L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 0L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 0L)));
    assertThat(metrics.upperBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 1L)));

    assertThat(metrics).extracting("originalTypes").isEqualTo(Map.of(1, Types.LongType.get()));
  }

  @Test
  public void testVariantFloatNaN() throws IOException {
    // NaN values are not counted because there is no ID for FieldMetrics
    VariantValue floatValue = Variants.of(1.0F);
    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(floatValue),
            Variant.of(EMPTY, floatValue),
            Variant.of(EMPTY, Variants.of(Float.NaN)));

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 2L, 2, 2L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 0L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 0L)));
    assertThat(metrics.upperBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 1L)));

    assertThat(metrics).extracting("originalTypes").isEqualTo(Map.of(1, Types.LongType.get()));
  }

  @Test
  public void testVariantDoubleNaN() throws IOException {
    // NaN values are not counted because there is no ID for FieldMetrics
    VariantValue doubleValue = Variants.of(1.0D);
    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(doubleValue),
            Variant.of(EMPTY, doubleValue),
            Variant.of(EMPTY, Variants.of(Double.NaN)));

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 2L, 2, 2L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 0L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 0L)));
    assertThat(metrics.upperBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 1L)));

    assertThat(metrics).extracting("originalTypes").isEqualTo(Map.of(1, Types.LongType.get()));
  }

  @Test
  public void testAllNull() throws IOException {
    ShreddedObject object = Variants.object(METADATA);
    object.put("a", Variants.ofIsoDate("2025-03-17"));
    object.put("b", Variants.of(34));
    object.put("c", Variants.of("iceberg"));
    Metrics metrics =
        writeParquet((id, name) -> ParquetVariantUtil.toParquetSchema(object), null, null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 2L, 2, 2L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 2L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 0L)));
    assertThat(metrics.upperBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 1L)));

    assertThat(metrics).extracting("originalTypes").isEqualTo(Map.of(1, Types.LongType.get()));
  }

  @Test
  public void testUnshredded() throws IOException {
    ShreddedObject object = Variants.object(METADATA);
    object.put("a", Variants.ofIsoDate("2025-03-17"));
    object.put("b", Variants.of(34));
    object.put("c", Variants.of("iceberg"));
    Metrics metrics = writeParquet((id, name) -> null, Variant.of(METADATA, object), null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 2L, 2, 2L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    assertThat(metrics.lowerBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 0L)));
    assertThat(metrics.upperBounds())
        .isEqualTo(Map.of(1, Conversions.toByteBuffer(Type.TypeID.LONG, 1L)));

    assertThat(metrics).extracting("originalTypes").isEqualTo(Map.of(1, Types.LongType.get()));
  }

  @Test
  public void testShreddedObject() throws IOException {
    VariantValue date = Variants.ofIsoDate("2025-03-17");
    VariantValue num = Variants.of(34);
    VariantValue str = Variants.of("iceberg");
    VariantValue dec = Variants.of(new BigDecimal("123456.789"));

    ShreddedObject inner = Variants.object(METADATA);
    inner.put("e", dec);
    ShreddedObject object = Variants.object(METADATA);
    object.put("a", date);
    object.put("b", num);
    object.put("c", str);
    object.put("d", inner);

    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(object),
            Variant.of(EMPTY, num),
            Variant.of(EMPTY, Variants.object(EMPTY)),
            Variant.of(METADATA, object),
            Variant.of(EMPTY, Variants.ofNull()),
            null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 5L, 2, 5L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    VariantMetadata boundMetadata = Variants.metadata("$['a']", "$['b']", "$['c']", "$['d']['e']");
    ShreddedObject expectedBounds = Variants.object(boundMetadata);
    expectedBounds.put("$['a']", date);
    expectedBounds.put("$['b']", num);
    expectedBounds.put("$['c']", str);
    expectedBounds.put("$['d']['e']", dec);

    assertThat(metrics.lowerBounds()).hasSize(2);
    assertThat(metrics.lowerBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(0L);
    assertThat(metrics.lowerBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics.upperBounds()).hasSize(2);
    assertThat(metrics.upperBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(4L);
    assertThat(metrics.upperBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics)
        .extracting("originalTypes")
        .isEqualTo(Map.of(1, Types.LongType.get(), 2, Types.VariantType.get()));
  }

  @Test
  public void testPartiallyShreddedObject() throws IOException {
    VariantValue date = Variants.ofIsoDate("2025-03-17");
    VariantValue num = Variants.of(34);
    VariantValue str = Variants.of("iceberg");
    VariantValue dec = Variants.of(new BigDecimal("123456.789"));

    ShreddedObject inner = Variants.object(METADATA);
    inner.put("e", dec);
    ShreddedObject object = Variants.object(METADATA);
    object.put("a", date);
    object.put("b", num);
    object.put("c", str);
    object.put("d", inner);

    // used to produce the shredding schema
    ShreddedObject example = Variants.object(METADATA);
    example.put("a", date);
    example.put("b", num);

    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(example),
            Variant.of(EMPTY, num),
            Variant.of(EMPTY, Variants.object(EMPTY)),
            Variant.of(METADATA, object),
            Variant.of(EMPTY, Variants.ofNull()),
            null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 5L, 2, 5L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    // only a and b were shredded so the other fields are not present
    VariantMetadata boundMetadata = Variants.metadata("$['a']", "$['b']");
    ShreddedObject expectedBounds = Variants.object(boundMetadata);
    expectedBounds.put("$['a']", date);
    expectedBounds.put("$['b']", num);

    assertThat(metrics.lowerBounds()).hasSize(2);
    assertThat(metrics.lowerBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(0L);
    assertThat(metrics.lowerBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics.upperBounds()).hasSize(2);
    assertThat(metrics.upperBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(4L);
    assertThat(metrics.upperBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics)
        .extracting("originalTypes")
        .isEqualTo(Map.of(1, Types.LongType.get(), 2, Types.VariantType.get()));
  }

  @Test
  public void testShreddedObjectFieldTypeMismatch() throws IOException {
    VariantValue date = Variants.ofIsoDate("2025-03-17");
    VariantValue num = Variants.of(34);
    VariantValue str = Variants.of("iceberg");
    VariantValue dec = Variants.of(new BigDecimal("123456.789"));

    ShreddedObject inner = Variants.object(METADATA);
    inner.put("e", dec);
    ShreddedObject object = Variants.object(METADATA);
    object.put("a", date);
    object.put("b", num);
    object.put("c", str);
    object.put("d", inner);

    ShreddedObject mismatched = Variants.object(METADATA);
    mismatched.put("a", Variants.ofNull()); // does not affect metrics
    mismatched.put("b", Variants.of((byte) -1)); // int and byte mismatch
    mismatched.put("c", num); // string and int mismatch
    // d is missing and does not affect metrics

    Metrics metrics =
        writeParquet(
            (id, name) -> ParquetVariantUtil.toParquetSchema(object),
            Variant.of(EMPTY, num),
            Variant.of(EMPTY, mismatched),
            Variant.of(EMPTY, Variants.object(EMPTY)),
            Variant.of(METADATA, object),
            Variant.of(EMPTY, Variants.ofNull()),
            null);

    assertThat(metrics.valueCounts()).isEqualTo(Map.of(1, 6L, 2, 6L));
    assertThat(metrics.nullValueCounts()).isEqualTo(Map.of(1, 0L, 2, 1L));
    assertThat(metrics.nanValueCounts()).isEqualTo(Map.of());

    // only a and b were shredded so the other fields are not present
    VariantMetadata boundMetadata = Variants.metadata("$['a']", "$['d']['e']");
    ShreddedObject expectedBounds = Variants.object(boundMetadata);
    expectedBounds.put("$['a']", date);
    expectedBounds.put("$['d']['e']", dec);

    assertThat(metrics.lowerBounds()).hasSize(2);
    assertThat(metrics.lowerBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(0L);
    assertThat(metrics.lowerBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics.upperBounds()).hasSize(2);
    assertThat(metrics.upperBounds().get(1))
        .extracting(bytes -> Conversions.fromByteBuffer(Types.LongType.get(), bytes))
        .isEqualTo(5L);
    assertThat(metrics.upperBounds().get(2))
        .extracting(bytes -> Variant.from(bytes).value())
        .isEqualTo(expectedBounds);

    assertThat(metrics)
        .extracting("originalTypes")
        .isEqualTo(Map.of(1, Types.LongType.get(), 2, Types.VariantType.get()));
  }

  private Metrics writeParquet(VariantShreddingFunction shredding, Variant... variants)
      throws IOException {
    OutputFile out = new InMemoryOutputFile();
    GenericRecord record = GenericRecord.create(SCHEMA);

    FileAppender<Record> writer =
        Parquet.write(out)
            .schema(SCHEMA)
            .variantShreddingFunc(shredding)
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build();

    try (writer) {
      for (int id = 0; id < variants.length; id += 1) {
        record.setField("id", (long) id);
        record.setField("var", variants[id]);
        writer.add(record);
      }
    }

    return writer.metrics();
  }

  private static VariantValue increment(VariantValue value) {
    VariantPrimitive<?> primitive = value.asPrimitive();
    switch (value.type()) {
      case BOOLEAN_TRUE:
      case BOOLEAN_FALSE:
        return Variants.of(true);
      case INT8:
        return Variants.of(value.type(), (byte) ((Byte) primitive.get() + 1));
      case INT16:
        return Variants.of(value.type(), (short) ((Short) primitive.get() + 1));
      case INT32:
      case DATE:
        return Variants.of(value.type(), (Integer) primitive.get() + 1);
      case INT64:
      case TIMESTAMPTZ:
      case TIMESTAMPNTZ:
      case TIME:
      case TIMESTAMPTZ_NANOS:
      case TIMESTAMPNTZ_NANOS:
        return Variants.of(value.type(), (Long) primitive.get() + 1L);
      case FLOAT:
        return Variants.of(value.type(), (Float) primitive.get() + 1.0F);
      case DOUBLE:
        return Variants.of(value.type(), (Double) primitive.get() + 1.0D);
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return Variants.of(value.type(), ((BigDecimal) primitive.get()).add(BigDecimal.ONE));
      case BINARY:
        return Variants.of(
            value.type(), BinaryUtil.truncateBinaryMax((ByteBuffer) primitive.get(), 2));
      case STRING:
        return Variants.of(
            value.type(), UnicodeUtil.truncateStringMax((String) primitive.get(), 5));
      case UUID:
        UUID uuid = (UUID) primitive.get();
        return Variants.of(
            value.type(),
            new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits() + 1));
    }

    throw new UnsupportedOperationException("Cannot increment value: " + value);
  }
}
