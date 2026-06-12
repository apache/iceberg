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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.VariantReaderBuilder;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class TestSparkVariantExtractionReaders {

  @Test
  void selectiveReaderReadsFewerColumnsThanFullVariantReader() {
    GroupType cityField = shreddedStringField("city");
    GroupType zipField = shreddedStringField("zip");
    GroupType sizeField = shreddedLongField("size");
    GroupType variantGroup = shreddedObjectVariant("v", cityField, zipField, sizeField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    StructType extractionStruct =
        new StructType(new StructField[] {extractionField(0, "$.size", DataTypes.LongType)});

    ParquetValueReader<?> fullReader =
        ParquetVariantVisitor.visit(
            variantGroup, new VariantReaderBuilder(fileSchema, List.of("v")));
    ParquetValueReader<?> selectiveReader =
        SparkVariantExtractionReaders.buildStructReader(
            fileSchema, variantGroup, List.of("v"), extractionStruct);

    int fullColumns = ParquetVariantExtractionReaders.leafColumnCount(fullReader);
    int selectiveColumns = ParquetVariantExtractionReaders.leafColumnCount(selectiveReader);

    assertThat(selectiveColumns)
        .as("selective reader should read fewer Parquet columns than the full variant")
        .isLessThan(fullColumns);
    assertThat(selectiveColumns)
        .as("selective reader should not include unrelated shredded fields (city, zip)")
        .isLessThan(fullColumns - 2);
  }

  @Test
  void multipleExtractionsUnionColumnPaths() {
    GroupType cityField = shreddedStringField("city");
    GroupType zipField = shreddedStringField("zip");
    GroupType sizeField = shreddedLongField("size");
    GroupType variantGroup = shreddedObjectVariant("v", cityField, zipField, sizeField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    StructType extractionStruct =
        new StructType(
            new StructField[] {
              extractionField(0, "$.city", DataTypes.StringType),
              extractionField(1, "$.zip", DataTypes.StringType)
            });

    ParquetValueReader<?> cityOnlyReader =
        SparkVariantExtractionReaders.buildStructReader(
            fileSchema,
            variantGroup,
            List.of("v"),
            new StructType(new StructField[] {extractionField(0, "$.city", DataTypes.StringType)}));
    ParquetValueReader<?> bothReader =
        SparkVariantExtractionReaders.buildStructReader(
            fileSchema, variantGroup, List.of("v"), extractionStruct);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(bothReader))
        .as("two extractions should read more columns than one")
        .isGreaterThan(ParquetVariantExtractionReaders.leafColumnCount(cityOnlyReader));
    assertThat(ParquetVariantExtractionReaders.leafColumnCount(bothReader))
        .as("two extractions should still read fewer columns than the full variant")
        .isLessThan(
            ParquetVariantExtractionReaders.leafColumnCount(
                ParquetVariantVisitor.visit(
                    variantGroup, new VariantReaderBuilder(fileSchema, List.of("v")))));
  }

  @Test
  void unshreddedVariantUsesMetadataAndValueColumnsOnly() {
    GroupType variantGroup = unshreddedVariant("v");
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    StructType extractionStruct =
        new StructType(new StructField[] {extractionField(0, "$.size", DataTypes.LongType)});

    ParquetValueReader<?> selectiveReader =
        SparkVariantExtractionReaders.buildStructReader(
            fileSchema, variantGroup, List.of("v"), extractionStruct);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(selectiveReader))
        .as("unshredded fallback reads metadata and root value only")
        .isEqualTo(2);
  }

  @Test
  void placeholderReadsMetadataOnly() {
    GroupType cityField = shreddedStringField("city");
    GroupType variantGroup = shreddedObjectVariant("v", cityField);
    MessageType fileSchema =
        org.apache.parquet.schema.Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .addField(variantGroup)
            .named("table");

    StructType extractionStruct =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(
                  "0",
                  DataTypes.BooleanType,
                  true,
                  new MetadataBuilder()
                      .putMetadata(
                          SparkVariantExtractionUtil.VARIANT_METADATA_KEY,
                          new MetadataBuilder()
                              .putString("path", SparkVariantExtractionUtil.PLACEHOLDER_PATH)
                              .build())
                      .build())
            });

    ParquetValueReader<?> reader =
        SparkVariantExtractionReaders.buildStructReader(
            fileSchema, variantGroup, List.of("v"), extractionStruct);

    assertThat(ParquetVariantExtractionReaders.leafColumnCount(reader)).isEqualTo(1);
  }

  @Test
  void dateValueCastsToSparkDate() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.ofDate(12_345), DataTypes.DateType))
        .isEqualTo(12_345);
  }

  @Test
  void timestamptzValueCastsToSparkTimestamp() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.ofTimestamptz(1_000_000L), DataTypes.TimestampType))
        .isEqualTo(1_000_000L);
  }

  @Test
  void timestampntzValueCastsToSparkTimestampNtz() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.ofTimestampntz(2_000_000L), DataTypes.TimestampNTZType))
        .isEqualTo(2_000_000L);
  }

  @Test
  void timestamptzNanosValueCastsToSparkTimestampMicros() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.ofTimestamptzNanos(5_000_000L), DataTypes.TimestampType))
        .isEqualTo(5_000L);
  }

  @Test
  void binaryValueCastsToSparkBinary() {
    byte[] payload = new byte[] {1, 2, 3};
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(ByteBuffer.wrap(payload)), DataTypes.BinaryType))
        .isEqualTo(payload);
  }

  @Test
  void byteValueCastsToSparkByte() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of((byte) 127), DataTypes.ByteType))
        .isEqualTo((byte) 127);
  }

  @Test
  void shortValueCastsToSparkShort() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of((short) 42), DataTypes.ShortType))
        .isEqualTo((short) 42);
  }

  @Test
  void int64OverflowToIntReturnsNull() {
    long overflowValue = (long) Integer.MAX_VALUE + 1;
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(overflowValue), DataTypes.IntegerType))
        .isNull();
  }

  @Test
  void int64OverflowToShortReturnsNull() {
    long overflowValue = (long) Short.MAX_VALUE + 1;
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(overflowValue), DataTypes.ShortType))
        .isNull();
  }

  @Test
  void int64OverflowToByteReturnsNull() {
    long overflowValue = (long) Byte.MAX_VALUE + 1;
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(overflowValue), DataTypes.ByteType))
        .isNull();
  }

  @Test
  void int32OverflowToShortReturnsNull() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of((int) Short.MAX_VALUE + 1), DataTypes.ShortType))
        .isNull();
  }

  @Test
  void int32OverflowToByteReturnsNull() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of((int) Byte.MAX_VALUE + 1), DataTypes.ByteType))
        .isNull();
  }

  @Test
  void doubleOutOfFloatRangeReturnsNull() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(Double.MAX_VALUE), DataTypes.FloatType))
        .isNull();
  }

  @Test
  void doubleInFloatRangeCastsToFloat() {
    assertThat(
            SparkVariantExtractionReaders.toSparkValueForTests(
                Variants.of(1.5d), DataTypes.FloatType))
        .isEqualTo(1.5f);
  }

  private static StructField extractionField(
      int ordinal, String path, org.apache.spark.sql.types.DataType type) {
    Metadata variantMetadata =
        new MetadataBuilder()
            .putString("path", path)
            .putBoolean("failOnError", false)
            .putString("timeZoneId", "UTC")
            .build();
    Metadata metadata =
        new MetadataBuilder()
            .putMetadata(SparkVariantExtractionUtil.VARIANT_METADATA_KEY, variantMetadata)
            .build();
    return DataTypes.createStructField(String.valueOf(ordinal), type, true, metadata);
  }

  private static GroupType shreddedObjectVariant(String name, GroupType... objectFields) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optionalGroup()
        .addFields(objectFields)
        .named("typed_value")
        .named(name);
  }

  private static GroupType unshreddedVariant(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named(name);
  }

  private static GroupType shreddedStringField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named("typed_value")
        .named(name);
  }

  private static GroupType shreddedLongField(String name) {
    return org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REQUIRED)
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .optional(PrimitiveType.PrimitiveTypeName.INT64)
        .named("typed_value")
        .named(name);
  }
}
