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
package org.apache.iceberg.spark;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.MetadataAttribute;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSparkSchemaUtil {

  private static final String CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT";
  private static final String EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT";

  private static final Schema TEST_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final Schema TEST_SCHEMA_WITH_METADATA_COLS =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          MetadataColumns.FILE_PATH,
          MetadataColumns.ROW_POSITION);

  @Test
  public void testEstimateSizeMaxValue() {
    assertThat(SparkSchemaUtil.estimateSize(null, Long.MAX_VALUE))
        .as("estimateSize returns Long max value")
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testEstimateSizeWithOverflow() {
    long tableSize =
        SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), Long.MAX_VALUE - 1);
    assertThat(tableSize).as("estimateSize handles overflow").isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void testEstimateSize() {
    long tableSize = SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), 1);
    assertThat(tableSize).as("estimateSize matches with expected approximation").isEqualTo(24);
  }

  @Test
  public void testSchemaConversionWithMetaDataColumnSchema() {
    StructType structType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_METADATA_COLS);
    List<AttributeReference> attrRefs =
        scala.collection.JavaConverters.seqAsJavaList(DataTypeUtils.toAttributes(structType));
    for (AttributeReference attrRef : attrRefs) {
      if (MetadataColumns.isMetadataColumn(attrRef.name())) {
        assertThat(MetadataAttribute.unapply(attrRef).isDefined())
            .as("metadata columns should have __metadata_col in attribute metadata")
            .isTrue();
      } else {
        assertThat(MetadataAttribute.unapply(attrRef).isDefined())
            .as("non metadata columns should not have __metadata_col in attribute metadata")
            .isFalse();
      }
    }
  }

  @Test
  public void testSchemaConversionWithOnlyWriteDefault() {
    Schema schema =
        new Schema(
            Types.NestedField.optional("field")
                .withId(1)
                .ofType(Types.StringType.get())
                .withWriteDefault(Literal.of("write_only"))
                .build());

    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    Metadata metadata = sparkSchema.fields()[0].metadata();

    assertThat(metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field with only write default should have CURRENT_DEFAULT metadata")
        .isTrue();
    assertThat(metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field with only write default should not have EXISTS_DEFAULT metadata")
        .isFalse();
    assertThat(metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        .as("Spark metadata CURRENT_DEFAULT should contain correctly formatted literal")
        .isEqualTo("'write_only'");
  }

  @Test
  public void testSchemaConversionWithOnlyInitialDefault() {
    Schema schema =
        new Schema(
            Types.NestedField.optional("field")
                .withId(1)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(Literal.of(42))
                .build());

    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    Metadata metadata = sparkSchema.fields()[0].metadata();

    assertThat(metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field with only initial default should not have CURRENT_DEFAULT metadata")
        .isFalse();
    assertThat(metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field with only initial default should have EXISTS_DEFAULT metadata")
        .isTrue();
    assertThat(metadata.getString(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
        .as("Spark metadata EXISTS_DEFAULT should contain correctly formatted literal")
        .isEqualTo("42");
  }

  @ParameterizedTest(name = "{0} with writeDefault={1}, initialDefault={2}")
  @MethodSource("schemaConversionWithDefaultsTestCases")
  public void testSchemaConversionWithDefaultsForPrimitiveTypes(
      Type type,
      Literal<?> writeDefault,
      Literal<?> initialDefault,
      String expectedCurrentDefaultValue,
      String expectedExistsDefaultValue) {
    Schema schema =
        new Schema(
            Types.NestedField.optional("field")
                .withId(1)
                .ofType(type)
                .withWriteDefault(writeDefault)
                .withInitialDefault(initialDefault)
                .build());

    StructType sparkSchema = SparkSchemaUtil.convert(schema);
    StructField defaultField = sparkSchema.fields()[0];
    Metadata metadata = defaultField.metadata();

    assertThat(metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field of type %s should have CURRENT_DEFAULT metadata", type)
        .isTrue();
    assertThat(metadata.contains(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
        .as("Field of type %s should have EXISTS_DEFAULT metadata", type)
        .isTrue();
    assertThat(metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        .as(
            "Spark metadata CURRENT_DEFAULT for type %s should contain correctly formatted literal",
            type)
        .isEqualTo(expectedCurrentDefaultValue);
    assertThat(metadata.getString(EXISTS_DEFAULT_COLUMN_METADATA_KEY))
        .as(
            "Spark metadata EXISTS_DEFAULT for type %s should contain correctly formatted literal",
            type)
        .isEqualTo(expectedExistsDefaultValue);
  }

  private static Stream<Arguments> schemaConversionWithDefaultsTestCases() {
    return Stream.of(
        Arguments.of(Types.IntegerType.get(), Literal.of(1), Literal.of(2), "1", "2"),
        Arguments.of(
            Types.StringType.get(),
            Literal.of("write_default"),
            Literal.of("initial_default"),
            "'write_default'",
            "'initial_default'"),
        Arguments.of(
            Types.UUIDType.get(),
            Literal.of("f79c3e09-677c-4bbd-a479-3f349cb785e7").to(Types.UUIDType.get()),
            Literal.of("f79c3e09-677c-4bbd-a479-3f349cb685e7").to(Types.UUIDType.get()),
            "'f79c3e09-677c-4bbd-a479-3f349cb785e7'",
            "'f79c3e09-677c-4bbd-a479-3f349cb685e7'"),
        Arguments.of(Types.BooleanType.get(), Literal.of(true), Literal.of(false), "true", "false"),
        Arguments.of(Types.IntegerType.get(), Literal.of(42), Literal.of(10), "42", "10"),
        Arguments.of(Types.LongType.get(), Literal.of(100L), Literal.of(50L), "100L", "50L"),
        Arguments.of(
            Types.FloatType.get(),
            Literal.of(3.14f),
            Literal.of(1.5f),
            "CAST('3.14' AS FLOAT)",
            "CAST('1.5' AS FLOAT)"),
        Arguments.of(
            Types.DoubleType.get(), Literal.of(2.718), Literal.of(1.414), "2.718D", "1.414D"),
        Arguments.of(
            Types.DecimalType.of(10, 2),
            Literal.of(new BigDecimal("99.99")),
            Literal.of(new BigDecimal("11.11")),
            "99.99BD",
            "11.11BD"),
        Arguments.of(
            Types.DateType.get(),
            Literal.of("2024-01-01").to(Types.DateType.get()),
            Literal.of("2023-01-01").to(Types.DateType.get()),
            "DATE '2024-01-01'",
            "DATE '2023-01-01'"),
        Arguments.of(
            Types.TimestampType.withZone(),
            Literal.of("2017-11-30T10:30:07.123456+00:00").to(Types.TimestampType.withZone()),
            Literal.of("2017-11-29T10:30:07.123456+00:00").to(Types.TimestampType.withZone()),
            "TIMESTAMP '2017-11-30 02:30:07.123456'",
            "TIMESTAMP '2017-11-29 02:30:07.123456'"),
        Arguments.of(
            Types.BinaryType.get(),
            Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b})),
            Literal.of(ByteBuffer.wrap(new byte[] {0x01, 0x02})),
            "X'0A0B'",
            "X'0102'"),
        Arguments.of(
            Types.FixedType.ofLength(4),
            Literal.of("test".getBytes()),
            Literal.of("init".getBytes()),
            "X'74657374'",
            "X'696E6974'"));
  }
}
