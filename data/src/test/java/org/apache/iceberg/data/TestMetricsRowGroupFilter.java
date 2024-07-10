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
package org.apache.iceberg.data;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetricsRowGroupFilter {

  @Parameters(name = "fileFormat = {0}")
  public static List<Object> parameters() {
    return Arrays.asList(FileFormat.PARQUET, FileFormat.ORC);
  }

  private static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "int_field", IntegerType.get()));

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "no_stats_parquet", StringType.get()),
          required(3, "required", StringType.get()),
          optional(4, "all_nulls", DoubleType.get()),
          optional(5, "some_nulls", StringType.get()),
          optional(6, "no_nulls", StringType.get()),
          optional(7, "struct_not_null", structFieldType),
          optional(9, "not_in_file", FloatType.get()),
          optional(10, "str", StringType.get()),
          optional(
              11,
              "map_not_null",
              Types.MapType.ofRequired(12, 13, StringType.get(), IntegerType.get())),
          optional(14, "all_nans", DoubleType.get()),
          optional(15, "some_nans", FloatType.get()),
          optional(16, "no_nans", DoubleType.get()),
          optional(17, "some_double_nans", DoubleType.get()));

  private static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA =
      new Schema(
          required(1, "_id", IntegerType.get()),
          optional(2, "_no_stats_parquet", StringType.get()),
          required(3, "_required", StringType.get()),
          optional(4, "_all_nulls", DoubleType.get()),
          optional(5, "_some_nulls", StringType.get()),
          optional(6, "_no_nulls", StringType.get()),
          optional(7, "_struct_not_null", _structFieldType),
          optional(10, "_str", StringType.get()),
          optional(14, "_all_nans", Types.DoubleType.get()),
          optional(15, "_some_nans", FloatType.get()),
          optional(16, "_no_nans", Types.DoubleType.get()),
          optional(17, "_some_double_nans", Types.DoubleType.get()));

  private static final String TOO_LONG_FOR_STATS_PARQUET;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID());
    }
    TOO_LONG_FOR_STATS_PARQUET = sb.toString();
  }

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  private File orcFile = null;
  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;

  @Parameter private FileFormat format;
  @TempDir private File tempDir;

  @BeforeEach
  public void createInputFile() throws IOException {
    switch (format) {
      case ORC:
        createOrcInputFile();
        break;
      case PARQUET:
        createParquetInputFile();
        break;
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }
  }

  public void createOrcInputFile() throws IOException {
    this.orcFile = File.createTempFile("junit", null, tempDir);
    assertThat(orcFile.delete()).isTrue();

    OutputFile outFile = Files.localOutput(orcFile);
    try (FileAppender<GenericRecord> appender =
        ORC.write(outFile)
            .schema(FILE_SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      GenericRecord record = GenericRecord.create(FILE_SCHEMA);
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        record.setField("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        record.setField(
            "_no_stats_parquet",
            TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
        // in Parquet, but will produce stats for ORC
        record.setField("_required", "req"); // required, always non-null
        record.setField("_all_nulls", null); // never non-null
        record.setField("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        record.setField("_no_nulls", ""); // optional, but always non-null
        record.setField("_str", i + "str" + i);
        record.setField("_all_nans", Double.NaN); // never non-nan
        record.setField("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        record.setField(
            "_some_double_nans", (i % 10 == 0) ? Double.NaN : 2D); // includes some nan values
        record.setField("_no_nans", 3D); // optional, but always non-nan

        GenericRecord structNotNull = GenericRecord.create(_structFieldType);
        structNotNull.setField("_int_field", INT_MIN_VALUE + i);
        record.setField("_struct_not_null", structNotNull); // struct with int

        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(orcFile);
    try (Reader reader =
        OrcFile.createReader(
            new Path(inFile.location()), OrcFile.readerOptions(new Configuration()))) {
      assertThat(reader.getStripes()).as("Should create only one stripe").hasSize(1);
    }

    orcFile.deleteOnExit();
  }

  private void createParquetInputFile() throws IOException {
    File parquetFile = File.createTempFile("junit", null, tempDir);
    assertThat(parquetFile.delete()).isTrue();

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender = Parquet.write(outFile).schema(FILE_SCHEMA).build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set(
            "_no_stats_parquet",
            TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
        // in Parquet
        builder.set("_required", "req"); // required, always non-null
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set(
            "_some_double_nans", (i % 10 == 0) ? Double.NaN : 2D); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan
        builder.set("_str", i + "str" + i);

        Record structNotNull = new Record(structSchema);
        structNotNull.put("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile(inFile))) {
      assertThat(reader.getRowGroups()).as("Should create only one row group").hasSize(1);
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    parquetFile.deleteOnExit();
  }

  @TestTemplate
  public void testAllNulls() {
    boolean shouldRead;

    shouldRead = shouldRead(notNull("all_nulls"));
    assertThat(shouldRead).as("Should skip: no non-null value in all null column").isFalse();

    shouldRead = shouldRead(notNull("some_nulls"));
    assertThat(shouldRead)
        .as("Should read: column with some nulls contains a non-null value")
        .isTrue();

    shouldRead = shouldRead(notNull("no_nulls"));
    assertThat(shouldRead).as("Should read: non-null column contains a non-null value").isTrue();

    shouldRead = shouldRead(notNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(notNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @TestTemplate
  public void testNoNulls() {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = shouldRead(isNull("some_nulls"));
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = shouldRead(isNull("no_nulls"));
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();

    shouldRead = shouldRead(isNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(isNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @TestTemplate
  public void testFloatWithNan() {
    // NaN's should break Parquet's Min/Max stats we should be reading in all cases
    boolean shouldRead = shouldRead(greaterThan("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThan("some_nans", 3.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(equal("some_nans", 2.0));
    assertThat(shouldRead).isTrue();
  }

  @TestTemplate
  public void testDoubleWithNan() {
    boolean shouldRead = shouldRead(greaterThan("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead)
        .as("Should read: column with some nans contains the target value")
        .isTrue();

    shouldRead = shouldRead(lessThan("some_double_nans", 3.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(equal("some_double_nans", 2.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();
  }

  @TestTemplate
  public void testIsNaN() {
    boolean shouldRead = shouldRead(isNaN("all_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(isNaN("some_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(isNaN("no_nans"));
    switch (format) {
      case ORC:
        assertThat(shouldRead)
            .as("Should read 0 rows due to the ORC filter push-down feature")
            .isFalse();
        break;
      case PARQUET:
        assertThat(shouldRead)
            .as("Should read: NaN counts are not tracked in Parquet metrics")
            .isTrue();
        break;
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }

    shouldRead = shouldRead(isNaN("all_nulls"));
    assertThat(shouldRead).as("Should skip: all null column will not contain nan value").isFalse();
  }

  @TestTemplate
  public void testNotNaN() {
    boolean shouldRead = shouldRead(notNaN("all_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("some_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("no_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("all_nulls"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();
  }

  @TestTemplate
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(notNull("required"));
    assertThat(shouldRead).as("Should read: required columns are always non-null").isTrue();

    shouldRead = shouldRead(isNull("required"));
    assertThat(shouldRead).as("Should skip: required columns are always non-null").isFalse();
  }

  @TestTemplate
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(lessThan("missing", 5)))
        .as("Should complain about missing column in expression")
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'missing'");
  }

  @TestTemplate
  public void testColumnNotInFile() {
    assumeThat(format)
        .as(
            "If a column is not in file, ORC does NOT try to apply predicates assuming null values for the column")
        .isNotEqualTo(FileFormat.ORC);

    Expression[] cannotMatch =
        new Expression[] {
          lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
          equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
          greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
        };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead)
          .as("Should skip when column is not in file (all nulls): " + expr)
          .isFalse();
    }

    Expression[] canMatch = new Expression[] {isNull("not_in_file"), notEqual("not_in_file", 1.0f)};

    for (Expression expr : canMatch) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead)
          .as("Should read when column is not in file (all nulls): " + expr)
          .isTrue();
    }
  }

  @TestTemplate
  public void testMissingStatsParquet() {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);

    Expression[] exprs =
        new Expression[] {
          lessThan("no_stats_parquet", "a"),
          lessThanOrEqual("no_stats_parquet", "b"),
          equal("no_stats_parquet", "c"),
          greaterThan("no_stats_parquet", "d"),
          greaterThanOrEqual("no_stats_parquet", "e"),
          notEqual("no_stats_parquet", "f"),
          isNull("no_stats_parquet"),
          notNull("no_stats_parquet"),
          startsWith("no_stats_parquet", "a"),
          notStartsWith("no_stats_parquet", "a")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(expr);
      assertThat(shouldRead).as("Should read when missing stats for expr: " + expr).isTrue();
    }
  }

  @TestTemplate
  public void testZeroRecordFileParquet() {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);

    BlockMetaData emptyBlock = new BlockMetaData();
    emptyBlock.setRowCount(0);

    Expression[] exprs =
        new Expression[] {
          lessThan("id", 5),
          lessThanOrEqual("id", 30),
          equal("id", 70),
          greaterThan("id", 78),
          greaterThanOrEqual("id", 90),
          notEqual("id", 101),
          isNull("some_nulls"),
          notNull("some_nulls")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldReadParquet(expr, true, parquetSchema, emptyBlock);
      assertThat(shouldRead).as("Should never read 0-record file: " + expr).isFalse();
    }
  }

  @TestTemplate
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @TestTemplate
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        shouldRead(
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @TestTemplate
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @TestTemplate
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @TestTemplate
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @TestTemplate
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @TestTemplate
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @TestTemplate
  public void testStructFieldLt() {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testStructFieldLtEq() {
    boolean shouldRead =
        shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @TestTemplate
  public void testStructFieldGt() {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testStructFieldGtEq() {
    boolean shouldRead =
        shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @TestTemplate
  public void testStructFieldEq() {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @TestTemplate
  public void testStructFieldNotEq() {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @TestTemplate
  public void testCaseInsensitive() {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();
  }

  @TestTemplate
  public void testStringStartsWith() {
    assumeThat(format)
        .as("ORC row group filter does not support StringStartsWith")
        .isNotEqualTo(FileFormat.ORC);

    boolean shouldRead = shouldRead(startsWith("str", "1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "0st"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "1str1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "1str1_xgd"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "2str"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(startsWith("str", "9xstr"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "0S"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "x"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(startsWith("str", "9str9aaa"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();
  }

  @TestTemplate
  public void testStringNotStartsWith() {
    assumeThat(format)
        .as("ORC row group filter does not support StringStartsWith")
        .isNotEqualTo(FileFormat.ORC);

    boolean shouldRead = shouldRead(notStartsWith("str", "1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "0st"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "1str1"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "1str1_xgd"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "2str"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("str", "9xstr"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("required", "r"));
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(notStartsWith("required", "requ"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("some_nulls", "ssome"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(notStartsWith("some_nulls", "som"));
    assertThat(shouldRead).as("Should read: range matches").isTrue();
  }

  @TestTemplate
  public void testIntegerIn() {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();
  }

  @TestTemplate
  public void testIntegerNotIn() {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();

    shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    if (format == FileFormat.PARQUET) {
      // no_nulls column has all values == "", so notIn("no_nulls", "") should always be false and
      // so should be skipped
      // However, the metrics evaluator in Parquets always reads row group for a notIn filter
      assertThat(shouldRead).as("Should read: notIn on no nulls column").isTrue();
    } else {
      assertThat(shouldRead).as("Should skip: notIn on no nulls column").isFalse();
    }
  }

  @TestTemplate
  public void testSomeNullsNotEq() {
    boolean shouldRead = shouldRead(notEqual("some_nulls", "some"));
    assertThat(shouldRead).as("Should read: notEqual on some nulls column").isTrue();
  }

  @TestTemplate
  public void testInLimitParquet() {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);

    boolean shouldRead = shouldRead(in("id", 1, 2));
    assertThat(shouldRead).as("Should not read if IN is evaluated").isFalse();

    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }

    shouldRead = shouldRead(in("id", ids));
    assertThat(shouldRead).as("Should read if IN is not evaluated").isTrue();
  }

  @TestTemplate
  public void testParquetTypePromotion() {
    assumeThat(format).as("Only valid for Parquet").isEqualTo(FileFormat.PARQUET);

    Schema promotedSchema = new Schema(required(1, "id", Types.LongType.get()));
    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(promotedSchema, equal("id", INT_MIN_VALUE + 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    assertThat(shouldRead).as("Should succeed with promoted schema").isTrue();
  }

  @TestTemplate
  public void testTransformFilter() {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);

    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(SCHEMA, equal(truncate("required", 2), "some_value"), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    assertThat(shouldRead)
        .as("Should read: filter contains non-reference evaluate as True")
        .isTrue();
  }

  private boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  private boolean shouldRead(Expression expression, boolean caseSensitive) {
    switch (format) {
      case ORC:
        return shouldReadOrc(expression, caseSensitive);
      case PARQUET:
        return shouldReadParquet(expression, caseSensitive, parquetSchema, rowGroupMetadata);
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }
  }

  private boolean shouldReadOrc(Expression expression, boolean caseSensitive) {
    try (CloseableIterable<org.apache.iceberg.data.Record> reader =
        ORC.read(Files.localInput(orcFile))
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(expression)
            .caseSensitive(caseSensitive)
            .build()) {
      return !Lists.newArrayList(reader).isEmpty();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean shouldReadParquet(
      Expression expression,
      boolean caseSensitive,
      MessageType messageType,
      BlockMetaData blockMetaData) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, expression, caseSensitive)
        .shouldRead(messageType, blockMetaData);
  }

  private org.apache.parquet.io.InputFile parquetInputFile(InputFile inFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inFile.getLength();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream stream = inFile.newStream();
        return new DelegatingSeekableInputStream(stream) {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }
        };
      }
    };
  }
}
