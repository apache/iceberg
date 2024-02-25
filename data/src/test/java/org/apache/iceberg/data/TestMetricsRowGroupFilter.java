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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
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
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestMetricsRowGroupFilter {

  private static Stream<Object[]> data()
  {
    return Arrays.stream(new Object[][]{{"parquet"}, {"orc"}});
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
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS_PARQUET = sb.toString();
  }

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  @TempDir
  public File orcFile;
  private MessageType parquetSchema = null;
  private BlockMetaData rowGroupMetadata = null;

  //@Rule public TemporaryFolder temp = new TemporaryFolder();
  @TempDir
  public File temp;


  @BeforeEach
  public void createInputFile() throws IOException {
    Arrays.stream(FileFormat.values())
            .filter(
                    f-> {
                      return f.name().equalsIgnoreCase("orc") || f.name().equalsIgnoreCase("parquet");
                    }
            )
            .forEach(format-> {
      switch (format) {
        case ORC:
          try {
            createOrcInputFile();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
        case PARQUET:
          try {
            createParquetInputFile();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
        default:
          throw new UnsupportedOperationException(
                  "Row group filter tests not supported for " + format);
      }
    });
  }

  public void createOrcInputFile() throws IOException {
    //this.orcFile = temp;
    org.junit.jupiter.api.Assertions.assertTrue(orcFile.delete());

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
      org.junit.jupiter.api.Assertions.assertEquals(1, reader.getStripes().size(), "Should create only one stripe");
    }

    orcFile.deleteOnExit();
  }

  private void createParquetInputFile() throws IOException {
    File parquetFile = temp;
    org.junit.jupiter.api.Assertions.assertTrue(parquetFile.delete());

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
      org.junit.jupiter.api.Assertions.assertEquals(1, reader.getRowGroups().size(), "Should create only one row group");
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    parquetFile.deleteOnExit();
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead;

    shouldRead = shouldRead(notNull("all_nulls"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead,"Should skip: no non-null value in all null column");

    shouldRead = shouldRead(notNull("some_nulls"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should read: column with some nulls contains a non-null value");

    shouldRead = shouldRead(notNull("no_nulls"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should read: non-null column contains a non-null value");

    shouldRead = shouldRead(notNull("map_not_null"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should read: map type is not skipped");

    shouldRead = shouldRead(notNull("struct_not_null"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should read: struct type is not skipped");
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: at least one null value in all null column");

    shouldRead = shouldRead(isNull("some_nulls"));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: column with some nulls contains a null value");

    shouldRead = shouldRead(isNull("no_nulls"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: non-null column contains no null values");

    shouldRead = shouldRead(isNull("map_not_null"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: map type is not skipped");

    shouldRead = shouldRead(isNull("struct_not_null"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: struct type is not skipped");
  }

  @Test
  public void testFloatWithNan() {
    // NaN's should break Parquet's Min/Max stats we should be reading in all cases
    boolean shouldRead = shouldRead(greaterThan("some_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("some_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead);

    shouldRead = shouldRead(lessThan("some_nans", 3.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("some_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead);

    shouldRead = shouldRead(equal("some_nans", 2.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead);
  }

  @Test
  public void testDoubleWithNan() {
    boolean shouldRead = shouldRead(greaterThan("some_double_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: column with some nans contains target value");

    shouldRead = shouldRead(greaterThanOrEqual("some_double_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: column with some nans contains the target value");

    shouldRead = shouldRead(lessThan("some_double_nans", 3.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: column with some nans contains target value");

    shouldRead = shouldRead(lessThanOrEqual("some_double_nans", 1.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: column with some nans contains target value");

    shouldRead = shouldRead(equal("some_double_nans", 2.0));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: column with some nans contains target value");
  }

  @ParameterizedTest(name = "format = {0}" )
  @MethodSource("data")
  public void testIsNaN(Object format) {
    boolean shouldRead = shouldRead(isNaN("all_nans"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");

    shouldRead = shouldRead(isNaN("some_nans"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");

    shouldRead = shouldRead(isNaN("no_nans"));
    switch (FileFormat.fromString(format.toString())) {
      case ORC:
        org.junit.jupiter.api.Assertions.assertFalse(shouldRead,"Should read 0 rows due to the ORC filter push-down feature");
        break;
      case PARQUET:
        org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");
        break;
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }

    shouldRead = shouldRead(isNaN("all_nulls"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: all null column will not contain nan value");
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(notNaN("all_nans"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");

    shouldRead = shouldRead(notNaN("some_nans"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: NaN counts are not tracked in Parquet metrics");

    shouldRead = shouldRead(notNaN("no_nans"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");

    shouldRead = shouldRead(notNaN("all_nulls"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: NaN counts are not tracked in Parquet metrics");
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(notNull("required"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: required columns are always non-null");

    shouldRead = shouldRead(isNull("required"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: required columns are always non-null");
  }

  @Test
  public void testMissingColumn() {
    Assertions.assertThatThrownBy(() -> shouldRead(lessThan("missing", 5)))
        .as("Should complain about missing column in expression")
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'missing'");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testColumnNotInFile(Object format) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        FileFormat.fromString(format.toString()) == FileFormat.ORC,
    "If a column is not in file, ORC does NOT try to apply predicates assuming null values for the column");
    Expression[] cannotMatch =
        new Expression[] {
          lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
          equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
          greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
        };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = shouldRead(expr);
      org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip when column is not in file (all nulls): " + expr);
    }

    Expression[] canMatch = new Expression[] {isNull("not_in_file"), notEqual("not_in_file", 1.0f)};

    for (Expression expr : canMatch) {
      boolean shouldRead = shouldRead(expr);
      org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read when column is not in file (all nulls): " + expr);
    }
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testMissingStatsParquet(Object format) {
    org.junit.jupiter.api.Assumptions.assumeTrue(FileFormat.fromString(format.toString()) == FileFormat.PARQUET);
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
      org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read when missing stats for expr: " + expr);
    }
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testZeroRecordFileParquet(Object format) {
    org.junit.jupiter.api.Assumptions.assumeTrue(FileFormat.fromString(format.toString()) == FileFormat.PARQUET);
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
      org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should never read 0-record file: " + expr);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: not(false)");

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    org.junit.jupiter.api.Assertions.assertFalse( shouldRead, "Should skip: not(true)");
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: and(false, true)");

    shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: and(false, false)");

    shouldRead =
        shouldRead(
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: and(true, true)");
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    org.junit.jupiter.api.Assertions.assertFalse( shouldRead, "Should skip: or(false, false)");

    shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: or(false, true)");
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range below lower bound (5 < 30)");

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertFalse( shouldRead, "Should not read: id range below lower bound (30 is not < 30)");

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead,"Should read: one possible id");

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range below lower bound (29 < 30)");

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead,"Should read: one possible id");

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: many possible ids");
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (85 < 79)");

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (79 is not > 79)");

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (85 < 79)");

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (80 > 79)");

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound");

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound");

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound");

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id between lower and upper bounds");

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound");

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertFalse( shouldRead, "Should not read: id above upper bound");

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse( shouldRead, "Should not read: id above upper bound");
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: id below lower bound");

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound");

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound");

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id between lower and upper bounds");

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound");

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound");

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound");

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound");

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id between lower and upper bounds");

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: id equal to upper bound");

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range below lower bound (5 < 30)");

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead,"Should not read: id range below lower bound (30 is not < 30)");

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead =
        shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead,"Should not read: id range below lower bound (5 < 30)");

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range below lower bound (29 < 30)");

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: many possible ids");
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (85 < 79)");

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (79 is not > 79)");

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead =
        shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (85 < 79)");

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id range above upper bound (80 > 79)");

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: one possible id");

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: may possible ids");
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id between lower and upper bounds");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id above upper bound");

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id above upper bound");
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound");

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound");

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound");

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id between lower and upper bounds");

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound");

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound");
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testStringStartsWith(Object format) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
         FileFormat.fromString(format.toString()) == FileFormat.ORC, "ORC row group filter does not support StringStartsWith");
    boolean shouldRead = shouldRead(startsWith("str", "1"));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: range matches");

    shouldRead = shouldRead(startsWith("str", "0st"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(startsWith("str", "1str1"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(startsWith("str", "1str1_xgd"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(startsWith("str", "2str"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(startsWith("str", "9xstr"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: range doesn't match");

    shouldRead = shouldRead(startsWith("str", "0S"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: range doesn't match");

    shouldRead = shouldRead(startsWith("str", "x"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: range doesn't match");

    shouldRead = shouldRead(startsWith("str", "9str9aaa"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: range doesn't match");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testStringNotStartsWith(Object format) {
    org.junit.jupiter.api.Assumptions.assumeFalse(FileFormat.fromString(format.toString()) == FileFormat.ORC,
            "ORC row group filter does not support StringStartsWith");
    boolean shouldRead = shouldRead(notStartsWith("str", "1"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("str", "0st"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: range matches");

    shouldRead = shouldRead(notStartsWith("str", "1str1"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("str", "1str1_xgd"));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("str", "2str"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("str", "9xstr"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("required", "r"));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: range doesn't match");

    shouldRead = shouldRead(notStartsWith("required", "requ"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");

    shouldRead = shouldRead(notStartsWith("some_nulls", "ssome"));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead,"Should read: range matches");

    shouldRead = shouldRead(notStartsWith("some_nulls", "som"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: range matches");
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound (5 < 30, 6 < 30)");

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id below lower bound (28 < 30, 29 < 30)");

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to lower bound (30 == 30)");

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    org.junit.jupiter.api.Assertions.assertTrue(
       shouldRead,"Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)");

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound (79 == 79)");

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id above upper bound (80 > 79, 81 > 79)");

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read: id above upper bound (85 > 79, 86 > 79)");

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: in on all nulls column");

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: in on some nulls column");

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: in on no nulls column");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testIntegerNotIn(Object format) {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id below lower bound (5 < 30, 6 < 30)");

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: id below lower bound (28 < 30, 29 < 30)");

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    org.junit.jupiter.api.Assertions.assertTrue( shouldRead, "Should read: id equal to lower bound (30 == 30)");

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)");

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id equal to upper bound (79 == 79)");

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound (80 > 79, 81 > 79)");

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: id above upper bound (85 > 79, 86 > 79)");

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: notIn on all nulls column");

    shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: notIn on some nulls column");

    shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    if (FileFormat.fromString(format.toString()) == FileFormat.PARQUET) {
      // no_nulls column has all values == "", so notIn("no_nulls", "") should always be false and
      // so should be skipped
      // However, the metrics evaluator in Parquets always reads row group for a notIn filter
      org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read: notIn on no nulls column");
    } else {
      org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should skip: notIn on no nulls column");
    }
  }

  @Test
  public void testSomeNullsNotEq() {
    boolean shouldRead = shouldRead(notEqual("some_nulls", "some"));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead,"Should read: notEqual on some nulls column");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testInLimitParquet(Object format) {
    org.junit.jupiter.api.Assumptions.assumeTrue(FileFormat.fromString(format.toString()) == FileFormat.PARQUET);

    boolean shouldRead = shouldRead(in("id", 1, 2));
    org.junit.jupiter.api.Assertions.assertFalse(shouldRead, "Should not read if IN is evaluated");

    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }

    shouldRead = shouldRead(in("id", ids));
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should read if IN is not evaluated");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testParquetTypePromotion(Object format) {
    org.junit.jupiter.api.Assumptions.assumeTrue(FileFormat.fromString(format.toString()) == FileFormat.PARQUET, "Only valid for Parquet");
    Schema promotedSchema = new Schema(required(1, "id", Types.LongType.get()));
    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(promotedSchema, equal("id", INT_MIN_VALUE + 1), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    org.junit.jupiter.api.Assertions.assertTrue(shouldRead, "Should succeed with promoted schema");
  }

  @ParameterizedTest(name = "format = {0}")
  @MethodSource("data")
  public void testTransformFilter(Object format) {
    Assumptions.assumeThat(FileFormat.fromString(format.toString())).isEqualTo(FileFormat.PARQUET);
    boolean shouldRead =
        new ParquetMetricsRowGroupFilter(SCHEMA, equal(truncate("required", 2), "some_value"), true)
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assertions.assertThat(shouldRead)
        .as("Should read: filter contains non-reference evaluate as True")
        .isTrue();
  }

  private boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  private boolean shouldRead(Expression expression, boolean caseSensitive) {
    return Arrays.stream(FileFormat.values())
            .filter(
            f-> {
              return f.name().equalsIgnoreCase("orc") || f.name().equalsIgnoreCase("parquet");
            }
    ).map(format ->{
    switch (format) {
      case ORC:
        return shouldReadOrc(expression, caseSensitive);
      case PARQUET:
        return shouldReadParquet(expression, caseSensitive, parquetSchema, rowGroupMetadata);
      default:
        throw new UnsupportedOperationException(
            "Row group filter tests not supported for " + format);
    }}).sequential().findAny().get();
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
