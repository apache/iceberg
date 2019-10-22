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

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestMetricsRowGroupFilter {

  private static final Types.StructType structFieldType =
          Types.StructType.of(Types.NestedField.required(8, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", StringType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", LongType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      optional(7, "struct_not_null", structFieldType),
      optional(9, "not_in_file", FloatType.get()),
      optional(10, "str", StringType.get()),
      optional(11, "map_not_null",
          Types.MapType.ofRequired(12, 13, StringType.get(), IntegerType.get()))
  );

  private static final Types.StructType _structFieldType =
          Types.StructType.of(Types.NestedField.required(8, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      optional(2, "_no_stats", StringType.get()),
      required(3, "_required", StringType.get()),
      optional(4, "_all_nulls", LongType.get()),
      optional(5, "_some_nulls", StringType.get()),
      optional(6, "_no_nulls", StringType.get()),
      optional(7, "_struct_not_null", _structFieldType),
      optional(10, "_str", StringType.get())
  );

  private static final String TOO_LONG_FOR_STATS;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS = sb.toString();
  }

  private static final File parquetFile = new File("/tmp/stats-row-group-filter-test.parquet");
  private static MessageType parquetSchema = null;
  private static BlockMetaData rowGroupMetadata = null;

  @BeforeClass
  public static void createInputFile() throws IOException {
    if (parquetFile.exists()) {
      Assert.assertTrue(parquetFile.delete());
    }

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(parquetFile);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 50 records
      for (int i = 0; i < 50; i += 1) {
        builder.set("_id", 30 + i); // min=30, max=79, num-nulls=0
        builder.set("_no_stats", TOO_LONG_FOR_STATS); // value longer than 4k will produce no stats
        builder.set("_required", "req"); // required, always non-null
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_str", i + "str" + i);

        Record structNotNull = new Record(structSchema);
        structNotNull.put("_int_field", 30 + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        appender.add(builder.build());
      }
    }

    InputFile inFile = Files.localInput(parquetFile);
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile))) {
      Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
      rowGroupMetadata = reader.getRowGroups().get(0);
      parquetSchema = reader.getFileMetaData().getSchema();
    }

    parquetFile.deleteOnExit();
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("all_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("map_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("struct_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("all_nulls"))
           .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("some_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("no_nulls"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("map_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("struct_not_null"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, isNull("required"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("missing", 5))
            .shouldRead(parquetSchema, rowGroupMetadata));
  }

  @Test
  public void testColumnNotInFile() {
    Expression[] cannotMatch = new Expression[] {
        lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
        equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
        greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
    };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata);
      Assert.assertFalse("Should skip when column is not in file (all nulls): " + expr, shouldRead);
    }

    Expression[] canMatch = new Expression[] {
        isNull("not_in_file"), notEqual("not_in_file", 1.0f)
    };

    for (Expression expr : canMatch) {
      boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata);
      Assert.assertTrue("Should read when column is not in file (all nulls): " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStats() {
    Expression[] exprs = new Expression[] {
        lessThan("no_stats", "a"), lessThanOrEqual("no_stats", "b"), equal("no_stats", "c"),
        greaterThan("no_stats", "d"), greaterThanOrEqual("no_stats", "e"),
        notEqual("no_stats", "f"), isNull("no_stats"), notNull("no_stats")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, rowGroupMetadata);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testZeroRecordFile() {
    BlockMetaData emptyBlock = new BlockMetaData();
    emptyBlock.setRowCount(0);

    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70), greaterThan("id", 78),
        greaterThanOrEqual("id", 90), notEqual("id", 101), isNull("some_nulls"),
        notNull("some_nulls")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, expr)
          .shouldRead(parquetSchema, emptyBlock);
      Assert.assertFalse("Should never read 0-record file: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(lessThan("id", 5)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(greaterThan("id", 5)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA,
        and(lessThan("id", 5), greaterThanOrEqual("id", 0)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA,
        and(greaterThan("id", 5), lessThanOrEqual("id", 30)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 80)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 60)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("id", 5))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("id", 30))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("id", 31))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("id", 5))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("id", 29))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("id", 30))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("id", 85))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("id", 78))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("id", 75))
          .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 85))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 80))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 75))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 5))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 29))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 30))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 75))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 80))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("id", 85))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 5))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 29))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 30))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 75))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 79))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 80))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 85))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 5)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 29)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 30)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 75)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 79)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 80)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, not(equal("id", 85)))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 5))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 30))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 31))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 5))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 29))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 30))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 85))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 78))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 75))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 85))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 80))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 75))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 5))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 29))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 30))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 75))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 80))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 85))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 5))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 29))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 30))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 75))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 79))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("id", 80))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 85))
            .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, equal("ID", 5), false)
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("no_stats", "a"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: no stats", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "1"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "0st"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "1str1"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "1str1_xgd"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "2str"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "9xstr"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "0S"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "x"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = new ParquetMetricsRowGroupFilter(SCHEMA, startsWith("str", "9str9aaa"))
        .shouldRead(parquetSchema, rowGroupMetadata);
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);
  }
}
