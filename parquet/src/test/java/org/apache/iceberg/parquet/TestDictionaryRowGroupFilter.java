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
import org.apache.parquet.column.page.DictionaryPageReadStore;
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

public class TestDictionaryRowGroupFilter {

  private static final Types.StructType structFieldType =
          Types.StructType.of(Types.NestedField.required(9, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", StringType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", LongType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      optional(7, "non_dict", StringType.get()),
      optional(8, "struct_not_null", structFieldType),
      optional(10, "not_in_file", FloatType.get())
  );

  private static final Types.StructType _structFieldType =
          Types.StructType.of(Types.NestedField.required(9, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      optional(2, "_no_stats", StringType.get()),
      required(3, "_required", StringType.get()),
      optional(4, "_all_nulls", LongType.get()),
      optional(5, "_some_nulls", StringType.get()),
      optional(6, "_no_nulls", StringType.get()),
      optional(7, "_non_dict", StringType.get()),
      optional(8, "_struct_not_null", _structFieldType)
  );

  private static final String TOO_LONG_FOR_STATS;
  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID().toString());
    }
    TOO_LONG_FOR_STATS = sb.toString();
  }

  private static final File PARQUET_FILE = new File("/tmp/stats-row-group-filter-test.parquet");
  private static MessageType PARQUET_SCHEMA = null;
  private static BlockMetaData ROW_GROUP_METADATA = null;
  private static DictionaryPageReadStore DICTIONARY_STORE = null;

  @BeforeClass
  public static void createInputFile() throws IOException {
    if (PARQUET_FILE.exists()) {
      Assert.assertTrue(PARQUET_FILE.delete());
    }

    // build struct field schema
    org.apache.avro.Schema structSchema = AvroSchemaUtil.convert(_structFieldType);

    OutputFile outFile = Files.localOutput(PARQUET_FILE);
    try (FileAppender<Record> appender = Parquet.write(outFile)
        .schema(FILE_SCHEMA)
        .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
      // create 20 copies of each record to ensure dictionary-encoding
      for (int copy = 0; copy < 20; copy += 1) {
        // create 50 records
        for (int i = 0; i < 50; i += 1) {
          builder.set("_id", 30 + i); // min=30, max=79, num-nulls=0
          builder.set("_no_stats", TOO_LONG_FOR_STATS); // value longer than 4k will produce no stats
          builder.set("_required", "req"); // required, always non-null
          builder.set("_all_nulls", null); // never non-null
          builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
          builder.set("_no_nulls", ""); // optional, but always non-null
          builder.set("_non_dict", UUID.randomUUID().toString()); // not dictionary-encoded

          Record struct_not_null = new Record(structSchema);
          struct_not_null.put("_int_field", 30 + i);
          builder.set("_struct_not_null", struct_not_null); // struct with int

          appender.add(builder.build());
        }
      }
    }

    InputFile inFile = Files.localInput(PARQUET_FILE);

    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inFile));

    Assert.assertEquals("Should create only one row group", 1, reader.getRowGroups().size());
    ROW_GROUP_METADATA = reader.getRowGroups().get(0);
    PARQUET_SCHEMA = reader.getFileMetaData().getSchema();
    DICTIONARY_STORE = reader.getNextDictionaryReader();

    PARQUET_FILE.deleteOnExit();
  }

  @Test
  public void testAssumptions() {
    // this case validates that other cases don't need to test expressions with null literals.
    TestHelpers.assertThrows("Should reject null literal in equal expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> equal("col", null));
    TestHelpers.assertThrows("Should reject null literal in notEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in lessThan expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> lessThan("col", null));
    TestHelpers.assertThrows("Should reject null literal in lessThanOrEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> lessThanOrEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in greaterThan expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> greaterThan("col", null));
    TestHelpers.assertThrows("Should reject null literal in greaterThanOrEqual expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> greaterThanOrEqual("col", null));
    TestHelpers.assertThrows("Should reject null literal in startsWith expression",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> startsWith("col", null));
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("all_nulls"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("some_nulls"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("no_nulls"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("struct_not_null"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("all_nulls"))
           .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("some_nulls"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("no_nulls"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("struct_not_null"))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary filter doesn't help", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notNull("required"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, isNull("required"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testStartsWith() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("non_dict", "re"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: no dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "re"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "req"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("some_nulls", "so"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: dictionary contains a matching entry", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("no_stats", UUID.randomUUID().toString()))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: no stats but dictionary is present", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("required", "reqs"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("some_nulls", "somex"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, startsWith("no_nulls", "xxx"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: no match in dictionary", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("missing", 5))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE));
  }

  @Test
  public void testColumnNotInFile() {
    Expression[] exprs = new Expression[] {
        lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
        equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
        greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file"),
        isNull("not_in_file"), notEqual("not_in_file", 1.0f)
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, expr)
          .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
      Assert.assertTrue("Should read: dictionary cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testColumnFallbackOrNotDictionaryEncoded() {
    Expression[] exprs = new Expression[] {
        lessThan("non_dict", "a"), lessThanOrEqual("non_dict", "a"), equal("non_dict", "a"),
        greaterThan("non_dict", "a"), greaterThanOrEqual("non_dict", "a"), notNull("non_dict"),
        isNull("non_dict"), notEqual("non_dict", "a")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, expr)
          .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
      Assert.assertTrue("Should read: dictionary cannot be found: " + expr, shouldRead);
    }
  }

  @Test
  public void testMissingStats() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("no_stats", "a"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: stats are missing but dictionary is present", shouldRead);
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(lessThan("id", 5)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(greaterThan("id", 5)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        and(lessThan("id", 5), greaterThanOrEqual("id", 0)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        and(greaterThan("id", 5), lessThanOrEqual("id", 30)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 80)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 60)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", 5))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", 30))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", 31))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", 5))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", 29))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", 30))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", 85))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", 78))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("id", 75))
          .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 85))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 80))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("id", 75))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 5))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 29))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 30))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 75))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 80))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("id", 85))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 5))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 29))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 30))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 75))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 80))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 85))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 5)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 29)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 30)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 75)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 79)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 80)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, not(equal("id", 85)))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testStringNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: contains null != 'some'", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("no_nulls", ""))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: contains only ''", shouldRead);
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 5))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 30))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 31))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThan("struct_not_null.int_field", 79))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 5))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 29))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 30))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, lessThanOrEqual("struct_not_null.int_field", 79))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);

  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 85))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 79))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 78))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThan("struct_not_null.int_field", 75))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA,
        greaterThanOrEqual("struct_not_null.int_field", 85))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 80))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 79))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, greaterThanOrEqual("struct_not_null.int_field", 75))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 5))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 29))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 30))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 75))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 79))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 80))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, equal("struct_not_null.int_field", 85))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 5))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 29))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 30))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 75))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 79))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("id", 80))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("struct_not_null.int_field", 85))
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("no_Nulls", ""), false)
        .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, DICTIONARY_STORE);
    Assert.assertFalse("Should skip: contains only ''", shouldRead);
  }

  @Test
  public void testMissingDictionaryPageForColumn() {
    TestHelpers.assertThrows("Should complain about missing dictionary",
        IllegalStateException.class, "Failed to read required dictionary page for id: 5",
        () -> new ParquetDictionaryRowGroupFilter(SCHEMA, notEqual("some_nulls", "some"))
            .shouldRead(PARQUET_SCHEMA, ROW_GROUP_METADATA, descriptor -> null));
  }
}
