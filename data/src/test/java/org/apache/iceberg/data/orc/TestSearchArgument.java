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

package org.apache.iceberg.data.orc;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSearchArgument {

  private static final Types.StructType structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "int_field", IntegerType.get()));

  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      required(3, "required", StringType.get()),
      optional(4, "all_nulls", LongType.get()),
      optional(5, "some_nulls", StringType.get()),
      optional(6, "no_nulls", StringType.get()),
      optional(7, "struct_not_null", structFieldType),
      optional(9, "not_in_file", FloatType.get()),
      optional(10, "str", StringType.get())
      // ORCSchemaUtil.buildOrcProjection has a bug which does not allow addition of container types with nested
      // required children. Enable this field after #961 is fixed along with commented tests for the same column below
      //  optional(11, "map_not_null",
      //      Types.MapType.ofRequired(12, 13, StringType.get(), IntegerType.get()))
  );

  private static final Types.StructType _structFieldType =
      Types.StructType.of(Types.NestedField.required(8, "_int_field", IntegerType.get()));

  private static final Schema FILE_SCHEMA = new Schema(
      required(1, "_id", IntegerType.get()),
      required(3, "_required", StringType.get()),
      optional(4, "_all_nulls", LongType.get()),
      optional(5, "_some_nulls", StringType.get()),
      optional(6, "_no_nulls", StringType.get()),
      optional(7, "_struct_not_null", _structFieldType),
      optional(10, "_str", StringType.get())
  );

  private static final File ORC_FILE = new File("/tmp/stats-search-argument-filter-test.orc");

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  @BeforeClass
  public static void createInputFile() throws IOException {
    if (ORC_FILE.exists()) {
      Assert.assertTrue(ORC_FILE.delete());
    }

    OutputFile outFile = Files.localOutput(ORC_FILE);
    try (FileAppender<GenericRecord> appender = ORC.write(outFile)
        .schema(FILE_SCHEMA)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      GenericRecord record = GenericRecord.create(FILE_SCHEMA);
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        record.setField("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        record.setField("_required", "req"); // required, always non-null
        record.setField("_all_nulls", null); // never non-null
        record.setField("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        record.setField("_no_nulls", ""); // optional, but always non-null
        record.setField("_str", i + "str" + i);

        GenericRecord structNotNull = GenericRecord.create(_structFieldType);
        structNotNull.setField("_int_field", INT_MIN_VALUE + i);
        record.setField("_struct_not_null", structNotNull); // struct with int

        appender.add(record);
      }
    }

    InputFile inFile = Files.localInput(ORC_FILE);
    try (Reader reader = OrcFile.createReader(new Path(inFile.location()),
        OrcFile.readerOptions(new Configuration()))) {
      Assert.assertEquals("Should create only one stripe", 1, reader.getStripes().size());
    }

    ORC_FILE.deleteOnExit();
  }

  @Test
  public void testAllNulls() throws IOException {
    // ORC-623: ORC does not skip a row group for a notNull predicate on a column with all nulls
    // boolean shouldRead = shouldRead(notNull("all_nulls"));
    // Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    boolean shouldRead = shouldRead(notNull("some_nulls"));
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = shouldRead(notNull("no_nulls"));
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);

    // Enable this test after #961 is fixed
    // shouldRead = shouldRead(notNull("map_not_null"));
    // Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = shouldRead(notNull("struct_not_null"));
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testNoNulls() throws IOException {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = shouldRead(isNull("some_nulls"));
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = shouldRead(isNull("no_nulls"));
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);

    // Enable this test after #961 is fixed
    // shouldRead = shouldRead(isNull("map_not_null"));
    // Assert.assertTrue("Should read: map type is not skipped", shouldRead);

    shouldRead = shouldRead(isNull("struct_not_null"));
    Assert.assertTrue("Should read: struct type is not skipped", shouldRead);
  }

  @Test
  public void testRequiredColumn() throws IOException {
    boolean shouldRead = shouldRead(notNull("required"));
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = shouldRead(isNull("required"));
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMissingColumn() throws IOException {
    thrown.expect(ValidationException.class);
    thrown.expectMessage("Cannot find field 'missing'");
    shouldRead(lessThan("missing", 5));
  }

  @Ignore("If a column is not in file, ORC does NOT try to apply predicates assuming null values for the column")
  @Test
  public void testColumnNotInFile() throws IOException {
    Expression[] cannotMatch = new Expression[] {
        lessThan("not_in_file", 1.0f), lessThanOrEqual("not_in_file", 1.0f),
        equal("not_in_file", 1.0f), greaterThan("not_in_file", 1.0f),
        greaterThanOrEqual("not_in_file", 1.0f), notNull("not_in_file")
    };

    for (Expression expr : cannotMatch) {
      boolean shouldRead = shouldRead(expr);
      Assert.assertFalse("Should skip when column is not in file (all nulls): " + expr, shouldRead);
    }

    Expression[] canMatch = new Expression[] {
        isNull("not_in_file"), notEqual("not_in_file", 1.0f)
    };

    for (Expression expr : canMatch) {
      boolean shouldRead = shouldRead(expr);
      Assert.assertTrue("Should read when column is not in file (all nulls): " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() throws IOException {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() throws IOException {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(and(lessThan("id", INT_MIN_VALUE - 25),
        greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    Assert.assertFalse("Should skip: and(false, true)", shouldRead);

    shouldRead = shouldRead(and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = shouldRead(and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() throws IOException {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(or(lessThan("id", INT_MIN_VALUE - 25),
        greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = shouldRead(or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() throws IOException {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() throws IOException {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() throws IOException {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() throws IOException {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() throws IOException {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() throws IOException {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() throws IOException {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldLt() throws IOException {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldLtEq() throws IOException {
    boolean shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGt() throws IOException {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldGtEq() throws IOException {
    boolean shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testStructFieldEq() throws IOException {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testStructFieldNotEq() throws IOException {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitive() throws IOException {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);
  }

  @Ignore("StartsWith is not supported in ORC SearchArgument yet")
  @Test
  public void testStringStartsWith() throws IOException {
    boolean shouldRead = shouldRead(startsWith("no_stats", "a"));
    Assert.assertTrue("Should read: no stats", shouldRead);

    shouldRead = shouldRead(startsWith("str", "1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "0st"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "1str1"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "1str1_xgd"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "2str"));
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = shouldRead(startsWith("str", "9xstr"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "0S"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "x"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);

    shouldRead = shouldRead(startsWith("str", "9str9aaa"));
    Assert.assertFalse("Should not read: range doesn't match", shouldRead);
  }

  @Test
  public void testIntegerIn() throws IOException {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    Assert.assertFalse("Should skip: in on all nulls column", shouldRead);

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    Assert.assertTrue("Should read: in on no nulls column", shouldRead);
  }

  @Test
  public void testIntegerNotIn() throws IOException {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    Assert.assertTrue("Should read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    Assert.assertTrue("Should read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    Assert.assertTrue("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    Assert.assertTrue("Should read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    Assert.assertTrue("Should read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    Assert.assertTrue("Should read: notIn on all nulls column", shouldRead);

    // ORC-623: ORC seems to incorrectly skip a row group for a notIn(column, {X, ...}) predicate on a column which
    // has only 1 non-null value X but also has nulls
    // boolean shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    // Assert.assertTrue("Should read: notIn on some nulls column", shouldRead);

    // no_nulls column has all values == "", so notIn("no_nulls", "") should always be false and so should be skipped
    // However, TestMetricsRowGroupFilter seems to test that this should read which does not seem right
    // Seems like TestMetricsRowGroupFilter always says that rows might match for notIn predicates
    shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    Assert.assertFalse("Should skip: notIn on no nulls column", shouldRead);
  }

  private boolean shouldRead(Expression expression) throws IOException {
    return shouldRead(expression, true);
  }

  private boolean shouldRead(Expression expression, boolean caseSensitive) throws IOException {
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(ORC_FILE))
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
        .filter(expression)
        .caseSensitive(caseSensitive)
        .build()) {
      return Lists.newArrayList(reader).size() > 0;
    }
  }
}
