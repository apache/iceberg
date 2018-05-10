/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.expressions;

import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.TestHelpers.Row;
import com.netflix.iceberg.TestHelpers.TestDataFile;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.types.Types.IntegerType;
import org.junit.Assert;
import org.junit.Test;


import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.isNull;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.not;
import static com.netflix.iceberg.expressions.Expressions.notEqual;
import static com.netflix.iceberg.expressions.Expressions.notNull;
import static com.netflix.iceberg.expressions.Expressions.or;
import static com.netflix.iceberg.types.Conversions.toByteBuffer;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestInclusiveMetricsEvaluator {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", IntegerType.get()),
      optional(2, "no_stats", Types.IntegerType.get()),
      required(3, "required", Types.StringType.get()),
      optional(4, "all_nulls", Types.StringType.get()),
      optional(5, "some_nulls", Types.StringType.get()),
      optional(6, "no_nulls", Types.StringType.get())
  );

  private static final DataFile FILE = new TestDataFile("file.avro", Row.of(), 50,
      // any value counts, including nulls
      ImmutableMap.of(
          4, 50L,
          5, 50L,
          6, 50L),
      // null value counts
      ImmutableMap.of(
          4, 50L,
          5, 10L,
          6, 0L),
      // lower bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), 30)),
      // upper bounds
      ImmutableMap.of(
          1, toByteBuffer(IntegerType.get(), 79)));

  @Test
  public void testAllNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("all_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("no_nulls")).eval(FILE);
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("all_nulls")).eval(FILE);
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("no_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("required")).eval(FILE);
    Assert.assertTrue("Should read: required columns are always non-null", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("required")).eval(FILE);
    Assert.assertFalse("Should skip: required columns are always non-null", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new InclusiveMetricsEvaluator(SCHEMA, lessThan("missing", 5)).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    DataFile missingStats = new TestDataFile("file.parquet", Row.of(), 50);

    Expression[] exprs = new Expression[] {
        lessThan("no_stats", 5), lessThanOrEqual("no_stats", 30), equal("no_stats", 70),
        greaterThan("no_stats", 78), greaterThanOrEqual("no_stats", 90), notEqual("no_stats", 101),
        isNull("no_stats"), notNull("no_stats")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(missingStats);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testZeroRecordFile() {
    DataFile empty = new TestDataFile("file.parquet", Row.of(), 0);

    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70), greaterThan("id", 78),
        greaterThanOrEqual("id", 90), notEqual("id", 101), isNull("some_nulls"),
        notNull("some_nulls")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(empty);
      Assert.assertFalse("Should never read 0-record file: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(lessThan("id", 5))).eval(FILE);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(greaterThan("id", 5))).eval(FILE);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        and(lessThan("id", 5), greaterThanOrEqual("id", 0))).eval(FILE);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        and(greaterThan("id", 5), lessThanOrEqual("id", 30))).eval(FILE);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 80))).eval(FILE);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA,
        or(lessThan("id", 5), greaterThanOrEqual("id", 60))).eval(FILE);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", 30)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", 31)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", 29)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", 79)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", 78)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", 80)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 29)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 80)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 5)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 29)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 80)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", 85)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }
}
