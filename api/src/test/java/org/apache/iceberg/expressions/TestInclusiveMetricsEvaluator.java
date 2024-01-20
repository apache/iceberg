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
package org.apache.iceberg.expressions;

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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.UnicodeUtil;
import org.junit.jupiter.api.Test;

public class TestInclusiveMetricsEvaluator extends BaseInclusiveMetricsEvaluator {

  @Test
  public void testAllNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("all_nulls")).eval(FILE);
    assertThat(shouldRead).as("Should skip: no non-null value in all null column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: lessThan on all null column").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: lessThanOrEqual on all null column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: greaterThan on all null column").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: greaterThanOrEqual on all null column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: equal on all null column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, startsWith("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should skip: startsWith on all null column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("all_nulls", "a")).eval(FILE);
    assertThat(shouldRead).as("Should read: notStartsWith on all null column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("some_nulls")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: column with some nulls contains a non-null value")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("no_nulls")).eval(FILE);
    assertThat(shouldRead).as("Should read: non-null column contains a non-null value").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("all_nulls")).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("some_nulls")).eval(FILE);
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("no_nulls")).eval(FILE);
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nans")).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one nan value in all nan column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("some_nans")).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one nan value in some nan column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("no_nans")).eval(FILE);
    assertThat(shouldRead).as("Should skip: no-nans column contains no nan values").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nulls_double")).eval(FILE);
    assertThat(shouldRead).as("Should skip: all-null column doesn't contain nan value").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("no_nan_stats")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("all_nans_v1_stats")).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one nan value in all nan column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNaN("nan_and_null_only")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: at least one nan value in nan and nulls only column")
        .isTrue();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nans")).eval(FILE);
    assertThat(shouldRead)
        .as("Should skip: column with all nans will not contain non-nan")
        .isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("some_nans")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: at least one non-nan value in some nan column")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("no_nans")).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one non-nan value in no nan column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nulls_double")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: at least one non-nan value in all null column")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("no_nan_stats")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("all_nans_v1_stats")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNaN("nan_and_null_only")).eval(FILE);
    assertThat(shouldRead)
        .as("Should read: at least one null value in nan and nulls only column")
        .isTrue();
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notNull("required")).eval(FILE);
    assertThat(shouldRead).as("Should read: required columns are always non-null").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, isNull("required")).eval(FILE);
    assertThat(shouldRead).as("Should skip: required columns are always non-null").isFalse();
  }

  @Test
  public void testMissingColumn() {
    AssertHelpers.assertThrows(
        "Should complain about missing column in expression",
        ValidationException.class,
        "Cannot find field 'missing'",
        () -> new InclusiveMetricsEvaluator(SCHEMA, lessThan("missing", 5)).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    DataFile missingStats = new TestDataFile("file.parquet", Row.of(), 50);

    Expression[] exprs =
        new Expression[] {
          lessThan("no_stats", 5),
          lessThanOrEqual("no_stats", 30),
          equal("no_stats", 70),
          greaterThan("no_stats", 78),
          greaterThanOrEqual("no_stats", 90),
          notEqual("no_stats", 101),
          isNull("no_stats"),
          notNull("no_stats"),
          isNaN("some_nans"),
          notNaN("some_nans")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(missingStats);
      assertThat(shouldRead).as("Should read when missing stats for expr: " + expr).isTrue();
    }
  }

  @Test
  public void testZeroRecordFile() {
    DataFile empty = new TestDataFile("file.parquet", Row.of(), 0);

    Expression[] exprs =
        new Expression[] {
          lessThan("id", 5),
          lessThanOrEqual("id", 30),
          equal("id", 70),
          greaterThan("id", 78),
          greaterThanOrEqual("id", 90),
          notEqual("id", 101),
          isNull("some_nulls"),
          notNull("some_nulls"),
          isNaN("some_nans"),
          notNaN("some_nans"),
        };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveMetricsEvaluator(SCHEMA, expr).eval(empty);
      assertThat(shouldRead).as("Should never read 0-record file: " + expr).isFalse();
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25))).eval(FILE);
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25)))
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA,
                and(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MIN_VALUE - 30)))
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA,
                and(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MAX_VALUE + 1)))
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA,
                and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA,
                or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)))
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(
                SCHEMA,
                or(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MAX_VALUE - 19)))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE - 25)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE)).eval(FILE);
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MIN_VALUE + 1)).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, lessThan("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 25)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE + 6)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1)).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThan("id", INT_MAX_VALUE - 4)).eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 6))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE - 4))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE - 25)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE - 1)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MIN_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE - 4)).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE + 1)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, equal("id", INT_MAX_VALUE + 6)).eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 25)).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE - 1)).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MIN_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE - 4)).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE)).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 1)).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notEqual("id", INT_MAX_VALUE + 6)).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 25))).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE - 1))).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MIN_VALUE))).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE - 4))).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE))).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 1))).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("id", INT_MAX_VALUE + 6))).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE - 25)), false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE - 1)), false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MIN_VALUE)), false).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE - 4)), false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE)), false).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE + 1)), false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", INT_MAX_VALUE + 6)), false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    AssertHelpers.assertThrows(
        "Should complain about missing column in expression",
        ValidationException.class,
        "Cannot find field 'ID'",
        () -> new InclusiveMetricsEvaluator(SCHEMA, not(equal("ID", 5)), true).eval(FILE));
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "a"), true).eval(FILE);
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "a"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aa"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aaa"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "1s"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "1str1x"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "ff"), true).eval(FILE_4);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "aB"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "dWX"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "5"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", "3str3x"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("some_empty", "房东整租霍"), true).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("all_nulls", ""), true).eval(FILE);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, startsWith("required", aboveMax), true).eval(FILE_4);
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();
  }

  @Test
  public void testStringNotStartsWith() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "a"), true).eval(FILE);
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "a"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aa"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aaa"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "1s"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "1str1x"), true)
            .eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "ff"), true).eval(FILE_4);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "aB"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "dWX"), true).eval(FILE_2);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "5"), true).eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", "3str3x"), true)
            .eval(FILE_3);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notStartsWith("required", aboveMax), true)
            .eval(FILE_4);
    assertThat(shouldRead).as("Should read: range matches").isTrue();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
            .eval(FILE);
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, in("all_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, in("some_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, in("no_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();

    // should read as the number of elements in the in expression is too big
    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }
    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, in("id", ids)).eval(FILE);
    assertThat(shouldRead).as("Should read: large in expression").isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3))
            .eval(FILE);
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7))
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notIn("all_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead =
        new InclusiveMetricsEvaluator(SCHEMA, notIn("some_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();

    shouldRead = new InclusiveMetricsEvaluator(SCHEMA, notIn("no_nulls", "abc", "def")).eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on no nulls column").isTrue();
  }
}
