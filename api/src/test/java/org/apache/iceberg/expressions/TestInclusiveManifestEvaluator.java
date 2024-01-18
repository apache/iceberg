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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestInclusiveManifestEvaluator extends BaseInclusiveManifestEvaluator {

  @Test
  public void testAllNulls() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(notNull("all_nulls_missing_nan"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as(
        "Should skip: all nulls column with non-floating type contains all null").
        isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(notNull("all_nulls_missing_nan_float"), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as(
        "Should read: no NaN information may indicate presence of NaN value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNull("some_nulls"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as(
        "Should read: column with some nulls contains a non-null value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNull("no_nulls"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as(
        "Should read: non-null column contains a non-null value").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("all_nulls_missing_nan", "asad"), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: startsWith on all null column").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("all_nulls_missing_nan", "asad"), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: notStartsWith on all null column").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(isNull("all_nulls_missing_nan"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(isNull("some_nulls"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(isNull("no_nulls"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();

    shouldRead = ManifestEvaluator.forRowFilter(isNull("both_nan_and_null"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: both_nan_and_null column contains no null values").isTrue();

  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(isNaN("float"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no information on if there are nan value in float column").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(isNaN("all_nulls_double"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no NaN information may indicate presence of NaN value").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(isNaN("all_nulls_missing_nan_float"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no NaN information may indicate presence of NaN value").isTrue();


    shouldRead = ManifestEvaluator.forRowFilter(isNaN("all_nulls_no_nans"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should skip: no nan column doesn't contain nan value").isFalse();

    shouldRead = ManifestEvaluator.forRowFilter(isNaN("all_nans"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: all_nans column contains nan value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(isNaN("both_nan_and_null"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: both_nan_and_null column contains nan value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(isNaN("no_nan_or_null"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should skip: no_nan_or_null column doesn't contain nan value").isFalse();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(notNaN("float"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no information on if there are nan value in float column").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNaN("all_nulls_double"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: all null column contains non nan value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNaN("all_nulls_no_nans"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no_nans column contains non nan value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNaN("all_nans"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should skip: all nans column doesn't contain non nan value").isFalse();

    shouldRead = ManifestEvaluator.forRowFilter(notNaN("both_nan_and_null"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: both_nan_and_null nans column contains non nan value").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(notNaN("no_nan_or_null"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: no_nan_or_null column contains non nan value").isTrue();
  }

  @Test
  public void testMissingColumn() {
    AssertHelpers.assertThrows(
        "Should complain about missing column in expression",
        ValidationException.class,
        "Cannot find field 'missing'",
        () -> ManifestEvaluator.forRowFilter(lessThan("missing", 5), SPEC, true).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    Expression[] exprs =
        new Expression[] {
          lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70),
          greaterThan("id", 78), greaterThanOrEqual("id", 90), notEqual("id", 101),
          isNull("id"), notNull("id"), startsWith("all_nulls_missing_nan", "a"),
          isNaN("float"), notNaN("float"), notStartsWith("all_nulls_missing_nan", "a")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = ManifestEvaluator.forRowFilter(expr, SPEC, true).eval(NO_STATS);
      assertThat(shouldRead).as("Should read when missing stats for expr: " + expr).isTrue();
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(not(lessThan("id", INT_MIN_VALUE - 25)), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(greaterThan("id", INT_MIN_VALUE - 25)), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(
                and(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MIN_VALUE - 30)),
                SPEC,
                true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                and(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MAX_VALUE + 1)),
                SPEC,
                true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)),
                SPEC,
                true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(
                or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)),
                SPEC,
                true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                or(
                    lessThan("id", INT_MIN_VALUE - 25),
                    greaterThanOrEqual("id", INT_MAX_VALUE - 19)),
                SPEC,
                true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(lessThan("id", INT_MIN_VALUE - 25), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThan("id", INT_MIN_VALUE), SPEC, true).eval(FILE);
   assertThat(shouldRead).as("Should not read: id range below lower bound (30 is not < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThan("id", INT_MIN_VALUE + 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThan("id", INT_MAX_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(lessThanOrEqual("id", INT_MIN_VALUE - 25), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThanOrEqual("id", INT_MIN_VALUE - 1), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThanOrEqual("id", INT_MIN_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(lessThanOrEqual("id", INT_MAX_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(greaterThan("id", INT_MAX_VALUE + 6), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThan("id", INT_MAX_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (79 is not > 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThan("id", INT_MAX_VALUE - 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThan("id", INT_MAX_VALUE - 4), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", INT_MAX_VALUE + 6), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", INT_MAX_VALUE + 1), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", INT_MAX_VALUE), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", INT_MAX_VALUE - 4), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(equal("id", INT_MIN_VALUE - 25), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(equal("id", INT_MIN_VALUE - 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", INT_MIN_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(equal("id", INT_MAX_VALUE - 4), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", INT_MAX_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(equal("id", INT_MAX_VALUE + 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(equal("id", INT_MAX_VALUE + 6), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MIN_VALUE - 25), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MIN_VALUE - 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MIN_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MAX_VALUE - 4), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MAX_VALUE), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MAX_VALUE + 1), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notEqual("id", INT_MAX_VALUE + 6), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MIN_VALUE - 25)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MIN_VALUE - 1)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MIN_VALUE)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MAX_VALUE - 4)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MAX_VALUE)), SPEC, true).eval(FILE);
      assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MAX_VALUE + 1)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("id", INT_MAX_VALUE + 6)), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MIN_VALUE - 25)), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MIN_VALUE - 1)), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MIN_VALUE)), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MAX_VALUE - 4)), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MAX_VALUE)), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MAX_VALUE + 1)), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(not(equal("ID", INT_MAX_VALUE + 6)), SPEC, false).eval(FILE);
   assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    AssertHelpers.assertThrows(
        "Should complain about missing column in expression",
        ValidationException.class,
        "Cannot find field 'ID'",
        () -> ManifestEvaluator.forRowFilter(not(equal("ID", 5)), SPEC, true).eval(FILE));
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "a"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "aa"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "dddd"), SPEC, false).eval(FILE);
   assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "z"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("no_nulls", "a"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "zzzz"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should skip: range doesn't match").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(startsWith("some_nulls", "1"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should skip: range doesn't match").isFalse();
  }

  @Test
  public void testStringNotStartsWith() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "a"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "aa"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "dddd"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "z"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("no_nulls", "a"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "zzzz"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("some_nulls", "1"), SPEC, false).eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("all_same_value_or_null", "a"), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: range matches on null").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("all_same_value_or_null", "aa"), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("all_same_value_or_null", "A"), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    // Iceberg does not implement SQL's 3-way boolean logic, so the choice of an all null column
    // matching is
    // by definition in order to surface more values to the query engine to allow it to make its own
    // decision.
    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("all_nulls_missing_nan", "A"), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notStartsWith("no_nulls_same_value_a", "a"), SPEC, false)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: all values start with the prefix").isFalse();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), SPEC, true)
            .eval(FILE);
     assertThat(shouldRead).as(
        "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("all_nulls_missing_nan", "abc", "def"), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("some_nulls", "abc", "def"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(in("no_nulls", "abc", "def"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(
                notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as(
        "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), SPEC, true)
            .eval(FILE);
      assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(
                notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7), SPEC, true)
            .eval(FILE);
      assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notIn("all_nulls_missing_nan", "abc", "def"), SPEC, true)
            .eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notIn("some_nulls", "abc", "def"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();

    shouldRead =
        ManifestEvaluator.forRowFilter(notIn("no_nulls", "abc", "def"), SPEC, true).eval(FILE);
    assertThat(shouldRead).as("Should read: notIn on no nulls column").isTrue();
  }
}
