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
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
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
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.ZoneOffset;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestInclusiveMetricsEvaluatorWithTransforms {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          required(2, "ts", Types.TimestampType.withZone()),
          optional(3, "all_nulls", Types.IntegerType.get()),
          optional(4, "all_nulls_str", Types.StringType.get()),
          optional(5, "no_stats", IntegerType.get()),
          optional(6, "str", Types.StringType.get()));

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  protected static final long TS_MIN_VALUE =
      DateTimeUtil.microsFromTimestamptz(
          DateTimeUtil.dateFromDays(30).atStartOfDay().atOffset(ZoneOffset.UTC));
  protected static final long TS_MAX_VALUE =
      DateTimeUtil.microsFromTimestamptz(
          DateTimeUtil.dateFromDays(79).atStartOfDay().atOffset(ZoneOffset.UTC));

  protected DataFile file() {
    return new TestDataFile(
        "file.avro",
        Row.of(),
        50,
        // any value counts, including nulls
        ImmutableMap.<Integer, Long>builder()
            .put(1, 50L)
            .put(2, 50L)
            .put(3, 50L)
            .put(4, 50L)
            .buildOrThrow(),
        // null value counts
        ImmutableMap.<Integer, Long>builder()
            .put(1, 0L)
            .put(2, 0L)
            .put(3, 50L) // all_nulls
            .put(4, 50L) // all_nulls_str
            .buildOrThrow(),
        // nan value counts
        null,
        // lower bounds
        ImmutableMap.of(
            2, Conversions.toByteBuffer(Types.TimestampType.withZone(), TS_MIN_VALUE),
            6, Conversions.toByteBuffer(Types.StringType.get(), "abc")),
        // upper bounds
        ImmutableMap.of(
            2, Conversions.toByteBuffer(Types.TimestampType.withZone(), TS_MAX_VALUE),
            6, Conversions.toByteBuffer(Types.StringType.get(), "abe")));
  }

  private boolean shouldRead(Expression expr) {
    return shouldRead(expr, file());
  }

  private boolean shouldRead(Expression expr, DataFile file) {
    return shouldRead(expr, file, true);
  }

  private boolean shouldReadCaseInsensitive(Expression expr) {
    return shouldRead(expr, file(), false);
  }

  private boolean shouldRead(Expression expr, DataFile file, boolean caseSensitive) {
    return new InclusiveMetricsEvaluator(SCHEMA, expr, caseSensitive).eval(file);
  }

  @Test
  public void testAllNullsWithNonOrderPreserving() {
    assertThat(shouldRead(isNull(bucket("all_nulls", 100))))
        .as("Should read: null values exist in all null column")
        .isTrue();

    assertThat(shouldRead(notNull(bucket("all_nulls", 100))))
        .as("Should skip: no non-null value in all null column")
        .isFalse();

    assertThat(shouldRead(lessThan(bucket("all_nulls", 100), 30)))
        .as("Should skip: lessThan on all null column")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(bucket("all_nulls", 100), 30)))
        .as("Should skip: lessThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThan(bucket("all_nulls", 100), 30)))
        .as("Should skip: greaterThan on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(bucket("all_nulls", 100), 30)))
        .as("Should skip: greaterThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(equal(bucket("all_nulls", 100), 30)))
        .as("Should skip: equal on all null column")
        .isFalse();

    assertThat(shouldRead(notEqual(bucket("all_nulls", 100), 30)))
        .as("Should read: notEqual on all null column")
        .isTrue();

    assertThat(shouldRead(in(bucket("all_nulls", 100), 1, 2)))
        .as("Should read: in on all nulls column")
        .isFalse();

    assertThat(shouldRead(notIn(bucket("all_nulls", 100), 1, 2)))
        .as("Should read: notIn on all nulls column")
        .isTrue();
  }

  @Test
  public void testRequiredWithNonOrderPreserving() {
    assertThat(shouldRead(isNull(bucket("ts", 100))))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(notNull(bucket("ts", 100))))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(lessThan(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(lessThanOrEqual(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(greaterThan(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(greaterThanOrEqual(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(equal(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(notEqual(bucket("ts", 100), 30)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(in(bucket("ts", 100), 1, 2)))
        .as("Should read: non-order-preserving transform")
        .isTrue();

    assertThat(shouldRead(notIn(bucket("ts", 100), 1, 2)))
        .as("Should read: non-order-preserving transform")
        .isTrue();
  }

  @Test
  public void testAllNulls() {
    assertThat(shouldRead(isNull(truncate("all_nulls", 10))))
        .as("Should read: null values exist in all null column")
        .isTrue();

    assertThat(shouldRead(notNull(truncate("all_nulls", 10))))
        .as("Should skip: no non-null value in all null column")
        .isFalse();

    assertThat(shouldRead(lessThan(truncate("all_nulls", 10), 30)))
        .as("Should skip: lessThan on all null column")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(truncate("all_nulls", 10), 30)))
        .as("Should skip: lessThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThan(truncate("all_nulls", 10), 30)))
        .as("Should skip: greaterThan on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(truncate("all_nulls", 10), 30)))
        .as("Should skip: greaterThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(equal(truncate("all_nulls", 10), 30)))
        .as("Should skip: equal on all null column")
        .isFalse();

    assertThat(shouldRead(notEqual(truncate("all_nulls", 10), 30)))
        .as("Should read: notEqual on all null column")
        .isTrue();

    assertThat(shouldRead(in(truncate("all_nulls", 10), 10, 20)))
        .as("Should skip: in on all nulls column")
        .isFalse();

    assertThat(shouldRead(notIn(truncate("all_nulls", 10), 10, 20)))
        .as("Should read: notIn on all nulls column")
        .isTrue();

    assertThat(shouldRead(startsWith(truncate("all_nulls_str", 10), "a")))
        .as("Should skip: startsWith on all null column")
        .isTrue(); // depends on rewriting the expression, not evaluation

    assertThat(shouldRead(notStartsWith(truncate("all_nulls_str", 10), "a")))
        .as("Should read: notStartsWith on all null column")
        .isTrue();
  }

  @Test
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(lessThan(truncate("missing", 10), 20)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing'");
  }

  private static final Expression[] MISSING_STATS_EXPRESSIONS =
      new Expression[] {
        lessThan(truncate("no_stats", 10), 5),
        lessThanOrEqual(truncate("no_stats", 10), 30),
        equal(truncate("no_stats", 10), 70),
        greaterThan(truncate("no_stats", 10), 78),
        greaterThanOrEqual(truncate("no_stats", 10), 90),
        notEqual(truncate("no_stats", 10), 101),
        isNull(truncate("no_stats", 10)),
        notNull(truncate("no_stats", 10))
      };

  @ParameterizedTest
  @FieldSource("MISSING_STATS_EXPRESSIONS")
  public void testMissingStats(Expression expr) {
    assertThat(shouldRead(expr)).as("Should read when missing stats for expr: " + expr).isTrue();
  }

  @ParameterizedTest
  @FieldSource("MISSING_STATS_EXPRESSIONS")
  public void testZeroRecordFile(Expression expr) {
    DataFile empty = new TestDataFile("file.parquet", Row.of(), 0);
    assertThat(shouldRead(expr, empty)).as("Should never read 0-record file: " + expr).isFalse();
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    assertThat(shouldRead(not(lessThan(day("ts"), INT_MIN_VALUE - 25))))
        .as("Should read: not(false)")
        .isTrue();

    assertThat(shouldRead(not(greaterThan(day("ts"), INT_MIN_VALUE - 25))))
        .as("Should skip: not(true)")
        .isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    assertThat(
            shouldRead(
                and(
                    lessThan(day("ts"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(day("ts"), INT_MIN_VALUE - 30))))
        .as("Should skip: and(false, true)")
        .isFalse();

    assertThat(
            shouldRead(
                and(
                    lessThan(day("ts"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(day("ts"), INT_MAX_VALUE + 1))))
        .as("Should skip: and(false, false)")
        .isFalse();

    assertThat(
            shouldRead(
                and(
                    greaterThan(day("ts"), INT_MIN_VALUE - 25),
                    lessThanOrEqual(day("ts"), INT_MIN_VALUE))))
        .as("Should read: and(true, true)")
        .isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    assertThat(
            shouldRead(
                or(
                    lessThan(day("ts"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(day("ts"), INT_MAX_VALUE + 1))))
        .as("Should skip: or(false, false)")
        .isFalse();

    assertThat(
            shouldRead(
                or(
                    lessThan(day("ts"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(day("ts"), INT_MAX_VALUE - 19))))
        .as("Should read: or(false, true)")
        .isTrue();
  }

  @Test
  public void testIntegerLt() {
    assertThat(shouldRead(lessThan(day("ts"), INT_MIN_VALUE - 25)))
        .as("Should not read: id range below lower bound (5 < 30)")
        .isFalse();

    assertThat(shouldRead(lessThan(day("ts"), INT_MIN_VALUE)))
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    assertThat(shouldRead(lessThan(day("ts"), INT_MIN_VALUE + 1)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(lessThan(day("ts"), INT_MAX_VALUE)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    assertThat(shouldRead(lessThanOrEqual(day("ts"), INT_MIN_VALUE - 25)))
        .as("Should not read: id range below lower bound (5 < 30)")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(day("ts"), INT_MIN_VALUE - 1)))
        .as("Should not read: id range below lower bound (29 < 30)")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(day("ts"), INT_MIN_VALUE)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(lessThanOrEqual(day("ts"), INT_MAX_VALUE)))
        .as("Should read: many possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerGt() {
    assertThat(shouldRead(greaterThan(day("ts"), INT_MAX_VALUE + 6)))
        .as("Should not read: id range above upper bound (85 < 79)")
        .isFalse();

    assertThat(shouldRead(greaterThan(day("ts"), INT_MAX_VALUE)))
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    assertThat(shouldRead(greaterThan(day("ts"), INT_MAX_VALUE - 1)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(greaterThan(day("ts"), INT_MAX_VALUE - 4)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    assertThat(shouldRead(greaterThanOrEqual(day("ts"), INT_MAX_VALUE + 6)))
        .as("Should not read: id range above upper bound (85 < 79)")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(day("ts"), INT_MAX_VALUE + 1)))
        .as("Should not read: id range above upper bound (80 > 79)")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(day("ts"), INT_MAX_VALUE)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(greaterThanOrEqual(day("ts"), INT_MAX_VALUE - 4)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerEq() {
    assertThat(shouldRead(equal(day("ts"), INT_MIN_VALUE - 25)))
        .as("Should not read: id below lower bound")
        .isFalse();

    assertThat(shouldRead(equal(day("ts"), INT_MIN_VALUE - 1)))
        .as("Should not read: id below lower bound")
        .isFalse();

    assertThat(shouldRead(equal(day("ts"), INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(equal(day("ts"), INT_MAX_VALUE - 4)))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(equal(day("ts"), INT_MAX_VALUE)))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(equal(day("ts"), INT_MAX_VALUE + 1)))
        .as("Should not read: id above upper bound")
        .isFalse();

    assertThat(shouldRead(equal(day("ts"), INT_MAX_VALUE + 6)))
        .as("Should not read: id above upper bound")
        .isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    assertThat(shouldRead(notEqual(day("ts"), INT_MIN_VALUE - 25)))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MIN_VALUE - 1)))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MAX_VALUE - 4)))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MAX_VALUE)))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MAX_VALUE + 1)))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(shouldRead(notEqual(day("ts"), INT_MAX_VALUE + 6)))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    assertThat(shouldRead(not(equal(day("ts"), INT_MIN_VALUE - 25))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MIN_VALUE - 1))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MIN_VALUE))))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MAX_VALUE - 4))))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MAX_VALUE))))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MAX_VALUE + 1))))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(shouldRead(not(equal(day("ts"), INT_MAX_VALUE + 6))))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MIN_VALUE - 25))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MIN_VALUE - 1))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MIN_VALUE))))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MAX_VALUE - 4))))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MAX_VALUE))))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MAX_VALUE + 1))))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(shouldReadCaseInsensitive(not(equal(day("TS"), INT_MAX_VALUE + 6))))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    assertThatThrownBy(() -> shouldRead(not(equal(day("TS"), 5))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'TS'");
  }

  @Test
  public void testStringStartsWith() {
    assertThat(shouldRead(startsWith(truncate("str", 10), "a")))
        .as("Should read: not rewritten")
        .isTrue();

    assertThat(shouldRead(startsWith(truncate("str", 10), "ab")))
        .as("Should read: not rewritten")
        .isTrue();

    assertThat(shouldRead(startsWith(truncate("str", 10), "b")))
        .as("Should read: not rewritten")
        .isTrue();
  }

  @Test
  public void testStringNotStartsWith() {
    assertThat(shouldRead(startsWith(truncate("str", 10), "a")))
        .as("Should read: not rewritten")
        .isTrue();

    assertThat(shouldRead(startsWith(truncate("str", 10), "ab")))
        .as("Should read: not rewritten")
        .isTrue();

    assertThat(shouldRead(startsWith(truncate("str", 10), "b")))
        .as("Should read: not rewritten")
        .isTrue();
  }

  @Test
  public void testIntegerIn() {
    assertThat(shouldRead(in(day("ts"), INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)))
        .as("Should not read: id below lower bound (5 < 30, 6 < 30)")
        .isFalse();

    assertThat(shouldRead(in(day("ts"), INT_MIN_VALUE - 2, INT_MIN_VALUE - 1)))
        .as("Should not read: id below lower bound (28 < 30, 29 < 30)")
        .isFalse();

    assertThat(shouldRead(in(day("ts"), INT_MIN_VALUE - 1, INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound (30 == 30)")
        .isTrue();

    assertThat(shouldRead(in(day("ts"), INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)))
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    assertThat(shouldRead(in(day("ts"), INT_MAX_VALUE, INT_MAX_VALUE + 1)))
        .as("Should read: id equal to upper bound (79 == 79)")
        .isTrue();

    assertThat(shouldRead(in(day("ts"), INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)))
        .as("Should not read: id above upper bound (80 > 79, 81 > 79)")
        .isFalse();

    assertThat(shouldRead(in(day("ts"), INT_MAX_VALUE + 6, INT_MAX_VALUE + 7)))
        .as("Should not read: id above upper bound (85 > 79, 86 > 79)")
        .isFalse();

    // should read as the number of elements in the in expression is too big
    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }
    assertThat(shouldRead(in(day("ts"), ids))).as("Should read: large in expression").isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    assertThat(shouldRead(notIn(day("ts"), INT_MIN_VALUE - 25, INT_MIN_VALUE - 24)))
        .as("Should read: id below lower bound (5 < 30, 6 < 30)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MIN_VALUE - 2, INT_MIN_VALUE - 1)))
        .as("Should read: id below lower bound (28 < 30, 29 < 30)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MIN_VALUE - 1, INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound (30 == 30)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)))
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MAX_VALUE, INT_MAX_VALUE + 1)))
        .as("Should read: id equal to upper bound (79 == 79)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)))
        .as("Should read: id above upper bound (80 > 79, 81 > 79)")
        .isTrue();

    assertThat(shouldRead(notIn(day("ts"), INT_MAX_VALUE + 6, INT_MAX_VALUE + 7)))
        .as("Should read: id above upper bound (85 > 79, 86 > 79)")
        .isTrue();
  }
}
