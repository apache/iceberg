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
import static org.apache.iceberg.expressions.Expressions.extract;
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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestInclusiveMetricsEvaluatorWithExtract {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          required(2, "variant", Types.VariantType.get()),
          optional(3, "all_nulls", Types.VariantType.get()));

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 79;

  private static final DataFile FILE =
      new TestDataFile(
          "file.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.<Integer, Long>builder().put(1, 50L).put(2, 50L).put(3, 50L).buildOrThrow(),
          // null value counts
          ImmutableMap.<Integer, Long>builder()
              .put(1, 0L)
              .put(2, 0L)
              .put(3, 50L) // all_nulls
              .buildOrThrow(),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(
              2,
              VariantTestUtil.variantBuffer(
                  Map.of(
                      "$['event_id']",
                      Variants.of(INT_MIN_VALUE),
                      "$['str']",
                      Variants.of("abc")))),
          // upper bounds
          ImmutableMap.of(
              2,
              VariantTestUtil.variantBuffer(
                  Map.of(
                      "$['event_id']",
                      Variants.of(INT_MAX_VALUE),
                      "$['str']",
                      Variants.of("abe")))));

  private boolean shouldRead(Expression expr) {
    return shouldRead(expr, FILE);
  }

  private boolean shouldRead(Expression expr, DataFile file) {
    return shouldRead(expr, file, true);
  }

  private boolean shouldReadCaseInsensitive(Expression expr) {
    return shouldRead(expr, FILE, false);
  }

  private boolean shouldRead(Expression expr, DataFile file, boolean caseSensitive) {
    return new InclusiveMetricsEvaluator(SCHEMA, expr, caseSensitive).eval(file);
  }

  @Test
  public void testAllNulls() {
    assertThat(shouldRead(isNull(extract("all_nulls", "$.event_id", "long"))))
        .as("Should read: null values exist in all null column")
        .isTrue();

    assertThat(shouldRead(notNull(extract("all_nulls", "$.event_id", "long"))))
        .as("Should skip: no non-null value in all null column")
        .isFalse();

    assertThat(shouldRead(lessThan(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should skip: lessThan on all null column")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should skip: lessThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThan(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should skip: greaterThan on all null column")
        .isFalse();

    assertThat(shouldRead(greaterThanOrEqual(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should skip: greaterThanOrEqual on all null column")
        .isFalse();

    assertThat(shouldRead(equal(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should skip: equal on all null column")
        .isFalse();

    assertThat(shouldRead(notEqual(extract("all_nulls", "$.event_id", "long"), 30)))
        .as("Should read: notEqual on all null column")
        .isTrue();

    assertThat(shouldRead(in(extract("all_nulls", "$.event_type", "string"), "abc", "def")))
        .as("Should skip: in on all nulls column")
        .isFalse();

    assertThat(shouldRead(notIn(extract("all_nulls", "$.event_type", "string"), "abc", "def")))
        .as("Should read: notIn on all nulls column")
        .isTrue();

    assertThat(shouldRead(startsWith(extract("all_nulls", "$.event_type", "string"), "a")))
        .as("Should skip: startsWith on all null column")
        .isFalse();

    assertThat(shouldRead(notStartsWith(extract("all_nulls", "$.event_type", "string"), "a")))
        .as("Should read: notStartsWith on all null column")
        .isTrue();

    assertThat(shouldRead(isNaN(extract("all_nulls", "$.measurement", "double"))))
        .as("Should skip: no NaN value in all null column")
        .isFalse();

    assertThat(shouldRead(notNaN(extract("all_nulls", "$.measurement", "double"))))
        .as("Should read: all null column has non-NaN values")
        .isTrue();
  }

  @Test
  public void testIsNaNAndNotNaN() {
    assertThat(shouldRead(isNaN(extract("variant", "$.measurement", "double"))))
        .as("Should read: variant may contain NaN")
        .isTrue();

    assertThat(shouldRead(notNaN(extract("variant", "$.measurement", "double"))))
        .as("Should read: variant may contain non-NaN")
        .isTrue();
  }

  @Test
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(lessThan(extract("missing", "$", "int"), 5)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing'");
  }

  private static final Expression[] MISSING_STATS_EXPRESSIONS =
      new Expression[] {
        lessThan(extract("variant", "$.missing", "long"), 5),
        lessThanOrEqual(extract("variant", "$.missing", "long"), 30),
        equal(extract("variant", "$.missing", "long"), 70),
        greaterThan(extract("variant", "$.missing", "long"), 78),
        greaterThanOrEqual(extract("variant", "$.missing", "long"), 90),
        notEqual(extract("variant", "$.missing", "long"), 101),
        isNull(extract("variant", "$.missing", "string")),
        notNull(extract("variant", "$.missing", "string")),
        isNaN(extract("variant", "$.missing", "float")),
        notNaN(extract("variant", "$.missing", "float"))
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
    assertThat(
            shouldRead(not(lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25))))
        .as("Should read: not(false)")
        .isTrue();

    assertThat(
            shouldRead(
                not(greaterThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25))))
        .as("Should skip: not(true)")
        .isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    assertThat(
            shouldRead(
                and(
                    lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(
                        extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 30))))
        .as("Should skip: and(false, true)")
        .isFalse();

    assertThat(
            shouldRead(
                and(
                    lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(
                        extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1))))
        .as("Should skip: and(false, false)")
        .isFalse();

    assertThat(
            shouldRead(
                and(
                    greaterThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25),
                    lessThanOrEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE))))
        .as("Should read: and(true, true)")
        .isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    assertThat(
            shouldRead(
                or(
                    lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(
                        extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1))))
        .as("Should skip: or(false, false)")
        .isFalse();

    assertThat(
            shouldRead(
                or(
                    lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25),
                    greaterThanOrEqual(
                        extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 19))))
        .as("Should read: or(false, true)")
        .isTrue();
  }

  @Test
  public void testIntegerLt() {
    assertThat(shouldRead(lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25)))
        .as("Should not read: id range below lower bound (5 < 30)")
        .isFalse();

    assertThat(shouldRead(lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE)))
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    assertThat(shouldRead(lessThan(extract("variant", "$.event_id", "long"), INT_MIN_VALUE + 1)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(lessThan(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    assertThat(
            shouldRead(
                lessThanOrEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25)))
        .as("Should not read: id range below lower bound (5 < 30)")
        .isFalse();

    assertThat(
            shouldRead(
                lessThanOrEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1)))
        .as("Should not read: id range below lower bound (29 < 30)")
        .isFalse();

    assertThat(shouldRead(lessThanOrEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(lessThanOrEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should read: many possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerGt() {
    assertThat(shouldRead(greaterThan(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6)))
        .as("Should not read: id range above upper bound (85 < 79)")
        .isFalse();

    assertThat(shouldRead(greaterThan(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    assertThat(shouldRead(greaterThan(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 1)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(shouldRead(greaterThan(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    assertThat(
            shouldRead(
                greaterThanOrEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6)))
        .as("Should not read: id range above upper bound (85 < 79)")
        .isFalse();

    assertThat(
            shouldRead(
                greaterThanOrEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1)))
        .as("Should not read: id range above upper bound (80 > 79)")
        .isFalse();

    assertThat(
            shouldRead(greaterThanOrEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should read: one possible id")
        .isTrue();

    assertThat(
            shouldRead(
                greaterThanOrEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4)))
        .as("Should read: may possible ids")
        .isTrue();
  }

  @Test
  public void testIntegerEq() {
    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25)))
        .as("Should not read: id below lower bound")
        .isFalse();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1)))
        .as("Should not read: id below lower bound")
        .isFalse();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4)))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1)))
        .as("Should not read: id above upper bound")
        .isFalse();

    assertThat(shouldRead(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6)))
        .as("Should not read: id above upper bound")
        .isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25)))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1)))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4)))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE)))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1)))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(shouldRead(notEqual(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6)))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 25))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MIN_VALUE))))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4))))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE))))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1))))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(shouldRead(not(equal(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6))))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MIN_VALUE - 25))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MIN_VALUE - 1))))
        .as("Should read: id below lower bound")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MIN_VALUE))))
        .as("Should read: id equal to lower bound")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MAX_VALUE - 4))))
        .as("Should read: id between lower and upper bounds")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MAX_VALUE))))
        .as("Should read: id equal to upper bound")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MAX_VALUE + 1))))
        .as("Should read: id above upper bound")
        .isTrue();

    assertThat(
            shouldReadCaseInsensitive(
                not(equal(extract("VARIANT", "$.event_id", "long"), INT_MAX_VALUE + 6))))
        .as("Should read: id above upper bound")
        .isTrue();
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    assertThatThrownBy(() -> shouldRead(not(equal(extract("VARIANT", "$.event_id", "long"), 5))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'VARIANT'");
  }

  @Test
  public void testStringStartsWith() {
    assertThat(shouldRead(startsWith(extract("variant", "$.str", "string"), "a")))
        .as("Should read: prefix of lower and upper")
        .isTrue();

    assertThat(shouldRead(startsWith(extract("variant", "$.str", "string"), "ab")))
        .as("Should read: prefix of lower and upper")
        .isTrue();

    assertThat(shouldRead(startsWith(extract("variant", "$.str", "string"), "abcd")))
        .as("Should read: lower is prefix of value")
        .isTrue();

    assertThat(shouldRead(startsWith(extract("variant", "$.str", "string"), "abd")))
        .as("Should read: value is between upper and lower")
        .isTrue();

    assertThat(shouldRead(startsWith(extract("variant", "$.str", "string"), "abex")))
        .as("Should skip: upper is prefix of value")
        .isFalse();
  }

  @Test
  public void testStringNotStartsWith() {
    assertThat(shouldRead(notStartsWith(extract("variant", "$.str", "string"), "a")))
        .as("Should skip: prefix of lower and upper, all values must match")
        .isFalse();

    assertThat(shouldRead(notStartsWith(extract("variant", "$.str", "string"), "ab")))
        .as("Should skip: prefix of lower and upper, all values must match")
        .isFalse();

    assertThat(shouldRead(notStartsWith(extract("variant", "$.str", "string"), "abcd")))
        .as("Should skip: lower is prefix of value, some values do not match")
        .isTrue();

    assertThat(shouldRead(notStartsWith(extract("variant", "$.str", "string"), "abd")))
        .as("Should read: value is between upper and lower, some values do not match")
        .isTrue();

    assertThat(shouldRead(notStartsWith(extract("variant", "$.str", "string"), "abex")))
        .as("Should read: upper is prefix of value, some values do not match")
        .isTrue();
  }

  @Test
  public void testIntegerIn() {
    assertThat(
            shouldRead(
                in(
                    extract("variant", "$.event_id", "long"),
                    INT_MIN_VALUE - 25,
                    INT_MIN_VALUE - 24)))
        .as("Should not read: id below lower bound (5 < 30, 6 < 30)")
        .isFalse();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 2, INT_MIN_VALUE - 1)))
        .as("Should not read: id below lower bound (28 < 30, 29 < 30)")
        .isFalse();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1, INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound (30 == 30)")
        .isTrue();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MAX_VALUE - 4, INT_MAX_VALUE - 3)))
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MAX_VALUE, INT_MAX_VALUE + 1)))
        .as("Should read: id equal to upper bound (79 == 79)")
        .isTrue();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 1, INT_MAX_VALUE + 2)))
        .as("Should not read: id above upper bound (80 > 79, 81 > 79)")
        .isFalse();

    assertThat(
            shouldRead(
                in(extract("variant", "$.event_id", "long"), INT_MAX_VALUE + 6, INT_MAX_VALUE + 7)))
        .as("Should not read: id above upper bound (85 > 79, 86 > 79)")
        .isFalse();

    // should read as the number of elements in the in expression is too big
    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }
    assertThat(shouldRead(in(extract("variant", "$.event_id", "long"), ids)))
        .as("Should read: large in expression")
        .isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    assertThat(
            shouldRead(
                notIn(
                    extract("variant", "$.event_id", "long"),
                    INT_MIN_VALUE - 25,
                    INT_MIN_VALUE - 24)))
        .as("Should read: id below lower bound (5 < 30, 6 < 30)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(
                    extract("variant", "$.event_id", "long"),
                    INT_MIN_VALUE - 2,
                    INT_MIN_VALUE - 1)))
        .as("Should read: id below lower bound (28 < 30, 29 < 30)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(extract("variant", "$.event_id", "long"), INT_MIN_VALUE - 1, INT_MIN_VALUE)))
        .as("Should read: id equal to lower bound (30 == 30)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(
                    extract("variant", "$.event_id", "long"),
                    INT_MAX_VALUE - 4,
                    INT_MAX_VALUE - 3)))
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(extract("variant", "$.event_id", "long"), INT_MAX_VALUE, INT_MAX_VALUE + 1)))
        .as("Should read: id equal to upper bound (79 == 79)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(
                    extract("variant", "$.event_id", "long"),
                    INT_MAX_VALUE + 1,
                    INT_MAX_VALUE + 2)))
        .as("Should read: id above upper bound (80 > 79, 81 > 79)")
        .isTrue();

    assertThat(
            shouldRead(
                notIn(
                    extract("variant", "$.event_id", "long"),
                    INT_MAX_VALUE + 6,
                    INT_MAX_VALUE + 7)))
        .as("Should read: id above upper bound (85 > 79, 86 > 79)")
        .isTrue();
  }

  private static Stream<Arguments> timestampEqParameters() {
    return Stream.of(
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-03-21T00:00:01.123456789",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-03-21",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")));
  }

  @ParameterizedTest
  @MethodSource("timestampEqParameters")
  public void testTimestampEq(
      String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    upperBound)));

    DataFile file =
        new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = equal(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  private static Stream<Arguments> timestampNanoEqParameters() {
    return Stream.of(
        Arguments.of(
            Types.TimestampType.withoutZone().toString(),
            "1970-03-21T00:00:01.123456",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-03-21",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")));
  }

  @ParameterizedTest
  @MethodSource("timestampNanoEqParameters")
  public void testTimestampNanoEq(
      String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    upperBound)));

    DataFile file =
        new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = equal(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  private static Stream<Arguments> dateEqParameters() {
    return Stream.of(
        Arguments.of(
            Types.TimestampType.withoutZone().toString(),
            "1970-03-21T00:00:01.123456",
            Variants.ofIsoDate("1970-01-31"),
            Variants.ofIsoDate("1970-03-31")),
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-03-21T00:00:01.123456789",
            Variants.ofIsoDate("1970-01-31"),
            Variants.ofIsoDate("1970-03-31")));
  }

  @ParameterizedTest
  @MethodSource("dateEqParameters")
  public void testDateEq(
      String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    upperBound)));

    DataFile file =
        new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = equal(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  private static Stream<Arguments> timestampNotEqParameters() {
    return Stream.of(
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-03-01T00:00:01.123456",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-04-01T00:00:01.123456",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-03-01",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-04-01",
            Variants.ofIsoTimestampntz("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntz("1970-03-31T00:00:01.123456")));
  }

  @ParameterizedTest
  @MethodSource("timestampNotEqParameters")
  public void testTimestampNotEq(
      String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    upperBound)));

    DataFile file =
        new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = notEqual(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  private static Stream<Arguments> timestampNanoNotEqParameters() {
    return Stream.of(
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-03-01T00:00:01.123456",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.TimestampNanoType.withoutZone().toString(),
            "1970-04-01T00:00:01.123456",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-03-01",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")),
        Arguments.of(
            Types.DateType.get().toString(),
            "1970-04-01",
            Variants.ofIsoTimestampntzNanos("1970-01-31T00:00:01.123456"),
            Variants.ofIsoTimestampntzNanos("1970-03-31T00:00:01.123456")));
  }

  @ParameterizedTest
  @MethodSource("timestampNanoNotEqParameters")
  public void testTimestampNanoNotEq(
      String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
        ImmutableMap.of(
            2,
            VariantTestUtil.variantBuffer(
                Map.of(
                    "$['event_timestamp']",
                    upperBound)));

    DataFile file =
        new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = notEqual(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  private static Stream<Arguments> dateNotEqParameters() {
    return Stream.of(
            Arguments.of(
                    Types.TimestampType.withoutZone().toString(),
                    "1970-03-01T00:00:01.123456",
                    Variants.ofIsoDate("1970-01-31"),
                    Variants.ofIsoDate("1970-03-31")),
            Arguments.of(
                    Types.TimestampType.withoutZone().toString(),
                    "1970-04-01T00:00:01.123456",
                    Variants.ofIsoDate("1970-01-31"),
                    Variants.ofIsoDate("1970-03-31")),
            Arguments.of(
                    Types.TimestampNanoType.withoutZone().toString(),
                    "1970-03-01T00:00:01.123456789",
                    Variants.ofIsoDate("1970-01-31"),
                    Variants.ofIsoDate("1970-03-31")),
            Arguments.of(
                    Types.TimestampNanoType.withoutZone().toString(),
                    "1970-04-01T00:00:01.123456789",
                    Variants.ofIsoDate("1970-01-31"),
                    Variants.ofIsoDate("1970-03-31")));
  }

  @ParameterizedTest
  @MethodSource("dateNotEqParameters")
  public void testDateNotEq(
          String variantType, String literal, VariantValue lowerBound, VariantValue upperBound) {
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
            ImmutableMap.of(
                    2,
                    VariantTestUtil.variantBuffer(
                            Map.of(
                                    "$['event_timestamp']",
                                    lowerBound)));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
            ImmutableMap.of(
                    2,
                    VariantTestUtil.variantBuffer(
                            Map.of(
                                    "$['event_timestamp']",
                                    upperBound)));

    DataFile file =
            new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = notEqual(extract("variant", "$.event_timestamp", variantType), literal);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }

  @Test
  public void testUUIDEq() {
    UUID uuid = UUID.randomUUID();
    // lower bounds
    Map<Integer, ByteBuffer> lowerBounds =
            ImmutableMap.of(
                    2,
                    VariantTestUtil.variantBuffer(
                            Map.of(
                                    "$['event_uuid']",
                                    Variants.ofUUID(uuid))));
    // upper bounds
    Map<Integer, ByteBuffer> upperBounds =
            ImmutableMap.of(
                    2,
                    VariantTestUtil.variantBuffer(
                            Map.of(
                                    "$['event_uuid']",
                                    Variants.ofUUID(uuid))));
    DataFile file =
            new TestDataFile("file.parquet", Row.of(), 50, null, null, null, lowerBounds, upperBounds);
    Expression expr = equal(extract("variant", "$.event_uuid", PhysicalType.UUID.name()), uuid);
    assertThat(shouldRead(expr, file)).as("Should read: many possible timestamps" + expr).isTrue();
  }
}
