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
import static org.apache.iceberg.expressions.Expressions.contains;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notContains;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.util.UnicodeUtil;
import org.junit.jupiter.api.Test;

public class TestInclusiveMetricsEvaluator<F> {
  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "no_stats", Types.IntegerType.get()),
          required(3, "required", Types.StringType.get()),
          optional(4, "all_nulls", Types.StringType.get()),
          optional(5, "some_nulls", Types.StringType.get()),
          optional(6, "no_nulls", Types.StringType.get()),
          optional(7, "all_nans", Types.DoubleType.get()),
          optional(8, "some_nans", Types.FloatType.get()),
          optional(9, "no_nans", Types.FloatType.get()),
          optional(10, "all_nulls_double", Types.DoubleType.get()),
          optional(11, "all_nans_v1_stats", Types.FloatType.get()),
          optional(12, "nan_and_null_only", Types.DoubleType.get()),
          optional(13, "no_nan_stats", Types.DoubleType.get()),
          optional(14, "some_empty", Types.StringType.get()));

  protected static final Schema NESTED_SCHEMA =
      new Schema(
          required(
              100,
              "required_address",
              Types.StructType.of(
                  required(102, "required_street1", Types.StringType.get()),
                  optional(103, "optional_street1", Types.StringType.get()))),
          optional(
              101,
              "optional_address",
              Types.StructType.of(
                  required(104, "required_street2", Types.StringType.get()),
                  optional(105, "optional_street2", Types.StringType.get()))));

  protected static final Schema FLOAT_SCHEMA = new Schema(required(1, "f", Types.FloatType.get()));

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  private static final DataFile FILE =
      new TestDataFile(
          "file.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.<Integer, Long>builder()
              .put(4, 50L)
              .put(5, 50L)
              .put(6, 50L)
              .put(7, 50L)
              .put(8, 50L)
              .put(9, 50L)
              .put(10, 50L)
              .put(11, 50L)
              .put(12, 50L)
              .put(13, 50L)
              .put(14, 50L)
              .buildOrThrow(),
          // null value counts
          ImmutableMap.<Integer, Long>builder()
              .put(4, 50L)
              .put(5, 10L)
              .put(6, 0L)
              .put(10, 50L)
              .put(11, 0L)
              .put(12, 1L)
              .put(14, 0L)
              .buildOrThrow(),
          // nan value counts
          ImmutableMap.of(
              7, 50L,
              8, 10L,
              9, 0L),
          // lower bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
              11, toByteBuffer(Types.FloatType.get(), Float.NaN),
              12, toByteBuffer(Types.DoubleType.get(), Double.NaN),
              14, toByteBuffer(Types.StringType.get(), "")),
          // upper bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
              11, toByteBuffer(Types.FloatType.get(), Float.NaN),
              12, toByteBuffer(Types.DoubleType.get(), Double.NaN),
              14, toByteBuffer(Types.StringType.get(), "房东整租霍营小区二层两居室")));

  private static final DataFile FILE_2 =
      new TestDataFile(
          "file_2.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(3, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "aa")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "dC")));

  private static final DataFile FILE_3 =
      new TestDataFile(
          "file_3.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(3, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "1str1")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "3str3")));

  private static final DataFile FILE_4 =
      new TestDataFile(
          "file_4.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(3, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "イロハニホヘト")));

  private static final DataFile FILE_5 =
      new TestDataFile(
          "file_5.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(3, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abcdefghi")));

  private static final DataFile FILE_6 =
      new TestDataFile(
          "file_6.avro",
          Row.of(),
          10,
          // any value counts, including nulls
          ImmutableMap.of(102, 5L, 103, 5L, 104, 5L, 105, 5L),
          // null value counts
          ImmutableMap.of(103, 5L, 104, 5L, 105, 5L),
          // nan value counts
          null,
          // lower bounds
          null,
          // upper bounds
          null);

  private static final DataFile MISSING_STATS = new TestDataFile("file.parquet", Row.of(), 50);

  private static final DataFile EMPTY_FILE = new TestDataFile("file.parquet", Row.of(), 0);

  private static final DataFile RANGE_OF_VALUES =
      new TestDataFile(
          "range_of_values.avro",
          Row.of(),
          10,
          ImmutableMap.of(3, 10L),
          ImmutableMap.of(3, 0L),
          null,
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "aaa")),
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "zzz")));

  private static final DataFile SINGLE_VALUE_FILE =
      new TestDataFile(
          "single_value.avro",
          Row.of(),
          10,
          ImmutableMap.of(3, 10L),
          ImmutableMap.of(3, 0L),
          null,
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")),
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")));

  // some_empty is optional because a required column cannot contain nulls
  private static final DataFile SINGLE_VALUE_WITH_NULLS =
      new TestDataFile(
          "single_value_nulls.avro",
          Row.of(),
          10,
          ImmutableMap.of(14, 10L),
          ImmutableMap.of(14, 2L),
          null,
          ImmutableMap.of(14, toByteBuffer(StringType.get(), "abc")),
          ImmutableMap.of(14, toByteBuffer(StringType.get(), "abc")));

  private static final DataFile SINGLE_VALUE_WITH_NAN =
      new TestDataFile(
          "single_value_nan.avro",
          Row.of(),
          10,
          ImmutableMap.of(9, 10L),
          ImmutableMap.of(9, 0L),
          ImmutableMap.of(9, 2L),
          ImmutableMap.of(9, toByteBuffer(Types.FloatType.get(), 5.0F)),
          ImmutableMap.of(9, toByteBuffer(Types.FloatType.get(), 5.0F)));

  private static final DataFile SINGLE_VALUE_NAN_BOUNDS =
      new TestDataFile(
          "single_value_nan_bounds.avro",
          Row.of(),
          10,
          ImmutableMap.of(9, 10L),
          ImmutableMap.of(9, 0L),
          ImmutableMap.of(9, 0L),
          ImmutableMap.of(9, toByteBuffer(Types.FloatType.get(), Float.NaN)),
          ImmutableMap.of(9, toByteBuffer(Types.FloatType.get(), Float.NaN)));

  private static final Map<Integer, ByteBuffer> FLOAT_BOUND =
      ImmutableMap.of(1, toByteBuffer(Types.FloatType.get(), 1.0f));

  private static final DataFile SINGLE_FLOAT_VALUE_FILE =
      new TestDataFile(
          "single_value_file.avro",
          Row.of(),
          10,
          ImmutableMap.of(1, 10L),
          ImmutableMap.of(1, 0L),
          ImmutableMap.of(1, 0L),
          FLOAT_BOUND,
          FLOAT_BOUND);

  private static final DataFile SINGLE_FLOAT_VALUE_FILE_WITH_NAN =
      new TestDataFile(
          "single_value_file.avro",
          Row.of(),
          10,
          ImmutableMap.of(1, 10L),
          ImmutableMap.of(1, 0L),
          ImmutableMap.of(1, 1L), // contains a NaN value
          FLOAT_BOUND,
          FLOAT_BOUND);

  protected boolean shouldRead(Schema schema, Expression expr, boolean caseSensitive, F testFile) {
    return new InclusiveMetricsEvaluator(schema, expr, caseSensitive).eval((DataFile) testFile);
  }

  protected boolean shouldRead(Schema schema, Expression expr, F testFile) {
    return shouldRead(schema, expr, true, testFile);
  }

  protected F file() {
    return asFile(FILE);
  }

  protected F file2() {
    return asFile(FILE_2);
  }

  protected F file3() {
    return asFile(FILE_3);
  }

  protected F file4() {
    return asFile(FILE_4);
  }

  protected F file5() {
    return asFile(FILE_5);
  }

  protected F file6() {
    return asFile(FILE_6);
  }

  protected F missingStats() {
    return asFile(MISSING_STATS);
  }

  protected F emptyFile() {
    return asFile(EMPTY_FILE);
  }

  protected F rangeOfValues() {
    return asFile(RANGE_OF_VALUES);
  }

  protected F singleValueFile() {
    return asFile(SINGLE_VALUE_FILE);
  }

  protected F singleValueWithNulls() {
    return asFile(SINGLE_VALUE_WITH_NULLS);
  }

  protected F singleValueWithNaN() {
    return asFile(SINGLE_VALUE_WITH_NAN);
  }

  protected F singleValueNaNBounds() {
    return asFile(SINGLE_VALUE_NAN_BOUNDS);
  }

  protected F singleFloatValueFile() {
    return asFile(SINGLE_FLOAT_VALUE_FILE);
  }

  protected F singleFloatValueFileWithNaN() {
    return asFile(SINGLE_FLOAT_VALUE_FILE_WITH_NAN);
  }

  @SuppressWarnings("unchecked")
  private F asFile(DataFile dataFile) {
    return (F) dataFile;
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = shouldRead(SCHEMA, notNull("all_nulls"), file());
    assertThat(shouldRead).as("Should skip: no non-null value in all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: lessThan on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: lessThanOrEqual on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: greaterThan on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: greaterThanOrEqual on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: equal on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should skip: startsWith on all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, notStartsWith("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should read: notStartsWith on all null column").isTrue();

    shouldRead = shouldRead(SCHEMA, notNull("some_nulls"), file());
    assertThat(shouldRead)
        .as("Should read: column with some nulls contains a non-null value")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notNull("no_nulls"), file());
    assertThat(shouldRead).as("Should read: non-null column contains a non-null value").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(SCHEMA, isNull("all_nulls"), file());
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = shouldRead(SCHEMA, isNull("some_nulls"), file());
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = shouldRead(SCHEMA, isNull("no_nulls"), file());
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = shouldRead(SCHEMA, isNaN("all_nans"), file());
    assertThat(shouldRead).as("Should read: at least one nan value in all nan column").isTrue();

    shouldRead = shouldRead(SCHEMA, isNaN("some_nans"), file());
    assertThat(shouldRead).as("Should read: at least one nan value in some nan column").isTrue();

    shouldRead = shouldRead(SCHEMA, isNaN("no_nans"), file());
    assertThat(shouldRead).as("Should skip: no-nans column contains no nan values").isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("all_nulls_double"), file());
    assertThat(shouldRead).as("Should skip: all-null column doesn't contain nan value").isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("no_nan_stats"), file());
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, isNaN("all_nans_v1_stats"), file());
    assertThat(shouldRead).as("Should read: at least one nan value in all nan column").isTrue();

    shouldRead = shouldRead(SCHEMA, isNaN("nan_and_null_only"), file());
    assertThat(shouldRead)
        .as("Should read: at least one nan value in nan and nulls only column")
        .isTrue();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(SCHEMA, notNaN("all_nans"), file());
    assertThat(shouldRead)
        .as("Should skip: column with all nans will not contain non-nan")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, notNaN("some_nans"), file());
    assertThat(shouldRead)
        .as("Should read: at least one non-nan value in some nan column")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("no_nans"), file());
    assertThat(shouldRead).as("Should read: at least one non-nan value in no nan column").isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("all_nulls_double"), file());
    assertThat(shouldRead)
        .as("Should read: at least one non-nan value in all null column")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("no_nan_stats"), file());
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("all_nans_v1_stats"), file());
    assertThat(shouldRead)
        .as("Should read: no guarantee on if contains nan value without nan stats")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("nan_and_null_only"), file());
    assertThat(shouldRead)
        .as("Should read: at least one null value in nan and nulls only column")
        .isTrue();
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(SCHEMA, notNull("required"), file());
    assertThat(shouldRead).as("Should read: required columns are always non-null").isTrue();

    shouldRead = shouldRead(SCHEMA, isNull("required"), file());
    assertThat(shouldRead).as("Should skip: required columns are always non-null").isFalse();
  }

  @Test
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(SCHEMA, lessThan("missing", 5), file()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing'");
  }

  @Test
  public void testMissingStats() {
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
      boolean shouldRead = shouldRead(SCHEMA, expr, missingStats());
      assertThat(shouldRead).as("Should read when missing stats for expr: " + expr).isTrue();
    }
  }

  @Test
  public void testZeroRecordFile() {
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
      boolean shouldRead = shouldRead(SCHEMA, expr, emptyFile());
      assertThat(shouldRead).as("Should never read 0-record file: " + expr).isFalse();
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead = shouldRead(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            SCHEMA,
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)),
            file());
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)),
            file());
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)),
            file());
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            SCHEMA,
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)),
            file());
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)),
            file());
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE), file());
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE + 1), file());
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE + 6), file());
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE), file());
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1), file());
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 6), file());
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(SCHEMA, equal("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE + 6), file());
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE + 6), file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE - 1)), file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE)), file());
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE - 4)), file());
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE)), file());
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE + 1)), file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE + 6)), file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MIN_VALUE - 25)), false, file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MIN_VALUE - 1)), false, file());
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MIN_VALUE)), false, file());
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MAX_VALUE - 4)), false, file());
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MAX_VALUE)), false, file());
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MAX_VALUE + 1)), false, file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("ID", INT_MAX_VALUE + 6)), false, file());
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    assertThatThrownBy(() -> shouldRead(SCHEMA, not(equal("ID", 5)), true, file()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "a"), true, file());
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "a"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "aa"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "aaa"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "1s"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "1str1x"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "ff"), true, file4());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "aB"), true, file2());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "dWX"), true, file2());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "5"), true, file3());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "3str3x"), true, file3());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("some_empty", "房东整租霍"), true, file());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, startsWith("all_nulls", ""), true, file());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = shouldRead(SCHEMA, startsWith("required", aboveMax), true, file4());
    assertThat(shouldRead).as("Should not read: range doesn't match").isFalse();
  }

  @Test
  public void testStringNotStartsWith() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "a"), true, file());
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "a"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "aa"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "aaa"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "1s"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "1str1x"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "ff"), true, file4());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "aB"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "dWX"), true, file2());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "5"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "3str3x"), true, file3());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = shouldRead(SCHEMA, notStartsWith("required", aboveMax), true, file4());
    assertThat(shouldRead).as("Should read: range matches").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "abc"), true, file5());
    assertThat(shouldRead).as("Should not read: all strings start with prefix").isFalse();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "abcd"), true, file5());
    assertThat(shouldRead).as("Should not read: lower shorter than prefix, cannot match").isTrue();
  }

  @Test
  public void testStringContains() {
    boolean shouldRead = shouldRead(SCHEMA, contains("required", "a"), true, file());
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("required", "a"), true, file2());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("required", "aB"), true, file2());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("required", "dWX"), true, file2());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("required", "5"), true, file3());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("required", "3str3x"), true, file3());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, contains("all_nulls", ""), true, file());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = shouldRead(SCHEMA, contains("required", aboveMax), true, file4());
    assertThat(shouldRead).as("Should read: contains never skips").isTrue();
  }

  @Test
  public void testStringNotContains() {
    boolean shouldRead = shouldRead(SCHEMA, notContains("required", "a"), true, file());
    assertThat(shouldRead).as("Should read: no stats").isTrue();

    shouldRead = shouldRead(SCHEMA, notContains("required", "a"), true, file2());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, notContains("required", "aB"), true, file2());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, notContains("required", "5"), true, file3());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();

    String aboveMax = UnicodeUtil.truncateStringMax(Literal.of("イロハニホヘト"), 4).value().toString();
    shouldRead = shouldRead(SCHEMA, notContains("required", aboveMax), true, file4());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, notContains("required", "abc"), true, file5());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();

    shouldRead = shouldRead(SCHEMA, notContains("required", "abcd"), true, file5());
    assertThat(shouldRead).as("Should read: notContains never skips").isTrue();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead =
        shouldRead(SCHEMA, in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), file());
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), file());
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), file());
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7), file());
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead = shouldRead(SCHEMA, in("all_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, in("some_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead = shouldRead(SCHEMA, in("no_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();

    // should read as the number of elements in the in expression is too big
    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }
    shouldRead = shouldRead(SCHEMA, in("id", ids), file());
    assertThat(shouldRead).as("Should read: large in expression").isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead =
        shouldRead(SCHEMA, notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), file());
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), file());
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), file());
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7), file());
    assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("all_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("some_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("no_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should read: notIn on no nulls column").isTrue();
  }

  @Test
  public void testIsNullInNestedStruct() {
    // read required_address and its nested fields
    boolean shouldRead = shouldRead(NESTED_SCHEMA, isNull("required_address"), file6());
    assertThat(shouldRead).as("Should not read: required_address is required").isFalse();

    shouldRead = shouldRead(NESTED_SCHEMA, isNull("required_address.required_street1"), file6());
    assertThat(shouldRead)
        .as("Should not read: required_address.required_street1 is required")
        .isFalse();

    shouldRead = shouldRead(NESTED_SCHEMA, isNull("required_address.optional_street1"), file6());
    assertThat(shouldRead)
        .as("Should read: required_address.optional_street1 is optional")
        .isTrue();

    // read optional_address and its nested fields
    shouldRead = shouldRead(NESTED_SCHEMA, isNull("optional_address"), file6());
    assertThat(shouldRead).as("Should read: optional_address is optional").isTrue();

    shouldRead = shouldRead(NESTED_SCHEMA, isNull("optional_address.required_street2"), file6());
    assertThat(shouldRead).as("Should read: optional_address is optional").isTrue();

    shouldRead = shouldRead(NESTED_SCHEMA, isNull("optional_address.optional_street2"), file6());
    assertThat(shouldRead).as("Should read: optional_address is optional").isTrue();
  }

  @Test
  public void testNotNullInNestedStruct() {
    // read required_address and its nested fields
    boolean shouldRead = shouldRead(NESTED_SCHEMA, notNull("required_address"), file6());
    assertThat(shouldRead).as("Should read: required_address is required").isTrue();

    shouldRead = shouldRead(NESTED_SCHEMA, notNull("required_address.required_street1"), file6());
    assertThat(shouldRead)
        .as("Should read: required_address.required_street1 is required")
        .isTrue();

    shouldRead = shouldRead(NESTED_SCHEMA, notNull("required_address.optional_street1"), file6());
    assertThat(shouldRead)
        .as("Should not read: required_address.optional_street1 is optional")
        .isFalse();

    // read optional_address and its nested fields
    shouldRead = shouldRead(NESTED_SCHEMA, notNull("optional_address"), file6());
    assertThat(shouldRead).as("Should read: metrics are not tracked for structs").isTrue();

    shouldRead = shouldRead(NESTED_SCHEMA, notNull("optional_address.required_street2"), file6());
    assertThat(shouldRead).as("Should not read: optional_address is optional").isFalse();

    shouldRead = shouldRead(NESTED_SCHEMA, notNull("optional_address.optional_street2"), file6());
    assertThat(shouldRead)
        .as("Should not read: optional_address.optional_street2 is optional")
        .isFalse();
  }

  @Test
  public void testNotEqSingleValueWithoutNaN() {
    assertThat(shouldRead(FLOAT_SCHEMA, notEqual("f", 1.0f), singleFloatValueFile()))
        .as("Should skip: file contains no values not equal to 1.0")
        .isFalse();
  }

  @Test
  public void testNotEqSingleValueWithNaN() {
    assertThat(shouldRead(FLOAT_SCHEMA, notEqual("f", 1.0f), singleFloatValueFileWithNaN()))
        .as("Should read: file contains a NaN value not equal to 1.0")
        .isTrue();
  }

  @Test
  public void testNotEqWithSingleValue() {
    boolean shouldRead = shouldRead(SCHEMA, notEqual("required", "aaa"), rangeOfValues());
    assertThat(shouldRead)
        .as("Should read: file has range of values, cannot exclude based on literal")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("required", "abc"), singleValueFile());
    assertThat(shouldRead)
        .as("Should not read: file contains single value equal to literal")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, notEqual("required", "def"), singleValueFile());
    assertThat(shouldRead)
        .as("Should read: file contains single value not equal to literal")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("some_empty", "abc"), singleValueWithNulls());
    assertThat(shouldRead).as("Should read: file has nulls which match != predicate").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("no_nans", 5.0F), singleValueWithNaN());
    assertThat(shouldRead).as("Should read: file has NaN values which match != predicate").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("no_nans", 5.0F), singleValueNaNBounds());
    assertThat(shouldRead).as("Should read: bounds are NaN").isTrue();
  }

  @Test
  public void testNotInWithSingleValue() {
    boolean shouldRead = shouldRead(SCHEMA, notIn("required", "aaa", "bbb"), rangeOfValues());
    assertThat(shouldRead)
        .as("Should read: file has range of values, cannot exclude based on literal")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("required", "abc", "def"), singleValueFile());
    assertThat(shouldRead)
        .as("Should not read: file contains single value in exclusion list")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, notIn("required", "def", "ghi"), singleValueFile());
    assertThat(shouldRead)
        .as("Should read: file contains single value not in exclusion list")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("some_empty", "abc", "def"), singleValueWithNulls());
    assertThat(shouldRead).as("Should read: file has nulls which match NOT IN predicate").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("no_nans", 5.0F, 10.0F), singleValueWithNaN());
    assertThat(shouldRead)
        .as("Should read: file has NaN values which match NOT IN predicate")
        .isTrue();
  }
}
