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
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Test;

public class TestStrictMetricsEvaluator<F> {
  protected static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "no_stats", IntegerType.get()),
          required(3, "required", StringType.get()),
          optional(4, "all_nulls", StringType.get()),
          optional(5, "some_nulls", StringType.get()),
          optional(6, "no_nulls", StringType.get()),
          required(7, "always_5", IntegerType.get()),
          optional(8, "all_nans", Types.DoubleType.get()),
          optional(9, "some_nans", Types.FloatType.get()),
          optional(10, "no_nans", Types.FloatType.get()),
          optional(11, "all_nulls_double", Types.DoubleType.get()),
          optional(12, "all_nans_v1_stats", Types.FloatType.get()),
          optional(13, "nan_and_null_only", Types.DoubleType.get()),
          optional(14, "no_nan_stats", Types.DoubleType.get()),
          optional(
              15,
              "struct",
              Types.StructType.of(
                  Types.NestedField.optional(16, "nested_col_no_stats", Types.IntegerType.get()),
                  Types.NestedField.optional(17, "nested_col_with_stats", Types.IntegerType.get()),
                  Types.NestedField.optional(18, "nested_string_col", Types.StringType.get()))));

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
              .put(8, 50L)
              .put(9, 50L)
              .put(10, 50L)
              .put(11, 50L)
              .put(12, 50L)
              .put(13, 50L)
              .put(14, 50L)
              .put(17, 50L)
              .buildOrThrow(),
          // null value counts
          ImmutableMap.<Integer, Long>builder()
              .put(4, 50L)
              .put(5, 10L)
              .put(6, 0L)
              .put(11, 50L)
              .put(12, 0L)
              .put(13, 1L)
              .put(17, 0L)
              .buildOrThrow(),
          // nan value counts
          ImmutableMap.of(
              8, 50L,
              9, 10L,
              10, 0L),
          // lower bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
              7, toByteBuffer(IntegerType.get(), 5),
              12, toByteBuffer(Types.FloatType.get(), Float.NaN),
              13, toByteBuffer(Types.DoubleType.get(), Double.NaN),
              17, toByteBuffer(Types.IntegerType.get(), INT_MIN_VALUE)),
          // upper bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
              7, toByteBuffer(IntegerType.get(), 5),
              12, toByteBuffer(Types.FloatType.get(), Float.NaN),
              13, toByteBuffer(Types.DoubleType.get(), Double.NaN),
              17, toByteBuffer(IntegerType.get(), INT_MAX_VALUE)));

  private static final DataFile FILE_2 =
      new TestDataFile(
          "file_2.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(
              4, 50L,
              5, 50L,
              6, 50L,
              8, 50L),
          // null value counts
          ImmutableMap.of(
              4, 50L,
              5, 10L,
              6, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")),
          // upper bounds
          ImmutableMap.of(5, toByteBuffer(StringType.get(), "eee")));

  private static final DataFile FILE_3 =
      new TestDataFile(
          "file_3.avro",
          Row.of(),
          50,
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
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")),
          // upper bounds
          ImmutableMap.of(5, toByteBuffer(StringType.get(), "bbb")));

  // String-focused file: required column 3 has no nulls and string bounds ["abc", "abd"]
  private static final DataFile STRING_FILE =
      new TestDataFile(
          "string_file.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abc")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "abd")));

  // String file with wider range: required column 3 has no nulls and bounds ["aa", "dC"]
  private static final DataFile STRING_FILE_2 =
      new TestDataFile(
          "string_file_2.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(3, 50L),
          // null value counts
          ImmutableMap.of(),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "aa")),
          // upper bounds
          ImmutableMap.of(3, toByteBuffer(StringType.get(), "dC")));

  // Int columns with bounds, but without any null value counts
  private static final DataFile MISSING_NULL_COUNTS_FILE =
      new TestDataFile(
          "missing_null_counts.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(1, 50L, 2, 50L),
          // null value counts
          null,
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
              2, toByteBuffer(IntegerType.get(), INT_MIN_VALUE)),
          // upper bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
              2, toByteBuffer(IntegerType.get(), INT_MAX_VALUE)));

  // Int columns with bounds, where null value counts are tracked only for another column
  private static final DataFile PARTIAL_NULL_COUNTS_FILE =
      new TestDataFile(
          "partial_null_counts.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(1, 50L, 2, 50L),
          // null value counts
          ImmutableMap.of(3, 0L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MIN_VALUE),
              2, toByteBuffer(IntegerType.get(), INT_MIN_VALUE)),
          // upper bounds
          ImmutableMap.of(
              1, toByteBuffer(IntegerType.get(), INT_MAX_VALUE),
              2, toByteBuffer(IntegerType.get(), INT_MAX_VALUE)));

  // Float columns without nulls, where NaN counts are tracked only for column 10 (no_nans)
  private static final DataFile FLOAT_FILE =
      new TestDataFile(
          "float_file.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(10, 50L, 14, 50L),
          // null value counts
          ImmutableMap.of(10, 0L, 14, 0L),
          // nan value counts
          ImmutableMap.of(10, 0L),
          // lower bounds
          ImmutableMap.of(
              10, toByteBuffer(Types.FloatType.get(), 1.0F),
              14, toByteBuffer(Types.DoubleType.get(), 1.0D)),
          // upper bounds
          ImmutableMap.of(
              10, toByteBuffer(Types.FloatType.get(), 5.0F),
              14, toByteBuffer(Types.DoubleType.get(), 5.0D)));

  private static final DataFile MISSING_STATS = new TestDataFile("file.parquet", Row.of(), 50);

  private static final DataFile EMPTY_FILE = new TestDataFile("file.parquet", Row.of(), 0);

  protected boolean shouldRead(Schema schema, Expression expr, F testFile) {
    return new StrictMetricsEvaluator(schema, expr).eval((DataFile) testFile);
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

  protected F stringFile() {
    return asFile(STRING_FILE);
  }

  protected F stringFile2() {
    return asFile(STRING_FILE_2);
  }

  protected F missingNullCountsFile() {
    return asFile(MISSING_NULL_COUNTS_FILE);
  }

  protected F partialNullCountsFile() {
    return asFile(PARTIAL_NULL_COUNTS_FILE);
  }

  protected F floatFile() {
    return asFile(FLOAT_FILE);
  }

  protected F missingStats() {
    return asFile(MISSING_STATS);
  }

  protected F emptyFile() {
    return asFile(EMPTY_FILE);
  }

  @SuppressWarnings("unchecked")
  private F asFile(DataFile dataFile) {
    return (F) dataFile;
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead = shouldRead(SCHEMA, notNull("all_nulls"), file());
    assertThat(shouldRead).as("Should not match: no non-null value in all null column").isFalse();

    shouldRead = shouldRead(SCHEMA, notNull("some_nulls"), file());
    assertThat(shouldRead)
        .as("Should not match: column with some nulls contains a non-null value")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, notNull("no_nulls"), file());
    assertThat(shouldRead).as("Should match: non-null column contains no null values").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should match: notEqual on all nulls column").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(SCHEMA, isNull("all_nulls"), file());
    assertThat(shouldRead).as("Should match: all values are null").isTrue();

    shouldRead = shouldRead(SCHEMA, isNull("some_nulls"), file());
    assertThat(shouldRead).as("Should not match: not all values are null").isFalse();

    shouldRead = shouldRead(SCHEMA, isNull("no_nulls"), file());
    assertThat(shouldRead).as("Should not match: no values are null").isFalse();
  }

  @Test
  public void testSomeNulls() {
    boolean shouldRead = shouldRead(SCHEMA, lessThan("some_nulls", "ggg"), file2());
    assertThat(shouldRead).as("Should not match: lessThan on some nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("some_nulls", "eee"), file2());
    assertThat(shouldRead).as("Should not match: lessThanOrEqual on some nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("some_nulls", "aaa"), file2());
    assertThat(shouldRead).as("Should not match: greaterThan on some nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("some_nulls", "bbb"), file2());
    assertThat(shouldRead)
        .as("Should not match: greaterThanOrEqual on some nulls column")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, equal("some_nulls", "bbb"), file3());
    assertThat(shouldRead).as("Should not match: equal on some nulls column").isFalse();
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = shouldRead(SCHEMA, isNaN("all_nans"), file());
    assertThat(shouldRead).as("Should match: all values are nan").isTrue();

    shouldRead = shouldRead(SCHEMA, isNaN("some_nans"), file());
    assertThat(shouldRead)
        .as("Should not match: at least one non-nan value in some nan column")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("no_nans"), file());
    assertThat(shouldRead)
        .as("Should not match: at least one non-nan value in no nan column")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("all_nulls_double"), file());
    assertThat(shouldRead)
        .as("Should not match: at least one non-nan value in all null column")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("no_nan_stats"), file());
    assertThat(shouldRead).as("Should not match: cannot determine without nan stats").isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("all_nans_v1_stats"), file());
    assertThat(shouldRead).as("Should not match: cannot determine without nan stats").isFalse();

    shouldRead = shouldRead(SCHEMA, isNaN("nan_and_null_only"), file());
    assertThat(shouldRead).as("Should not match: null values are not nan").isFalse();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(SCHEMA, notNaN("all_nans"), file());
    assertThat(shouldRead).as("Should not match: all values are nan").isFalse();

    shouldRead = shouldRead(SCHEMA, notNaN("some_nans"), file());
    assertThat(shouldRead)
        .as("Should not match: at least one nan value in some nan column")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, notNaN("no_nans"), file());
    assertThat(shouldRead).as("Should match: no value is nan").isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("all_nulls_double"), file());
    assertThat(shouldRead).as("Should match: no nan value in all null column").isTrue();

    shouldRead = shouldRead(SCHEMA, notNaN("no_nan_stats"), file());
    assertThat(shouldRead).as("Should not match: cannot determine without nan stats").isFalse();

    shouldRead = shouldRead(SCHEMA, notNaN("all_nans_v1_stats"), file());
    assertThat(shouldRead).as("Should not match: all values are nan").isFalse();

    shouldRead = shouldRead(SCHEMA, notNaN("nan_and_null_only"), file());
    assertThat(shouldRead).as("Should not match: null values are not nan").isFalse();
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(SCHEMA, notNull("required"), file());
    assertThat(shouldRead).as("Should match: required columns are always non-null").isTrue();

    shouldRead = shouldRead(SCHEMA, isNull("required"), file());
    assertThat(shouldRead).as("Should not match: required columns never contain null").isFalse();
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
          isNaN("all_nans"),
          notNaN("all_nans")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(SCHEMA, expr, missingStats());
      assertThat(shouldRead)
          .as("Should never match when stats are missing for expr: " + expr)
          .isFalse();
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
          isNaN("all_nans"),
          notNaN("all_nans")
        };

    for (Expression expr : exprs) {
      boolean shouldRead = shouldRead(SCHEMA, expr, emptyFile());
      assertThat(shouldRead).as("Should always match 0-record file: " + expr).isTrue();
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(SCHEMA, not(lessThan("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should not match: not(false)").isTrue();

    shouldRead = shouldRead(SCHEMA, not(greaterThan("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should match: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            SCHEMA,
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)),
            file());
    assertThat(shouldRead).as("Should not match: range may not overlap data").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)),
            file());
    assertThat(shouldRead).as("Should not match: range does not overlap data").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            and(lessThan("id", INT_MAX_VALUE + 6), greaterThanOrEqual("id", INT_MIN_VALUE - 30)),
            file());
    assertThat(shouldRead).as("Should match: range includes all data").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            SCHEMA,
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)),
            file());
    assertThat(shouldRead).as("Should not match: no matching values").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)),
            file());
    assertThat(shouldRead).as("Should not match: some values do not match").isFalse();

    shouldRead =
        shouldRead(
            SCHEMA,
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE)),
            file());
    assertThat(shouldRead).as("Should match: all values match >= 30").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: always false").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: 32 and greater not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should not match: 79 not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should match: all values in range").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should not match: always false").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: 31 and greater not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should match: all values in range").isTrue();

    shouldRead = shouldRead(SCHEMA, lessThanOrEqual("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should match: all values in range").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should not match: always false").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MAX_VALUE - 1), file());
    assertThat(shouldRead).as("Should not match: 77 and less not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: 30 not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThan("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should match: all values in range").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: no values in range").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should not match: 78 and lower are not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MIN_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: 30 not in range").isFalse();

    shouldRead = shouldRead(SCHEMA, greaterThanOrEqual("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should match: all values in range").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(SCHEMA, equal("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should not match: all values != 5").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: some values != 30").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should not match: some values != 75").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should not match: some values != 79").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: some values != 80").isFalse();

    shouldRead = shouldRead(SCHEMA, equal("always_5", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should match: all values == 5").isTrue();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE - 25), file());
    assertThat(shouldRead).as("Should match: no values == 5").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE - 1), file());
    assertThat(shouldRead).as("Should match: no values == 39").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: some value may be == 30").isFalse();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE - 4), file());
    assertThat(shouldRead).as("Should not match: some value may be == 75").isFalse();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("Should not match: some value may be == 79").isFalse();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should match: no values == 80").isTrue();

    shouldRead = shouldRead(SCHEMA, notEqual("id", INT_MAX_VALUE + 6), file());
    assertThat(shouldRead).as("Should read: no values == 85").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE - 25)), file());
    assertThat(shouldRead).as("Should match: no values == 5").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE - 1)), file());
    assertThat(shouldRead).as("Should match: no values == 39").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MIN_VALUE)), file());
    assertThat(shouldRead).as("Should not match: some value may be == 30").isFalse();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE - 4)), file());
    assertThat(shouldRead).as("Should not match: some value may be == 75").isFalse();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE)), file());
    assertThat(shouldRead).as("Should not match: some value may be == 79").isFalse();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE + 1)), file());
    assertThat(shouldRead).as("Should match: no values == 80").isTrue();

    shouldRead = shouldRead(SCHEMA, not(equal("id", INT_MAX_VALUE + 6)), file());
    assertThat(shouldRead).as("Should read: no values == 85").isTrue();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead =
        shouldRead(SCHEMA, in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), file());
    assertThat(shouldRead).as("Should not match: all values != 5 and != 6").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: some values != 30 and != 31").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), file());
    assertThat(shouldRead).as("Should not match: some values != 75 and != 76").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: some values != 78 and != 79").isFalse();

    shouldRead = shouldRead(SCHEMA, in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), file());
    assertThat(shouldRead).as("Should not match: some values != 80 and != 81)").isFalse();

    shouldRead = shouldRead(SCHEMA, in("always_5", 5, 6), file());
    assertThat(shouldRead).as("Should match: all values == 5").isTrue();

    shouldRead = shouldRead(SCHEMA, in("all_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should not match: in on all nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, in("some_nulls", "abc", "def"), file3());
    assertThat(shouldRead).as("Should not match: in on some nulls column").isFalse();

    shouldRead = shouldRead(SCHEMA, in("no_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should not match: no_nulls field does not have bounds").isFalse();
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead =
        shouldRead(SCHEMA, notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24), file());
    assertThat(shouldRead).as("Should match: all values !=5 and !=6").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE), file());
    assertThat(shouldRead).as("Should not match: some values may be == 30").isFalse();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3), file());
    assertThat(shouldRead).as("Should not match: some value may be == 75 or == 76").isFalse();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1), file());
    assertThat(shouldRead).as("Should not match: some value may be == 79").isFalse();

    shouldRead = shouldRead(SCHEMA, notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2), file());
    assertThat(shouldRead).as("Should match: no values == 80 or == 81").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("always_5", 5, 6), file());
    assertThat(shouldRead).as("Should not match: all values == 5").isFalse();

    shouldRead = shouldRead(SCHEMA, notIn("all_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should match: notIn on all nulls column").isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("some_nulls", "abc", "def"), file3());
    assertThat(shouldRead)
        .as("Should match: notIn on some nulls column, 'bbb' > 'abc' and 'bbb' < 'def'")
        .isTrue();

    shouldRead = shouldRead(SCHEMA, notIn("no_nulls", "abc", "def"), file());
    assertThat(shouldRead).as("Should not match: no_nulls field does not have bounds").isFalse();
  }

  @Test
  public void testEvaluateOnNestedColumnWithoutStats() {
    boolean shouldRead =
        shouldRead(SCHEMA, greaterThanOrEqual("struct.nested_col_no_stats", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("greaterThanOrEqual nested column should not match").isFalse();

    shouldRead =
        shouldRead(SCHEMA, lessThanOrEqual("struct.nested_col_no_stats", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("lessThanOrEqual nested column should not match").isFalse();

    shouldRead = shouldRead(SCHEMA, isNull("struct.nested_col_no_stats"), file());
    assertThat(shouldRead).as("isNull nested column should not match").isFalse();

    shouldRead = shouldRead(SCHEMA, notNull("struct.nested_col_no_stats"), file());
    assertThat(shouldRead).as("notNull nested column should not match").isFalse();
  }

  @Test
  public void testEvaluateOnNestedColumnWithStats() {
    boolean shouldRead =
        shouldRead(
            SCHEMA, greaterThanOrEqual("struct.nested_col_with_stats", INT_MIN_VALUE), file());
    assertThat(shouldRead).as("greaterThanOrEqual nested column should not match").isFalse();

    shouldRead =
        shouldRead(SCHEMA, lessThanOrEqual("struct.nested_col_with_stats", INT_MAX_VALUE), file());
    assertThat(shouldRead).as("lessThanOrEqual nested column should not match").isFalse();

    shouldRead = shouldRead(SCHEMA, isNull("struct.nested_col_with_stats"), file());
    assertThat(shouldRead).as("isNull nested column should not match").isFalse();

    shouldRead = shouldRead(SCHEMA, notNull("struct.nested_col_with_stats"), file());
    assertThat(shouldRead).as("notNull nested column should not match").isFalse();
  }

  @Test
  public void testNotStartsWithAllNulls() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("all_nulls", "a"), file());
    assertThat(shouldRead).as("Should match: all null values satisfy notStartsWith").isTrue();
  }

  @Test
  public void testNotStartsWithBoundsAbovePrefix() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "aaa"), stringFile());
    assertThat(shouldRead).as("Should match: all values are above the prefix range").isTrue();
  }

  @Test
  public void testNotStartsWithBoundsBelowPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "zzz"), stringFile());
    assertThat(shouldRead).as("Should match: all values are below the prefix range").isTrue();
  }

  @Test
  public void testNotStartsWithBoundsOverlapPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "ab"), stringFile());
    assertThat(shouldRead).as("Should not match: bounds overlap the prefix range").isFalse();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "abc"), stringFile());
    assertThat(shouldRead).as("Should not match: lower bound starts with the prefix").isFalse();
  }

  @Test
  public void testNotStartsWithWiderRange() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "e"), stringFile2());
    assertThat(shouldRead).as("Should match: all values are below the prefix").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "a"), stringFile2());
    assertThat(shouldRead).as("Should not match: lower bound starts with the prefix").isFalse();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "c"), stringFile2());
    assertThat(shouldRead).as("Should not match: prefix is within the bounds range").isFalse();
  }

  @Test
  public void testNotStartsWithNoStats() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "a"), file());
    assertThat(shouldRead).as("Should not match: no bounds available for column").isFalse();
  }

  @Test
  void testStartsWithBothBoundsMatchPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "ab"), stringFile());
    assertThat(shouldRead).as("Should match: both bounds start with the prefix").isTrue();
  }

  @Test
  void testStartsWithSingleCharPrefixBothBoundsMatch() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "a"), stringFile());
    assertThat(shouldRead)
        .as("Should match: both bounds start with the single char prefix")
        .isTrue();
  }

  @Test
  void testStartsWithOnlyLowerBoundMatchesPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "abc"), stringFile());
    assertThat(shouldRead)
        .as("Should not match: upper bound does not start with the prefix")
        .isFalse();
  }

  @Test
  void testStartsWithBoundsDoNotMatchPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "zzz"), stringFile());
    assertThat(shouldRead).as("Should not match: no bounds start with the prefix").isFalse();
  }

  @Test
  void testStartsWithWiderRange() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "a"), stringFile2());
    assertThat(shouldRead)
        .as("Should not match: upper bound does not start with the prefix")
        .isFalse();

    shouldRead = shouldRead(SCHEMA, startsWith("required", "e"), stringFile2());
    assertThat(shouldRead).as("Should not match: no bounds start with the prefix").isFalse();
  }

  @Test
  void testStartsWithNoStats() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "a"), file());
    assertThat(shouldRead).as("Should not match: no bounds available for column").isFalse();
  }

  @Test
  public void testNotStartsWithSomeNullsBoundsOutsidePrefix() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("some_nulls", "zzz"), file2());
    assertThat(shouldRead).as("Should match: all values are below the prefix").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("some_nulls", "aaa"), file2());
    assertThat(shouldRead).as("Should match: all values are above the prefix").isTrue();
  }

  @Test
  public void testNotStartsWithPrefixLongerThanBounds() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", "aaaaaaa"), stringFile());
    assertThat(shouldRead).as("Should match: all values are above the long prefix").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "zzzzzzz"), stringFile());
    assertThat(shouldRead).as("Should match: all values are below the long prefix").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("required", "abcdef"), stringFile());
    assertThat(shouldRead).as("Should not match: prefix overlaps with bound range").isFalse();
  }

  @Test
  void testNotStartsWithEmptyPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("required", ""), stringFile());
    assertThat(shouldRead).as("Should not match: all strings start with empty prefix").isFalse();
  }

  @Test
  void testNotStartsWithExactBoundMatch() {
    // file3 has column 5 (some_nulls) with exact bounds ["bbb", "bbb"]
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("some_nulls", "bbb"), file3());
    assertThat(shouldRead).as("Should not match: bounds exactly equal the prefix").isFalse();

    shouldRead = shouldRead(SCHEMA, notStartsWith("some_nulls", "zzz"), file3());
    assertThat(shouldRead).as("Should match: all values are below the prefix").isTrue();

    shouldRead = shouldRead(SCHEMA, notStartsWith("some_nulls", "aaa"), file3());
    assertThat(shouldRead).as("Should match: all values are above the prefix").isTrue();
  }

  @Test
  public void testNotStartsWithNestedColumn() {
    boolean shouldRead = shouldRead(SCHEMA, notStartsWith("struct.nested_string_col", "a"), file());
    assertThat(shouldRead).as("notStartsWith nested column should not match").isFalse();
  }

  @Test
  void testStartsWithAllNulls() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("all_nulls", "a"), file());
    assertThat(shouldRead)
        .as("Should not match: all null values do not satisfy startsWith")
        .isFalse();
  }

  @Test
  void testStartsWithSomeNulls() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("some_nulls", "b"), file2());
    assertThat(shouldRead)
        .as("Should not match: some nulls means not all rows can satisfy startsWith")
        .isFalse();
  }

  @Test
  void testStartsWithPrefixLongerThanBounds() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", "abcdef"), stringFile());
    assertThat(shouldRead).as("Should not match: prefix is longer than the bounds").isFalse();
  }

  @Test
  void testStartsWithEmptyPrefix() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("required", ""), stringFile());
    assertThat(shouldRead).as("Should match: all strings start with empty prefix").isTrue();
  }

  @Test
  void testStartsWithNestedColumn() {
    boolean shouldRead = shouldRead(SCHEMA, startsWith("struct.nested_string_col", "a"), file());
    assertThat(shouldRead).as("Should not match: nested column is not supported").isFalse();
  }

  @Test
  void missingNullCounts() {
    boolean shouldRead =
        shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE + 1), missingNullCountsFile());
    assertThat(shouldRead).as("Should match: required column cannot contain nulls").isTrue();

    shouldRead =
        shouldRead(SCHEMA, lessThan("no_stats", INT_MAX_VALUE + 1), missingNullCountsFile());
    assertThat(shouldRead)
        .as("Should not match: optional column may contain nulls without a null count")
        .isFalse();
  }

  @Test
  void partialNullCounts() {
    boolean shouldRead =
        shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE + 1), partialNullCountsFile());
    assertThat(shouldRead).as("Should match: required column cannot contain nulls").isTrue();

    shouldRead =
        shouldRead(SCHEMA, lessThan("no_stats", INT_MAX_VALUE + 1), partialNullCountsFile());
    assertThat(shouldRead)
        .as("Should not match: optional column may contain nulls without a null count")
        .isFalse();
  }

  @Test
  void missingNaNCounts() {
    boolean shouldRead = shouldRead(SCHEMA, lessThan("no_nans", 10.0F), floatFile());
    assertThat(shouldRead).as("Should match: float column has no nulls and no NaNs").isTrue();

    shouldRead = shouldRead(SCHEMA, lessThan("no_nan_stats", 10.0D), floatFile());
    assertThat(shouldRead)
        .as("Should not match: float column may contain NaNs without a NaN count")
        .isFalse();
  }
}
