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
package org.apache.iceberg.data;

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
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class MetricsRowGroupFilterTestBase {

  protected static final Types.StructType STRUCT_FIELD_TYPE =
      Types.StructType.of(Types.NestedField.required(7, "int_field", Types.IntegerType.get()));

  protected static final Types.StructType UNDERSCORE_STRUCT_FIELD_TYPE =
      Types.StructType.of(Types.NestedField.required(7, "_int_field", Types.IntegerType.get()));

  protected static final Schema BASE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "required", Types.StringType.get()),
          optional(3, "all_nulls", Types.DoubleType.get()),
          optional(4, "some_nulls", Types.StringType.get()),
          optional(5, "no_nulls", Types.StringType.get()),
          optional(6, "struct_not_null", STRUCT_FIELD_TYPE),
          optional(8, "not_in_file", Types.FloatType.get()),
          optional(9, "str", Types.StringType.get()),
          optional(
              10,
              "map_not_null",
              Types.MapType.ofRequired(11, 12, Types.StringType.get(), Types.IntegerType.get())),
          optional(13, "all_nans", Types.DoubleType.get()),
          optional(14, "some_nans", Types.FloatType.get()),
          optional(15, "no_nans", Types.DoubleType.get()),
          optional(16, "some_double_nans", Types.DoubleType.get()));

  protected static final Schema BASE_FILE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "_id", Types.IntegerType.get()),
          Types.NestedField.required(2, "_required", Types.StringType.get()),
          Types.NestedField.optional(3, "_all_nulls", Types.DoubleType.get()),
          Types.NestedField.optional(4, "_some_nulls", Types.StringType.get()),
          Types.NestedField.optional(5, "_no_nulls", Types.StringType.get()),
          Types.NestedField.optional(6, "_struct_not_null", UNDERSCORE_STRUCT_FIELD_TYPE),
          Types.NestedField.optional(9, "_str", Types.StringType.get()),
          Types.NestedField.optional(13, "_all_nans", Types.DoubleType.get()),
          Types.NestedField.optional(14, "_some_nans", Types.FloatType.get()),
          Types.NestedField.optional(15, "_no_nans", Types.DoubleType.get()),
          Types.NestedField.optional(16, "_some_double_nans", Types.DoubleType.get()));

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  @TempDir protected File tempDir;

  @BeforeEach
  public final void before() throws IOException {
    createInputFile();
  }

  protected abstract void createInputFile() throws IOException;

  protected void populateBaseFields(GenericRecord record, int rowIndex) {
    record.setField("_id", INT_MIN_VALUE + rowIndex);
    record.setField("_required", "req");
    record.setField("_all_nulls", null);
    record.setField("_some_nulls", (rowIndex % 10 == 0) ? null : "some");
    record.setField("_no_nulls", "");
    record.setField("_all_nans", Double.NaN);
    record.setField("_some_nans", (rowIndex % 10 == 0) ? Float.NaN : 2F);
    record.setField("_some_double_nans", (rowIndex % 10 == 0) ? Double.NaN : 2D);
    record.setField("_no_nans", 3D);
    record.setField("_str", rowIndex + "str" + rowIndex);
    GenericRecord struct = GenericRecord.create(UNDERSCORE_STRUCT_FIELD_TYPE);
    struct.setField("_int_field", INT_MIN_VALUE + rowIndex);
    record.setField("_struct_not_null", struct);
  }

  protected boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  protected abstract boolean shouldRead(Expression expression, boolean caseSensitive);

  @Test
  public void testAllNulls() {
    boolean shouldRead;

    shouldRead = shouldRead(notNull("all_nulls"));
    assertThat(shouldRead).as("Should skip: no non-null value in all null column").isFalse();

    shouldRead = shouldRead(notNull("some_nulls"));
    assertThat(shouldRead)
        .as("Should read: column with some nulls contains a non-null value")
        .isTrue();

    shouldRead = shouldRead(notNull("no_nulls"));
    assertThat(shouldRead).as("Should read: non-null column contains a non-null value").isTrue();

    shouldRead = shouldRead(notNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(notNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = shouldRead(isNull("some_nulls"));
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = shouldRead(isNull("no_nulls"));
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();

    shouldRead = shouldRead(isNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(isNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @Test
  public void testFloatWithNan() {
    boolean shouldRead = shouldRead(greaterThan("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThan("some_nans", 3.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(equal("some_nans", 2.0));
    assertThat(shouldRead).isTrue();
  }

  @Test
  public void testDoubleWithNan() {
    boolean shouldRead = shouldRead(greaterThan("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead)
        .as("Should read: column with some nans contains the target value")
        .isTrue();

    shouldRead = shouldRead(lessThan("some_double_nans", 3.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(equal("some_double_nans", 2.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = shouldRead(isNaN("all_nans"));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(isNaN("some_nans"));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(isNaN("all_nulls"));
    assertThat(shouldRead).as("Should skip: all null column will not contain NaN").isFalse();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(notNaN("all_nans"));
    assertThat(shouldRead).as("Should read: NaN counts are not tracked in metrics").isTrue();

    shouldRead = shouldRead(notNaN("some_nans"));
    assertThat(shouldRead).as("Should read: NaN counts are not tracked in metrics").isTrue();

    shouldRead = shouldRead(notNaN("no_nans"));
    assertThat(shouldRead).as("Should read: NaN counts are not tracked in metrics").isTrue();

    shouldRead = shouldRead(notNaN("all_nulls"));
    assertThat(shouldRead).as("Shou  ld read: NaN counts are not tracked in metrics").isTrue();
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(notNull("required"));
    assertThat(shouldRead).as("Should read: required columns are always non-null").isTrue();

    shouldRead = shouldRead(isNull("required"));
    assertThat(shouldRead).as("Should skip: required columns are always non-null").isFalse();
  }

  @Test
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(lessThan("missing", 5)))
        .as("Should complain about missing column in expression")
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'missing'");
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        shouldRead(
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead =
        shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead =
        shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();
  }

  @Test
  public void testIntegerNotIn() {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();
  }

  @Test
  public void testSomeNullsNotEq() {
    boolean shouldRead = shouldRead(notEqual("some_nulls", "some"));
    assertThat(shouldRead).as("Should read: notEqual on some nulls column").isTrue();
  }
}
