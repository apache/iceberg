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

package com.netflix.iceberg.expressions;

import com.google.common.collect.ImmutableList;
import com.netflix.iceberg.ManifestFile;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

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

public class TestInclusiveManifestEvaluator {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(4, "all_nulls", Types.StringType.get()),
      optional(5, "some_nulls", Types.StringType.get()),
      optional(6, "no_nulls", Types.StringType.get())
  );

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .withSpecId(0)
      .identity("id")
      .identity("all_nulls")
      .identity("some_nulls")
      .identity("no_nulls")
      .build();

  private static final ByteBuffer INT_MIN = toByteBuffer(Types.IntegerType.get(), 30);
  private static final ByteBuffer INT_MAX = toByteBuffer(Types.IntegerType.get(), 79);

  private static final ByteBuffer STRING_MIN = toByteBuffer(Types.StringType.get(), "a");
  private static final ByteBuffer STRING_MAX = toByteBuffer(Types.StringType.get(), "z");

  private static final ManifestFile NO_STATS = new TestHelpers.TestManifestFile(
      "manifest-list.avro", 1024, 0, System.currentTimeMillis(), null, null, null, null);

  private static final ManifestFile FILE = new TestHelpers.TestManifestFile("manifest-list.avro",
      1024, 0, System.currentTimeMillis(), 5, 10, 0, ImmutableList.of(
          new TestHelpers.TestFieldSummary(false, INT_MIN, INT_MAX),
          new TestHelpers.TestFieldSummary(true, null, null),
          new TestHelpers.TestFieldSummary(true, STRING_MIN, STRING_MAX),
          new TestHelpers.TestFieldSummary(false, STRING_MIN, STRING_MAX)));

  @Test
  public void testAllNulls() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, notNull("all_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notNull("no_nulls")).eval(FILE);
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, isNull("all_nulls")).eval(FILE);
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, isNull("some_nulls")).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, isNull("no_nulls")).eval(FILE);
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    TestHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> new InclusiveManifestEvaluator(SPEC, lessThan("missing", 5)).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70),
        greaterThan("id", 78), greaterThanOrEqual("id", 90), notEqual("id", 101),
        isNull("id"), notNull("id")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = new InclusiveManifestEvaluator(SPEC, expr).eval(NO_STATS);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, not(lessThan("id", 5))).eval(FILE);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(greaterThan("id", 5))).eval(FILE);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveManifestEvaluator(
        SPEC, and(lessThan("id", 5), greaterThanOrEqual("id", 0))).eval(FILE);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(
        SPEC, and(greaterThan("id", 5), lessThanOrEqual("id", 30))).eval(FILE);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = new InclusiveManifestEvaluator(
        SPEC, or(lessThan("id", 5), greaterThanOrEqual("id", 80))).eval(FILE);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(
        SPEC, or(lessThan("id", 5), greaterThanOrEqual("id", 60))).eval(FILE);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, lessThan("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThan("id", 30)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThan("id", 31)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThan("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, lessThanOrEqual("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThanOrEqual("id", 29)).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThanOrEqual("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, lessThanOrEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, greaterThan("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, greaterThan("id", 79)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, greaterThan("id", 78)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, greaterThan("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = new InclusiveManifestEvaluator(
        SPEC, greaterThanOrEqual("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(
        SPEC, greaterThanOrEqual("id", 80)).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(
        SPEC, greaterThanOrEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(
        SPEC, greaterThanOrEqual("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 5)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 29)).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 80)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, equal("id", 85)).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 5)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 29)).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 30)).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 75)).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 79)).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 80)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, notEqual("id", 85)).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 5))).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 29))).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 30))).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 75))).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 79))).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 80))).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("id", 85))).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 5)), false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 29)), false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 30)), false).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 75)), false).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 79)), false).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 80)), false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 85)), false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test(expected = ValidationException.class)
  public void testCaseSensitiveIntegerNotEqRewritten() {
    boolean shouldRead = new InclusiveManifestEvaluator(SPEC, not(equal("ID", 5)), true).eval(FILE);
  }
}
