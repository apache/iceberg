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

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

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
    boolean shouldRead = ManifestEvaluator.forRowFilter(notNull("all_nulls"), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: no non-null value in all null column", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notNull("some_nulls"), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a non-null value", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notNull("no_nulls"), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: non-null column contains a non-null value", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("all_nulls", "asad"), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: startsWith on all null column", shouldRead);
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(isNull("all_nulls"), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: at least one null value in all null column", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(isNull("some_nulls"), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: column with some nulls contains a null value", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(isNull("no_nulls"), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: non-null column contains no null values", shouldRead);
  }

  @Test
  public void testMissingColumn() {
    AssertHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'missing'",
        () -> ManifestEvaluator.forRowFilter(lessThan("missing", 5), SPEC, true).eval(FILE));
  }

  @Test
  public void testMissingStats() {
    Expression[] exprs = new Expression[] {
        lessThan("id", 5), lessThanOrEqual("id", 30), equal("id", 70),
        greaterThan("id", 78), greaterThanOrEqual("id", 90), notEqual("id", 101),
        isNull("id"), notNull("id"), startsWith("all_nulls", "a")
    };

    for (Expression expr : exprs) {
      boolean shouldRead = ManifestEvaluator.forRowFilter(expr, SPEC, true).eval(NO_STATS);
      Assert.assertTrue("Should read when missing stats for expr: " + expr, shouldRead);
    }
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = ManifestEvaluator.forRowFilter(not(lessThan("id", 5)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: not(false)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(greaterThan("id", 5)), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: not(true)", shouldRead);
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = ManifestEvaluator.forRowFilter(
        and(lessThan("id", 5), greaterThanOrEqual("id", 0)), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: and(false, false)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(
        and(greaterThan("id", 5), lessThanOrEqual("id", 30)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: and(true, true)", shouldRead);
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = ManifestEvaluator.forRowFilter(
        or(lessThan("id", 5), greaterThanOrEqual("id", 80)), SPEC, true).eval(FILE);
    Assert.assertFalse("Should skip: or(false, false)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(
        or(lessThan("id", 5), greaterThanOrEqual("id", 60)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: or(false, true)", shouldRead);
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(lessThan("id", 5), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThan("id", 30), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (30 is not < 30)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThan("id", 31), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThan("id", 79), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(lessThanOrEqual("id", 5), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (5 < 30)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThanOrEqual("id", 29), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range below lower bound (29 < 30)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThanOrEqual("id", 30), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(lessThanOrEqual("id", 79), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: many possible ids", shouldRead);
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(greaterThan("id", 85), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThan("id", 79), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (79 is not > 79)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThan("id", 78), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThan("id", 75), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", 85), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (85 < 79)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", 80), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id range above upper bound (80 > 79)", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", 79), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: one possible id", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(greaterThanOrEqual("id", 75), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: may possible ids", shouldRead);
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(equal("id", 5), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 29), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 30), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 75), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 79), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 80), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(equal("id", 85), SPEC, true).eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 5), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 29), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 30), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 75), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 79), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 80), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(notEqual("id", 85), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 5)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 29)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 30)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 75)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 79)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 80)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("id", 85)), SPEC, true).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseInsensitiveIntegerNotEqRewritten() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 5)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 29)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id below lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 30)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 75)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id between lower and upper bounds", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 79)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 80)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(not(equal("ID", 85)), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: id above upper bound", shouldRead);
  }

  @Test
  public void testCaseSensitiveIntegerNotEqRewritten() {
    AssertHelpers.assertThrows("Should complain about missing column in expression",
        ValidationException.class, "Cannot find field 'ID'",
        () -> ManifestEvaluator.forRowFilter(not(equal("ID", 5)), SPEC, true).eval(FILE));
  }

  @Test
  public void testStringStartsWith() {
    boolean shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "a"), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "aa"), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "dddd"), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "z"), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("no_nulls", "a"), SPEC, false).eval(FILE);
    Assert.assertTrue("Should read: range matches", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "zzzz"), SPEC, false).eval(FILE);
    Assert.assertFalse("Should skip: range doesn't match", shouldRead);

    shouldRead = ManifestEvaluator.forRowFilter(startsWith("some_nulls", "1"), SPEC, false).eval(FILE);
    Assert.assertFalse("Should skip: range doesn't match", shouldRead);
  }
}
