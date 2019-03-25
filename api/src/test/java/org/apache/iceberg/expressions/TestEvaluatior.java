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

import org.apache.avro.util.Utf8;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestEvaluatior {
  private static final StructType STRUCT = StructType.of(
      required(13, "x", Types.IntegerType.get()),
      required(14, "y", Types.IntegerType.get()),
      optional(15, "z", Types.IntegerType.get())
  );

  @Test
  public void testLessThan() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThan("x", 7));
    Assert.assertFalse("7 < 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 < 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
  }

  @Test
  public void testLessThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThanOrEqual("x", 7));
    Assert.assertTrue("7 <= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 <= 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertFalse("8 <= 7 => false", evaluator.eval(TestHelpers.Row.of(8, 8, null)));
  }

  @Test
  public void testGreaterThan() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThan("x", 7));
    Assert.assertFalse("7 > 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 > 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 > 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));
  }

  @Test
  public void testGreaterThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThanOrEqual("x", 7));
    Assert.assertTrue("7 >= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 >= 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 >= 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));
  }

  @Test
  public void testEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, equal("x", 7));
    Assert.assertTrue("7 == 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 == 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
  }

  @Test
  public void testNotEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, notEqual("x", 7));
    Assert.assertFalse("7 != 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 != 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
  }

  @Test
  public void testAlwaysTrue() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysTrue());
    Assert.assertTrue("always true", evaluator.eval(TestHelpers.Row.of()));
  }

  @Test
  public void testAlwaysFalse() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysFalse());
    Assert.assertFalse("always false", evaluator.eval(TestHelpers.Row.of()));
  }

  @Test
  public void testIsNull() {
    Evaluator evaluator = new Evaluator(STRUCT, isNull("z"));
    Assert.assertTrue("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertFalse("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));
  }

  @Test
  public void testNotNull() {
    Evaluator evaluator = new Evaluator(STRUCT, notNull("z"));
    Assert.assertFalse("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertTrue("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));
  }

  @Test
  public void testAnd() {
    Evaluator evaluator = new Evaluator(STRUCT, and(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertFalse("8, 3 => false", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertFalse("7, null => false", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));
  }

  @Test
  public void testOr() {
    Evaluator evaluator = new Evaluator(STRUCT, or(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertTrue("8, 3 => true", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertTrue("7, null => true", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));
  }

  @Test
  public void testNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("x", 7)));
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));
  }

  @Test
  public void testCaseInsensitiveNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("X", 7)), false);
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));
  }

  @Test
  public void testCaseSensitiveNot() {
    TestHelpers.assertThrows(
        "X != x when case sensitivity is on",
        ValidationException.class,
        "Cannot find field 'X' in struct",
        () -> new Evaluator(STRUCT, not(equal("X", 7)), true));
  }

  @Test
  public void testCharSeqValue() {
    StructType struct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, equal("s", "abc"));
    Assert.assertTrue("string(abc) == utf8(abc) => true",
        evaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertFalse("string(abc) == utf8(abcd) => false",
        evaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }
}
