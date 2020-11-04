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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
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
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestEvaluator {
  private static final StructType STRUCT = StructType.of(
      required(13, "x", Types.IntegerType.get()),
      required(14, "y", Types.DoubleType.get()),
      optional(15, "z", Types.IntegerType.get()),
      optional(16, "s1", Types.StructType.of(
          Types.NestedField.required(17, "s2", Types.StructType.of(
              Types.NestedField.required(18, "s3", Types.StructType.of(
                  Types.NestedField.required(19, "s4", Types.StructType.of(
                      Types.NestedField.required(20, "i", Types.IntegerType.get()))))))))));

  @Test
  public void testLessThan() {
    testLessThan(new Evaluator(STRUCT, lessThan("x", 7)),
        new Evaluator(STRUCT, lessThan("s1.s2.s3.s4.i", 7)));
  }

  @Test
  public void testLessThanWithUnboundTerm() {
    testLessThan(new Evaluator(STRUCT, lessThan(self("x"), 7)),
        new Evaluator(STRUCT, lessThan(self("s1.s2.s3.s4.i"), 7)));
  }

  private void testLessThan(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertFalse("7 < 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null, null)));
    Assert.assertTrue("6 < 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null, null)));

    Assert.assertFalse("7 < 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertTrue("6 < 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));
  }

  @Test
  public void testLessThanOrEqual() {
    testLessThanOrEqual(new Evaluator(STRUCT, lessThanOrEqual("x", 7)),
        new Evaluator(STRUCT, lessThanOrEqual("s1.s2.s3.s4.i", 7)));
  }

  @Test
  public void testLessThanOrEqualWithUnboundTerm() {
    testLessThanOrEqual(new Evaluator(STRUCT, lessThanOrEqual(self("x"), 7)),
        new Evaluator(STRUCT, lessThanOrEqual(self("s1.s2.s3.s4.i"), 7)));
  }

  private void testLessThanOrEqual(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertTrue("7 <= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 <= 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertFalse("8 <= 7 => false", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Assert.assertTrue("7 <= 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));

    Assert.assertTrue("6 <= 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));

    Assert.assertFalse("8 <= 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testGreaterThan() {
    testGreaterThan(new Evaluator(STRUCT, greaterThan("x", 7)),
        new Evaluator(STRUCT, greaterThan("s1.s2.s3.s4.i", 7)));
  }

  @Test
  public void testGreaterThanWithUnboundTerm() {
    testGreaterThan(new Evaluator(STRUCT, greaterThan(self("x"), 7)),
        new Evaluator(STRUCT, greaterThan(self("s1.s2.s3.s4.i"), 7)));
  }

  private void testGreaterThan(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertFalse("7 > 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 > 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 > 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Assert.assertFalse("7 > 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertFalse("6 > 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));
    Assert.assertTrue("8 > 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testGreaterThanOrEqual() {
    testGreaterThanOrEqual(new Evaluator(STRUCT, greaterThanOrEqual("x", 7)),
        new Evaluator(STRUCT, greaterThanOrEqual("s1.s2.s3.s4.i", 7)));
  }

  @Test
  public void testGreaterThanOrEqualWithUnboundTerm() {
    testGreaterThanOrEqual(new Evaluator(STRUCT, greaterThanOrEqual(self("x"), 7)),
        new Evaluator(STRUCT, greaterThanOrEqual(self("s1.s2.s3.s4.i"), 7)));
  }

  private void testGreaterThanOrEqual(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertTrue("7 >= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 >= 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 >= 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Assert.assertTrue("7 >= 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertFalse("6 >= 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));
    Assert.assertTrue("8 >= 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testEqual() {
    testEqual(new Evaluator(STRUCT, equal("x", 7)),
        new Evaluator(STRUCT, equal("s1.s2.s3.s4.i", 7)));
  }

  @Test
  public void testEqualWithUnboundTerm() {
    testEqual(new Evaluator(STRUCT, equal(self("x"), 7)),
        new Evaluator(STRUCT, equal(self("s1.s2.s3.s4.i"), 7)));
  }

  private void testEqual(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertEquals(1, equal("x", 5).literals().size());

    Assert.assertTrue("7 == 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 == 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));

    Assert.assertTrue("7 == 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertFalse("6 == 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));
  }

  @Test
  public void testNotEqual() {
    testNotEqual(notEqual("x", 7), notEqual("s1.s2.s3.s4.i", 7));
  }

  @Test
  public void testNotEqualWithUnboundTerm() {
    testNotEqual(notEqual(self("x"), 7), notEqual(self("s1.s2.s3.s4.i"), 7));
  }

  private void testNotEqual(UnboundPredicate<?> predicate, UnboundPredicate<?> structPredicate) {
    Assert.assertEquals(1, predicate.literals().size());

    Evaluator evaluator = new Evaluator(STRUCT, predicate);
    Assert.assertFalse("7 != 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 != 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, structPredicate);
    Assert.assertFalse("7 != 7 => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertTrue("6 != 7 => true",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));
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
    testIsNull(new Evaluator(STRUCT, isNull("z")),
        new Evaluator(STRUCT, isNull("s1.s2.s3.s4.i")));
  }

  @Test
  public void testIsNullWithUnboundTerm() {
    testIsNull(new Evaluator(STRUCT, isNull(self("z"))),
        new Evaluator(STRUCT, isNull(self("s1.s2.s3.s4.i"))));
  }

  private void testIsNull(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertTrue("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertFalse("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));

    Assert.assertFalse("3 is not null", structEvaluator.eval(TestHelpers.Row.of(1, 2, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(3)))))));
  }

  @Test
  public void testNotNull() {
    testNotNull(new Evaluator(STRUCT, notNull("z")),
        new Evaluator(STRUCT, notNull("s1.s2.s3.s4.i")));
  }

  @Test
  public void testNotNullWithUnboundTerm() {
    testNotNull(new Evaluator(STRUCT, notNull(self("z"))),
        new Evaluator(STRUCT, notNull(self("s1.s2.s3.s4.i"))));
  }

  private void testNotNull(Evaluator evaluator, Evaluator structEvaluator) {
    Assert.assertFalse("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertTrue("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));

    Assert.assertTrue("3 is not null", structEvaluator.eval(TestHelpers.Row.of(1, 2, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(3)))))));
  }

  @Test
  public void testAnd() {
    Evaluator evaluator = new Evaluator(STRUCT, and(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertFalse("8, 3 => false", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertFalse("7, null => false", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, and(equal("s1.s2.s3.s4.i", 7),
        notNull("s1.s2.s3.s4.i")));

    Assert.assertTrue("7, 7 => true", structEvaluator.eval(TestHelpers.Row.of(7, 0, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(7)))))));
    Assert.assertFalse("8, 8 => false", structEvaluator.eval(TestHelpers.Row.of(8, 0, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(8)))))));

    Assert.assertFalse("7, null => false", structEvaluator.eval(TestHelpers.Row.of(7, 0, null, null)));

    Assert.assertFalse("8, notnull => false", structEvaluator.eval(TestHelpers.Row.of(8, 0, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testOr() {
    Evaluator evaluator = new Evaluator(STRUCT, or(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertTrue("8, 3 => true", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertTrue("7, null => true", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));


    Evaluator structEvaluator = new Evaluator(STRUCT, or(equal("s1.s2.s3.s4.i", 7),
        notNull("s1.s2.s3.s4.i")));

    Assert.assertTrue("7, 7 => true", structEvaluator.eval(TestHelpers.Row.of(7, 0, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(7)))))));
    Assert.assertTrue("8, 8 => false", structEvaluator.eval(TestHelpers.Row.of(8, 0, 3,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(8)))))));

    Assert.assertTrue("7, notnull => false", structEvaluator.eval(TestHelpers.Row.of(7, 0, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(7)))))));
  }

  @Test
  public void testNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("x", 7)));
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)));
    Assert.assertFalse("not(7 == 7) => false", structEvaluator.eval(TestHelpers.Row.of(7, null, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(7)))))));
    Assert.assertTrue("not(8 == 7) => false", structEvaluator.eval(TestHelpers.Row.of(8, null, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testCaseInsensitiveNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("X", 7)), false);
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)), false);
    Assert.assertFalse("not(7 == 7) => false", structEvaluator.eval(TestHelpers.Row.of(7, null, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(7)))))));
    Assert.assertTrue("not(8 == 7) => false", structEvaluator.eval(TestHelpers.Row.of(8, null, null,
        TestHelpers.Row.of(
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testCaseSensitiveNot() {
    AssertHelpers.assertThrows(
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

  @Test
  public void testIn() {
    Assert.assertEquals(3, in("s", 7, 8, 9).literals().size());
    Assert.assertEquals(3, in("s", 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, in("s", "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, in("s").literals().size());
    Assert.assertEquals(1, in("s", 5).literals().size());
    Assert.assertEquals(2, in("s", 5, 5).literals().size());
    Assert.assertEquals(2, in("s", Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, in("s", Collections.emptyList()).literals().size());

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, in("s", "abc", "abd", "abc"));

    testIn(new Evaluator(STRUCT, in("x", 7, 8, Long.MAX_VALUE)),
        new Evaluator(STRUCT, in("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE)),
        new Evaluator(STRUCT, in("s1.s2.s3.s4.i", 7, 8, 9)),
        new Evaluator(STRUCT, in("y", 7, 8, 9.1)),
        charSeqEvaluator);
  }

  @Test
  public void testInWithUnboundTerm() {
    Assert.assertEquals(3, in(self("s"), 7, 8, 9).literals().size());
    Assert.assertEquals(3, in(self("s"), 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, in(self("s"), "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, in(self("s")).literals().size());
    Assert.assertEquals(1, in(self("s"), 5).literals().size());
    Assert.assertEquals(2, in(self("s"), 5, 5).literals().size());
    Assert.assertEquals(2, in(self("s"), Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, in(self("s"), Collections.emptyList()).literals().size());

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, in(self("s"), "abc", "abd", "abc"));

    testIn(new Evaluator(STRUCT, in(self("x"), 7, 8, Long.MAX_VALUE)),
        new Evaluator(STRUCT, in(self("x"), Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE)),
        new Evaluator(STRUCT, in(self("s1.s2.s3.s4.i"), 7, 8, 9)),
        new Evaluator(STRUCT, in(self("y"), 7, 8, 9.1)),
        charSeqEvaluator);
  }

  private void testIn(Evaluator evaluator, Evaluator intSetEvaluator,
                      Evaluator structEvaluator, Evaluator integerEvaluator, Evaluator charSeqEvaluator) {
    Assert.assertTrue("7 in [7, 8] => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("9 in [7, 8]  => false", evaluator.eval(TestHelpers.Row.of(9, 8, null)));

    Assert.assertTrue("Integer.MAX_VALUE in [Integer.MAX_VALUE] => true",
        intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)));
    Assert.assertFalse("6 in [Integer.MAX_VALUE]  => false",
        intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Assert.assertTrue("7 in [7, 8, 9] => true",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertFalse("6 in [7, 8, 9]  => false",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));

    Assert.assertTrue("7.0 in [7, 8, 9.1] => true",
        integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)));
    Assert.assertTrue("9.1 in [7, 8, 9.1] => true",
        integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)));
    Assert.assertFalse("6.8 in [7, 8, 9.1]  => false",
        integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Assert.assertTrue("utf8(abc) in [string(abc), string(abd)] => true",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertFalse("utf8(abcd) in [string(abc), string(abd)] => false",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }

  @Test
  public void testInExceptions() {
    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> in("x", (Literal) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> in(self("x"), (Literal) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Values cannot be null for IN predicate",
        () -> in("x", (Collection<?>) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Values cannot be null for IN predicate",
        () -> in(self("x"), (Collection<?>) null));

    AssertHelpers.assertThrows(
        "Throw exception if calling literal() for IN predicate",
        IllegalArgumentException.class,
        "IN predicate cannot return a literal",
        () -> in("x", 5, 6).literal());

    AssertHelpers.assertThrows(
        "Throw exception if calling literal() for IN predicate",
        IllegalArgumentException.class,
        "IN predicate cannot return a literal",
        () -> in(self("x"), 5, 6).literal());

    AssertHelpers.assertThrows(
        "Throw exception if any value in the input is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> in("x", 1, 2, null));

    AssertHelpers.assertThrows(
        "Throw exception if any value in the input is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> in(self("x"), 1, 2, null));

    AssertHelpers.assertThrows(
        "Throw exception if binding fails for any element in the set",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, in("x", 7, 8, 9.1)));

    AssertHelpers.assertThrows(
        "Throw exception if binding fails for any element in the set",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, in(self("x"), 7, 8, 9.1)));

    AssertHelpers.assertThrows(
        "Throw exception if no input value",
        IllegalArgumentException.class,
        "Cannot create IN predicate without a value",
        () -> predicate(Expression.Operation.IN, "x"));

    AssertHelpers.assertThrows(
        "Implicit conversion IN to EQ and throw exception if binding fails",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, predicate(Expression.Operation.IN, "x", 5.1)));
  }

  @Test
  public void testNotIn() {
    Assert.assertEquals(3, notIn("s", 7, 8, 9).literals().size());
    Assert.assertEquals(3, notIn("s", 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, notIn("s", "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, notIn("s").literals().size());
    Assert.assertEquals(1, notIn("s", 5).literals().size());
    Assert.assertEquals(2, notIn("s", 5, 5).literals().size());
    Assert.assertEquals(2, notIn("s", Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, notIn("s", Collections.emptyList()).literals().size());

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, notIn("s", "abc", "abd", "abc"));

    testNotIn(
        new Evaluator(STRUCT, notIn("x", 7, 8, Long.MAX_VALUE)),
        new Evaluator(STRUCT, notIn("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE)),
        new Evaluator(STRUCT, notIn("s1.s2.s3.s4.i", 7, 8, 9)),
        charSeqEvaluator,
        new Evaluator(STRUCT, notIn("y", 7, 8, 9.1)));
  }

  @Test
  public void testNotInWithUnboundTerm() {
    Assert.assertEquals(3, notIn(self("s"), 7, 8, 9).literals().size());
    Assert.assertEquals(3, notIn(self("s"), 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, notIn(self("s"), "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, notIn(self("s")).literals().size());
    Assert.assertEquals(1, notIn(self("s"), 5).literals().size());
    Assert.assertEquals(2, notIn(self("s"), 5, 5).literals().size());
    Assert.assertEquals(2, notIn(self("s"), Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, notIn(self("s"), Collections.emptyList()).literals().size());

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, notIn(self("s"), "abc", "abd", "abc"));

    testNotIn(
        new Evaluator(STRUCT, notIn(self("x"), 7, 8, Long.MAX_VALUE)),
        new Evaluator(STRUCT, notIn(self("x"), Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE)),
        new Evaluator(STRUCT, notIn(self("s1.s2.s3.s4.i"), 7, 8, 9)),
        charSeqEvaluator,
        new Evaluator(STRUCT, notIn(self("y"), 7, 8, 9.1)));
  }

  private void testNotIn(Evaluator evaluator, Evaluator intSetEvaluator,
                         Evaluator structEvaluator, Evaluator charSeqEvaluator, Evaluator integerEvaluator) {
    Assert.assertFalse("7 not in [7, 8] => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 not in [7, 8]  => true", evaluator.eval(TestHelpers.Row.of(9, 8, null)));

    Assert.assertFalse("Integer.MAX_VALUE not_in [Integer.MAX_VALUE] => false",
        intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)));
    Assert.assertTrue("6 not_in [Integer.MAX_VALUE]  => true",
        intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Assert.assertFalse("7 not in [7, 8, 9] => false",
        structEvaluator.eval(TestHelpers.Row.of(7, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(7)))))));
    Assert.assertTrue("6 not in [7, 8, 9]  => true",
        structEvaluator.eval(TestHelpers.Row.of(6, 8, null,
            TestHelpers.Row.of(
                TestHelpers.Row.of(
                    TestHelpers.Row.of(
                        TestHelpers.Row.of(6)))))));

    Assert.assertFalse("utf8(abc) not in [string(abc), string(abd)] => false",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertTrue("utf8(abcd) not in [string(abc), string(abd)] => true",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));

    Assert.assertFalse("7.0 not in [7, 8, 9] => false",
        integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)));
    Assert.assertFalse("9.1 not in [7, 8, 9.1] => false",
        integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)));
    Assert.assertTrue("6.8 not in [7, 8, 9.1]  => true",
        integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

  }

  @Test
  public void testNotInExceptions() {
    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notIn("x", (Literal) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notIn(self("x"), (Literal) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Values cannot be null for NOT_IN predicate",
        () -> notIn("x", (Collection<?>) null));

    AssertHelpers.assertThrows(
        "Throw exception if value is null",
        NullPointerException.class,
        "Values cannot be null for NOT_IN predicate",
        () -> notIn(self("x"), (Collection<?>) null));

    AssertHelpers.assertThrows(
        "Throw exception if calling literal() for IN predicate",
        IllegalArgumentException.class,
        "NOT_IN predicate cannot return a literal",
        () -> notIn("x", 5, 6).literal());

    AssertHelpers.assertThrows(
        "Throw exception if calling literal() for IN predicate",
        IllegalArgumentException.class,
        "NOT_IN predicate cannot return a literal",
        () -> notIn(self("x"), 5, 6).literal());

    AssertHelpers.assertThrows(
        "Throw exception if any value in the input is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notIn("x", 1, 2, null));

    AssertHelpers.assertThrows(
        "Throw exception if any value in the input is null",
        NullPointerException.class,
        "Cannot create expression literal from null",
        () -> notIn(self("x"), 1, 2, null));

    AssertHelpers.assertThrows(
        "Throw exception if binding fails for any element in the set",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, notIn("x", 7, 8, 9.1)));

    AssertHelpers.assertThrows(
        "Throw exception if binding fails for any element in the set",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, notIn(self("x"), 7, 8, 9.1)));

    AssertHelpers.assertThrows(
        "Throw exception if no input value",
        IllegalArgumentException.class,
        "Cannot create NOT_IN predicate without a value",
        () -> predicate(Expression.Operation.NOT_IN, "x"));

    AssertHelpers.assertThrows(
        "Implicit conversion NOT_IN to NOT_EQ and throw exception if binding fails",
        ValidationException.class,
        "Invalid value for conversion to type int",
        () -> new Evaluator(STRUCT, predicate(Expression.Operation.NOT_IN, "x", 5.1)));
  }

  private <T> UnboundTerm<T> self(String name) {
    Type type = Types.IntegerType.get();
    if (name.equals("y")) {
      type = Types.DoubleType.get();
    }
    return new UnboundTransform<>(ref(name), Transforms.identity(type));
  }
}
