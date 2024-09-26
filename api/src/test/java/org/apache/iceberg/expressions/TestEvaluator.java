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

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
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
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.expressions.Expressions.termPredicate;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestEvaluator {
  private static final StructType STRUCT =
          StructType.of(
                  required(13, "x", Types.IntegerType.get()),
                  required(14, "y", Types.DoubleType.get()),
                  optional(15, "z", Types.IntegerType.get()),
                  optional(
                          16,
                          "s1",
                          Types.StructType.of(
                                  Types.NestedField.required(
                                          17,
                                          "s2",
                                          Types.StructType.of(
                                                  Types.NestedField.required(
                                                          18,
                                                          "s3",
                                                          Types.StructType.of(
                                                                  Types.NestedField.required(
                                                                          19,
                                                                          "s4",
                                                                          Types.StructType.of(
                                                                                  Types.NestedField.required(
                                                                                          20, "i", Types.IntegerType.get()))))))))),
                  optional(
                          21,
                          "s5",
                          Types.StructType.of(
                                  Types.NestedField.required(
                                          22,
                                          "s6",
                                          Types.StructType.of(
                                                  Types.NestedField.required(23, "f", Types.FloatType.get()))))));

  @Test
  public void testLessThan() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThan("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null, null))).as("7 < 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null))).as("6 < 7 => true").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, lessThan("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 < 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 < 7 => true")
            .isTrue();
  }

  @Test
  public void testRefCompareLessThan() {
    Evaluator evaluator = new Evaluator(STRUCT, termPredicate(Expression.Operation.LT, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 < 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 < 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 < 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 < null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null < null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.LT, Expressions.ref("s1.s2.s3.s4.i"), Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 < 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 < 7 => true")
            .isTrue();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.LT, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testLessThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThanOrEqual("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 <= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null))).as("6 <= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, null))).as("8 <= 7 => false").isFalse();

    Evaluator structEvaluator = new Evaluator(STRUCT, lessThanOrEqual("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 <= 7 => true")
            .isTrue();

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 <= 7 => true")
            .isTrue();

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 <= 7 => false")
            .isFalse();
  }

  @Test
  public void testRefCompareLessThanOrEqual() {
    Evaluator evaluator =
            new Evaluator(STRUCT, termPredicate(Expression.Operation.LT_EQ, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 <= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 <= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 <= 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 <= null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null <= null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.LT_EQ,
                            Expressions.ref("s1.s2.s3.s4.i"),
                            Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 <= 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 <= 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 <= 7 => false")
            .isFalse();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.LT_EQ, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testGreaterThan() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThan("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 > 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null))).as("6 > 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, null))).as("8 > 7 => true").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, greaterThan("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 > 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 > 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 > 7 => true")
            .isTrue();
  }

  @Test
  public void testRefCompareGreaterThan() {
    Evaluator evaluator = new Evaluator(STRUCT, termPredicate(Expression.Operation.GT, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 > 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 > 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 > 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 > null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null > null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.GT, Expressions.ref("s1.s2.s3.s4.i"), Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 > 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 > 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 > 7 => true")
            .isTrue();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.GT, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testGreaterThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThanOrEqual("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 >= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null))).as("6 >= 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, null))).as("8 >= 7 => true").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, greaterThanOrEqual("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 >= 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 >= 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 >= 7 => true")
            .isTrue();
  }

  @Test
  public void testRefCompareGreaterThanOrEqual() {
    Evaluator evaluator =
            new Evaluator(STRUCT, termPredicate(Expression.Operation.GT_EQ, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 >= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 >= 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 >= 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 >= null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null >= null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.GT_EQ,
                            Expressions.ref("s1.s2.s3.s4.i"),
                            Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 >= 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 >= 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 >= 7 => true")
            .isTrue();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.GT_EQ, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testEqual() {
    assertThat(equal("x", 5).literals().size()).isEqualTo(1);

    Evaluator evaluator = new Evaluator(STRUCT, equal("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 == 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null))).as("6 == 7 => false").isFalse();

    Evaluator structEvaluator = new Evaluator(STRUCT, equal("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 == 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 == 7 => false")
            .isFalse();
  }

  @Test
  public void testRefCompareEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, termPredicate(Expression.Operation.EQ, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 = 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 = 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 = 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 = null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null = null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.EQ, Expressions.ref("s1.s2.s3.s4.i"), Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 = 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 = 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 = 7 => false")
            .isFalse();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.EQ, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testNotEqual() {
    assertThat(notEqual("x", 5).literals().size()).isEqualTo(1);

    Evaluator evaluator = new Evaluator(STRUCT, notEqual("x", 7));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 != 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null))).as("6 != 7 => true").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, notEqual("s1.s2.s3.s4.i", 7));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 != 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 != 7 => true")
            .isTrue();
  }

  @Test
  public void testRefCompareNotEqual() {
    Evaluator evaluator =
            new Evaluator(STRUCT, termPredicate(Expression.Operation.NOT_EQ, "x", "z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, 7, null))).as("7 <> 7 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, 7, null))).as("6 <> 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 8, 7, null))).as("8 <> 7 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(6, 8, null, null)))
            .as("6 <> null => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(null, 8, 8, null)))
            .as("null <> null => false")
            .isFalse();

    Evaluator structEvaluator =
            new Evaluator(
                    STRUCT,
                    predicate(
                            Expression.Operation.NOT_EQ,
                            Expressions.ref("s1.s2.s3.s4.i"),
                            Expressions.ref("x")));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 <> 7 => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 <> 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8 <> 7 => true")
            .isTrue();

    assertThatThrownBy(
            () ->
                    new Evaluator(STRUCT, termPredicate(Expression.Operation.NOT_EQ, "x", "y"))
                            .eval(TestHelpers.Row.of(6, 8, 8, null)))
            .hasMessage("Cannot compare different types: int and double");
  }

  @Test
  public void testStartsWith() {
    StructType struct = StructType.of(required(24, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, startsWith("s", "abc"));
    assertThat(evaluator.eval(TestHelpers.Row.of("abc")))
            .as("abc startsWith abc should be true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of("xabc")))
            .as("xabc startsWith abc should be false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of("Abc")))
            .as("Abc startsWith abc should be false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
            .as("a startsWith abc should be false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of("abcd")))
            .as("abcd startsWith abc should be true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of((String) null)))
            .as("null startsWith abc should be false")
            .isFalse();
  }

  @Test
  public void testNotStartsWith() {
    StructType struct = StructType.of(required(24, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, notStartsWith("s", "abc"));
    assertThat(evaluator.eval(TestHelpers.Row.of("abc")))
            .as("abc notStartsWith abc should be false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of("xabc")))
            .as("xabc notStartsWith abc should be true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of("Abc")))
            .as("Abc notStartsWith abc should be true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of("a")))
            .as("a notStartsWith abc should be true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of("abcde")))
            .as("abcde notStartsWith abc should be false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of("Abcde")))
            .as("Abcde notStartsWith abc should be true")
            .isTrue();
  }

  @Test
  public void testAlwaysTrue() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysTrue());
    assertThat(evaluator.eval(TestHelpers.Row.of())).as("always true").isTrue();
  }

  @Test
  public void testAlwaysFalse() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysFalse());
    assertThat(evaluator.eval(TestHelpers.Row.of())).as("always false").isFalse();
  }

  @Test
  public void testIsNull() {
    Evaluator evaluator = new Evaluator(STRUCT, isNull("z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2, null))).as("null is null").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2, 3))).as("3 is not null").isFalse();

    Evaluator structEvaluator = new Evaluator(STRUCT, isNull("s1.s2.s3.s4.i"));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            1,
                            2,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(3)))))))
            .as("3 is not null")
            .isFalse();
  }

  @Test
  public void testNotNull() {
    Evaluator evaluator = new Evaluator(STRUCT, notNull("z"));
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2, null))).as("null is null").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2, 3))).as("3 is not null").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, notNull("s1.s2.s3.s4.i"));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            1,
                            2,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(3)))))))
            .as("3 is not null")
            .isTrue();
  }

  @Test
  public void testIsNan() {
    Evaluator evaluator = new Evaluator(STRUCT, isNaN("y"));
    assertThat(evaluator.eval(TestHelpers.Row.of(1, Double.NaN, 3))).as("NaN is NaN").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2.0, 3))).as("2 is not NaN").isFalse();

    Evaluator structEvaluator = new Evaluator(STRUCT, isNaN("s5.s6.f"));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(Float.NaN)))))
            .as("NaN is NaN")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(4F)))))
            .as("4F is not NaN")
            .isFalse();
  }

  @Test
  public void testNotNaN() {
    Evaluator evaluator = new Evaluator(STRUCT, notNaN("y"));
    assertThat(evaluator.eval(TestHelpers.Row.of(1, Double.NaN, 3))).as("NaN is NaN").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(1, 2.0, 3))).as("2 is not NaN").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, notNaN("s5.s6.f"));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(Float.NaN)))))
            .as("NaN is NaN")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(4F)))))
            .as("4F is not NaN")
            .isTrue();
  }

  @Test
  public void testAnd() {
    Evaluator evaluator = new Evaluator(STRUCT, and(equal("x", 7), notNull("z")));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 0, 3))).as("7, 3 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 0, 3))).as("8, 3 => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 0, null))).as("7, null => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 0, null))).as("8, null => false").isFalse();

    Evaluator structEvaluator =
            new Evaluator(STRUCT, and(equal("s1.s2.s3.s4.i", 7), notNull("s1.s2.s3.s4.i")));

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            0,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7, 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            8,
                            0,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8, 8 => false")
            .isFalse();

    assertThat(structEvaluator.eval(TestHelpers.Row.of(7, 0, null, null)))
            .as("7, null => false")
            .isFalse();

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            8,
                            0,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8, notnull => false")
            .isFalse();
  }

  @Test
  public void testOr() {
    Evaluator evaluator = new Evaluator(STRUCT, or(equal("x", 7), notNull("z")));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 0, 3))).as("7, 3 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 0, 3))).as("8, 3 => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 0, null))).as("7, null => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(8, 0, null))).as("8, null => false").isFalse();

    Evaluator structEvaluator =
            new Evaluator(STRUCT, or(equal("s1.s2.s3.s4.i", 7), notNull("s1.s2.s3.s4.i")));

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            0,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7, 7 => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            8,
                            0,
                            3,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("8, 8 => false")
            .isTrue();

    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            0,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7, notnull => false")
            .isTrue();
  }

  @Test
  public void testNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("x", 7)));
    assertThat(evaluator.eval(TestHelpers.Row.of(7))).as("not(7 == 7) => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8))).as("not(8 == 7) => false").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            null,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("not(7 == 7) => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            8,
                            null,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("not(8 == 7) => false")
            .isTrue();
  }

  @Test
  public void testCaseInsensitiveNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("X", 7)), false);
    assertThat(evaluator.eval(TestHelpers.Row.of(7))).as("not(7 == 7) => false").isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(8))).as("not(8 == 7) => false").isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)), false);
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            null,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("not(7 == 7) => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            8,
                            null,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))))
            .as("not(8 == 7) => false")
            .isTrue();
  }

  @Test
  public void testCaseSensitiveNot() {
    assertThatThrownBy(() -> new Evaluator(STRUCT, not(equal("X", 7)), true))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Cannot find field 'X' in struct");
  }

  @Test
  public void testCharSeqValue() {
    StructType struct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, equal("s", "abc"));
    assertThat(evaluator.eval(TestHelpers.Row.of(new Utf8("abc"))))
            .as("string(abc) == utf8(abc) => true")
            .isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))))
            .as("string(abc) == utf8(abcd) => false")
            .isFalse();
  }

  @Test
  public void testIn() {
    assertThat(in("s", 7, 8, 9).literals()).hasSize(3);
    assertThat(in("s", 7, 8.1, Long.MAX_VALUE).literals()).hasSize(3);
    assertThat(in("s", "abc", "abd", "abc").literals()).hasSize(3);
    assertThat(in("s").literals()).isEmpty();
    assertThat(in("s", 5).literals()).hasSize(1);
    assertThat(in("s", 5, 5).literals()).hasSize(2);
    assertThat(in("s", Arrays.asList(5, 5)).literals()).hasSize(2);
    assertThat(in("s", Collections.emptyList()).literals()).isEmpty();

    Evaluator evaluator = new Evaluator(STRUCT, in("x", 7, 8, Long.MAX_VALUE));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null))).as("7 in [7, 8] => true").isTrue();
    assertThat(evaluator.eval(TestHelpers.Row.of(9, 8, null)))
            .as("9 in [7, 8]  => false")
            .isFalse();

    Evaluator intSetEvaluator =
            new Evaluator(STRUCT, in("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE));
    assertThat(intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)))
            .as("Integer.MAX_VALUE in [Integer.MAX_VALUE] => true")
            .isTrue();
    assertThat(intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)))
            .as("6 in [Integer.MAX_VALUE]  => false")
            .isFalse();

    Evaluator integerEvaluator = new Evaluator(STRUCT, in("y", 7, 8, 9.1));
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)))
            .as("7.0 in [7, 8, 9.1] => true")
            .isTrue();
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)))
            .as("9.1 in [7, 8, 9.1] => true")
            .isTrue();
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)))
            .as("6.8 in [7, 8, 9.1]  => false")
            .isFalse();

    Evaluator structEvaluator = new Evaluator(STRUCT, in("s1.s2.s3.s4.i", 7, 8, 9));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 in [7, 8, 9] => true")
            .isTrue();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 in [7, 8, 9]  => false")
            .isFalse();

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, in("s", "abc", "abd", "abc"));
    assertThat(charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))))
            .as("utf8(abc) in [string(abc), string(abd)] => true")
            .isTrue();
    assertThat(charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))))
            .as("utf8(abcd) in [string(abc), string(abd)] => false")
            .isFalse();
  }

  @Test
  public void testInExceptions() {
    assertThatThrownBy(() -> in("x", (Literal) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Cannot create expression literal from null");

    assertThatThrownBy(() -> in("x", (Collection<?>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Values cannot be null for IN predicate.");

    assertThatThrownBy(() -> in("x", 5, 6).literal())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("IN predicate cannot return a literal");

    assertThatThrownBy(() -> in("x", 1, 2, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Cannot create expression literal from null");

    assertThatThrownBy(() -> new Evaluator(STRUCT, in("x", 7, 8, 9.1)))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Invalid value for conversion to type int");

    assertThatThrownBy(() -> predicate(Expression.Operation.IN, "x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create IN predicate without a value");

    assertThatThrownBy(() -> new Evaluator(STRUCT, predicate(Expression.Operation.IN, "x", 5.1)))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Invalid value for conversion to type int");
  }

  @Test
  public void testNotIn() {
    assertThat(notIn("s", 7, 8, 9).literals()).hasSize(3);
    assertThat(notIn("s", 7, 8.1, Long.MAX_VALUE).literals()).hasSize(3);
    assertThat(notIn("s", "abc", "abd", "abc").literals()).hasSize(3);
    assertThat(notIn("s").literals()).isEmpty();
    assertThat(notIn("s", 5).literals()).hasSize(1);
    assertThat(notIn("s", 5, 5).literals()).hasSize(2);
    assertThat(notIn("s", Arrays.asList(5, 5)).literals()).hasSize(2);
    assertThat(notIn("s", Collections.emptyList()).literals()).isEmpty();

    Evaluator evaluator = new Evaluator(STRUCT, notIn("x", 7, 8, Long.MAX_VALUE));
    assertThat(evaluator.eval(TestHelpers.Row.of(7, 8, null)))
            .as("7 not in [7, 8] => false")
            .isFalse();
    assertThat(evaluator.eval(TestHelpers.Row.of(9, 8, null)))
            .as("6 not in [7, 8]  => true")
            .isTrue();

    Evaluator intSetEvaluator =
            new Evaluator(STRUCT, notIn("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE));
    assertThat(intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)))
            .as("Integer.MAX_VALUE not_in [Integer.MAX_VALUE] => false")
            .isFalse();
    assertThat(intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)))
            .as("6 not_in [Integer.MAX_VALUE]  => true")
            .isTrue();

    Evaluator integerEvaluator = new Evaluator(STRUCT, notIn("y", 7, 8, 9.1));
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)))
            .as("7.0 not in [7, 8, 9] => false")
            .isFalse();
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)))
            .as("9.1 not in [7, 8, 9.1] => false")
            .isFalse();
    assertThat(integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)))
            .as("6.8 not in [7, 8, 9.1]  => true")
            .isTrue();

    Evaluator structEvaluator = new Evaluator(STRUCT, notIn("s1.s2.s3.s4.i", 7, 8, 9));
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            7,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))))
            .as("7 not in [7, 8, 9] => false")
            .isFalse();
    assertThat(
            structEvaluator.eval(
                    TestHelpers.Row.of(
                            6,
                            8,
                            null,
                            TestHelpers.Row.of(
                                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))))
            .as("6 not in [7, 8, 9]  => true")
            .isTrue();

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, notIn("s", "abc", "abd", "abc"));
    assertThat(charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))))
            .as("utf8(abc) not in [string(abc), string(abd)] => false")
            .isFalse();
    assertThat(charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))))
            .as("utf8(abcd) not in [string(abc), string(abd)] => true")
            .isTrue();
  }

  @Test
  public void testNotInExceptions() {
    assertThatThrownBy(() -> notIn("x", (Literal) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Cannot create expression literal from null");

    assertThatThrownBy(() -> notIn("x", (Collection<?>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Values cannot be null for NOT_IN predicate.");

    assertThatThrownBy(() -> notIn("x", 5, 6).literal())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("NOT_IN predicate cannot return a literal");

    assertThatThrownBy(() -> notIn("x", 1, 2, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("Cannot create expression literal from null");

    assertThatThrownBy(() -> new Evaluator(STRUCT, notIn("x", 7, 8, 9.1)))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Invalid value for conversion to type int");

    assertThatThrownBy(() -> predicate(Expression.Operation.NOT_IN, "x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create NOT_IN predicate without a value");

    assertThatThrownBy(
            () -> new Evaluator(STRUCT, predicate(Expression.Operation.NOT_IN, "x", 5.1)))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Invalid value for conversion to type int");
  }
}
