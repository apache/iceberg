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
package org.apache.iceberg.spark.broadcastvar.expressions;

import static org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils.createPredicate;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.BaseTestEvaluator;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class TestEvaluatorWithRangeIn extends BaseTestEvaluator {
  @Test
  public void testRangeIn() {
    Assert.assertEquals(3, createPredicate("s", new Object[] {7, 8, 9}).literals().size());
    Assert.assertEquals(
        3, createPredicate("s", new Object[] {7, 8.1, Integer.MAX_VALUE}).literals().size());
    Assert.assertEquals(
        3,
        RangeInTestUtils.<String>createPredicate(
                "s", new Object[] {"abc", "abd", "abc"}, DataTypes.StringType)
            .literals()
            .size());
    Assert.assertEquals(0, createPredicate("s", new Object[] {}).literals().size());
    Assert.assertEquals(1, createPredicate("s", new Object[] {5}).literals().size());
    Assert.assertEquals(2, createPredicate("s", new Object[] {5, 5}).literals().size());
    Assert.assertEquals(2, createPredicate("s", Arrays.asList(5, 5).toArray()).literals().size());
    Assert.assertEquals(
        0, createPredicate("s", Collections.emptyList().toArray()).literals().size());

    Evaluator evaluator =
        new Evaluator(STRUCT, createPredicate("x", new Object[] {7, 8, Integer.MAX_VALUE}));
    Assert.assertTrue("7 in [7, 8] => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("9 in [7, 8]  => false", evaluator.eval(TestHelpers.Row.of(9, 8, null)));

    Evaluator intSetEvaluator =
        new Evaluator(
            STRUCT,
            createPredicate(
                "x", new Object[] {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE}));
    Assert.assertTrue(
        "Integer.MAX_VALUE in [Integer.MAX_VALUE] => true",
        intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7, null)));
    Assert.assertFalse(
        "6 in [Integer.MAX_VALUE]  => false", intSetEvaluator.eval(TestHelpers.Row.of(6, 5, null)));

    Evaluator integerEvaluator =
        new Evaluator(
            STRUCT,
            RangeInTestUtils.<Double>createPredicate(
                "y", new Object[] {7d, 8d, 9.1d}, DataTypes.DoubleType));
    Assert.assertTrue(
        "7.0 in [7, 8, 9.1] => true", integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)));
    Assert.assertTrue(
        "9.1 in [7, 8, 9.1] => true", integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)));
    Assert.assertFalse(
        "6.8 in [7, 8, 9.1]  => false", integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Evaluator structEvaluator =
        new Evaluator(STRUCT, createPredicate("s1.s2.s3.s4.i", new Object[] {7, 8, 9}));
    Assert.assertTrue(
        "7 in [7, 8, 9] => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "6 in [7, 8, 9]  => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));

    Types.StructType charSeqStruct = Types.StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator =
        new Evaluator(
            charSeqStruct,
            RangeInTestUtils.<String>createPredicate(
                "s", new Object[] {"abc", "abd", "abc"}, DataTypes.StringType));
    Assert.assertTrue(
        "utf8(abc) in [string(abc), string(abd)] => true",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertFalse(
        "utf8(abcd) in [string(abc), string(abd)] => false",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }
}
