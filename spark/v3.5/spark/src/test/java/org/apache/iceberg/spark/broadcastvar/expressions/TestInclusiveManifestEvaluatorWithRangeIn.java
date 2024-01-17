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

import org.apache.iceberg.expressions.BaseInclusiveManifestEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.junit.Assert;
import org.junit.Test;

public class TestInclusiveManifestEvaluatorWithRangeIn extends BaseInclusiveManifestEvaluator {

  @Test
  public void testIntegerRangeIn() {
    boolean shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MIN_VALUE - 25, INT_MIN_VALUE - 24}),
                SPEC,
                true)
            .eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MIN_VALUE - 2, INT_MIN_VALUE - 1}),
                SPEC,
                true)
            .eval(FILE);
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MIN_VALUE - 1, INT_MIN_VALUE}), SPEC, true)
            .eval(FILE);
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MAX_VALUE - 4, INT_MAX_VALUE - 3}),
                SPEC,
                true)
            .eval(FILE);
    Assert.assertTrue(
        "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MAX_VALUE, INT_MAX_VALUE + 1}), SPEC, true)
            .eval(FILE);
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MAX_VALUE + 1, INT_MAX_VALUE + 2}),
                SPEC,
                true)
            .eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("id", new Object[] {INT_MAX_VALUE + 6, INT_MAX_VALUE + 7}),
                SPEC,
                true)
            .eval(FILE);
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("all_nulls_missing_nan", new Object[] {"abc", "def"}), SPEC, true)
            .eval(FILE);
    Assert.assertFalse("Should skip: in on all nulls column", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("some_nulls", new Object[] {"abc", "def"}), SPEC, true)
            .eval(FILE);
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead =
        ManifestEvaluator.forRowFilter(
                createPredicate("no_nulls", new Object[] {"abc", "def"}), SPEC, true)
            .eval(FILE);
    Assert.assertTrue("Should read: in on no nulls column", shouldRead);
  }
}
