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

import org.apache.iceberg.expressions.BaseStrictMetricsEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.junit.Assert;
import org.junit.Test;

public class TestStrictMetricsEvaluatorWithRangeIn extends BaseStrictMetricsEvaluator {
  @Test
  public void testIntegerRangeIn() {
    boolean shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA,
                createPredicate("id", new Object[] {INT_MIN_VALUE - 25, INT_MIN_VALUE - 24}))
            .eval(FILE);
    Assert.assertFalse("Should not match: all values != 5 and != 6", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("id", new Object[] {INT_MIN_VALUE - 1, INT_MIN_VALUE}))
            .eval(FILE);
    Assert.assertFalse("Should not match: some values != 30 and != 31", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("id", new Object[] {INT_MAX_VALUE - 4, INT_MAX_VALUE - 3}))
            .eval(FILE);
    Assert.assertFalse("Should not match: some values != 75 and != 76", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("id", new Object[] {INT_MAX_VALUE, INT_MAX_VALUE + 1}))
            .eval(FILE);
    Assert.assertFalse("Should not match: some values != 78 and != 79", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("id", new Object[] {INT_MAX_VALUE + 1, INT_MAX_VALUE + 2}))
            .eval(FILE);
    Assert.assertFalse("Should not match: some values != 80 and != 81)", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, createPredicate("always_5", new Object[] {5, 6}))
            .eval(FILE);
    Assert.assertTrue("Should match: all values == 5", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("all_nulls", new Object[] {"abc", "def"}))
            .eval(FILE);
    Assert.assertFalse("Should not match: in on all nulls column", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(
                SCHEMA, createPredicate("some_nulls", new Object[] {"abc", "def"}))
            .eval(FILE_3);
    Assert.assertFalse("Should not match: in on some nulls column", shouldRead);

    shouldRead =
        new StrictMetricsEvaluator(SCHEMA, createPredicate("no_nulls", new Object[] {"abc", "def"}))
            .eval(FILE);
    Assert.assertFalse("Should not match: no_nulls field does not have bounds", shouldRead);
  }
}
