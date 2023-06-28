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
package org.apache.iceberg.spark.data;

import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.MetricsRowGroupFilterBase;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestMetricsRowGroupFilterWithRangeIn extends MetricsRowGroupFilterBase {

  public TestMetricsRowGroupFilterWithRangeIn(String format) {
    super(format);
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MIN_VALUE - 25, INT_MIN_VALUE - 24}));
    Assert.assertFalse("Should not read: id below lower bound (5 < 30, 6 < 30)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MIN_VALUE - 2, INT_MIN_VALUE - 1}));
    Assert.assertFalse("Should not read: id below lower bound (28 < 30, 29 < 30)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MIN_VALUE - 1, INT_MIN_VALUE}));
    Assert.assertTrue("Should read: id equal to lower bound (30 == 30)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MAX_VALUE - 4, INT_MAX_VALUE - 3}));
    Assert.assertTrue(
        "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MAX_VALUE, INT_MAX_VALUE + 1}));
    Assert.assertTrue("Should read: id equal to upper bound (79 == 79)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MAX_VALUE + 1, INT_MAX_VALUE + 2}));
    Assert.assertFalse("Should not read: id above upper bound (80 > 79, 81 > 79)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "id", new Object[] {INT_MAX_VALUE + 6, INT_MAX_VALUE + 7}));
    Assert.assertFalse("Should not read: id above upper bound (85 > 79, 86 > 79)", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "all_nulls", new Object[] {1.0d, 2.0d}, DataTypes.DoubleType));
    Assert.assertFalse("Should skip: in on all nulls column", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "some_nulls", new Object[] {"aaa", "some"}, DataTypes.StringType));
    Assert.assertTrue("Should read: in on some nulls column", shouldRead);

    shouldRead =
        shouldRead(
            RangeInTestUtils.createPredicate(
                "no_nulls", new Object[] {"aaa", ""}, DataTypes.StringType));
    Assert.assertTrue("Should read: in on no nulls column", shouldRead);
  }

  @Test
  public void testInLimitParquet() {
    Assume.assumeTrue(format == FileFormat.PARQUET);

    boolean shouldRead = shouldRead(RangeInTestUtils.createPredicate("id", new Object[] {1, 2}));
    Assert.assertFalse("Should not read if IN is evaluated", shouldRead);

    List<Integer> ids = Lists.newArrayListWithExpectedSize(400);
    for (int id = -400; id <= 0; id++) {
      ids.add(id);
    }

    shouldRead = shouldRead(RangeInTestUtils.createPredicate("id", ids.toArray()));
    Assert.assertFalse("Should not read", shouldRead);
  }
}
