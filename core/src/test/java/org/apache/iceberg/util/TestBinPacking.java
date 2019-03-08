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

package org.apache.iceberg.util;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.junit.Assert;
import org.junit.Test;

public class TestBinPacking {
  @Test
  public void testBasicBinPacking() {
    Assert.assertEquals("Should pack the first 2 values",
        l(l(1, 2), l(3), l(4), l(5)), pack(l(1, 2, 3, 4, 5), 3));

    Assert.assertEquals("Should pack the first 2 values",
        l(l(1, 2), l(3), l(4), l(5)), pack(l(1, 2, 3, 4, 5), 5));

    Assert.assertEquals("Should pack the first 3 values",
        l(l(1, 2, 3), l(4), l(5)), pack(l(1, 2, 3, 4, 5), 6));

    Assert.assertEquals("Should pack the first 3 values",
        l(l(1, 2, 3), l(4), l(5)), pack(l(1, 2, 3, 4, 5), 8));

    Assert.assertEquals("Should pack the first 3 values, last 2 values",
        l(l(1, 2, 3), l(4, 5)), pack(l(1, 2, 3, 4, 5), 9));

    Assert.assertEquals("Should pack the first 4 values",
        l(l(1, 2, 3, 4), l(5)), pack(l(1, 2, 3, 4, 5), 10));

    Assert.assertEquals("Should pack the first 4 values",
        l(l(1, 2, 3, 4), l(5)), pack(l(1, 2, 3, 4, 5), 14));

    Assert.assertEquals("Should pack the first 5 values",
        l(l(1, 2, 3, 4, 5)), pack(l(1, 2, 3, 4, 5), 15));
  }

  @Test
  public void testReverseBinPackingSingleLookback() {
    Assert.assertEquals("Should pack the first 2 values",
        l(l(1, 2), l(3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 3, 1));

    Assert.assertEquals("Should pack the first 2 values",
        l(l(1, 2), l(3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 4, 1));

    Assert.assertEquals("Should pack the second and third values",
        l(l(1), l(2, 3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 5, 1));

    Assert.assertEquals("Should pack the first 3 values",
        l(l(1, 2, 3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 6, 1));

    Assert.assertEquals("Should pack the first two pairs of values",
        l(l(1, 2), l(3, 4), l(5)), packEnd(l(1, 2, 3, 4, 5), 7, 1));

    Assert.assertEquals("Should pack the first two pairs of values",
        l(l(1, 2), l(3, 4), l(5)), packEnd(l(1, 2, 3, 4, 5), 8, 1));

    Assert.assertEquals("Should pack the first 3 values, last 2 values",
        l(l(1, 2, 3), l(4, 5)), packEnd(l(1, 2, 3, 4, 5), 9, 1));

    Assert.assertEquals("Should pack the first 3 values, last 2 values",
        l(l(1, 2, 3), l(4, 5)), packEnd(l(1, 2, 3, 4, 5), 11, 1));

    Assert.assertEquals("Should pack the first 3 values, last 2 values",
        l(l(1, 2), l(3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 12, 1));

    Assert.assertEquals("Should pack the last 4 values",
        l(l(1), l(2, 3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 14, 1));

    Assert.assertEquals("Should pack the first 5 values",
        l(l(1, 2, 3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 15, 1));
  }

  @Test
  public void testReverseBinPackingUnlimitedLookback() {
    Assert.assertEquals("Should pack the first 2 values",
        l(l(1, 2), l(3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 3));

    Assert.assertEquals("Should pack 1 with 3",
        l(l(2), l(1, 3), l(4), l(5)), packEnd(l(1, 2, 3, 4, 5), 4));

    Assert.assertEquals("Should pack 2,3 and 1,4",
        l(l(2, 3), l(1, 4), l(5)), packEnd(l(1, 2, 3, 4, 5), 5));

    Assert.assertEquals("Should pack 2,4 and 1,5",
        l(l(3), l(2, 4), l(1, 5)), packEnd(l(1, 2, 3, 4, 5), 6));

    Assert.assertEquals("Should pack 3,4 and 2,5",
        l(l(1), l(3, 4), l(2, 5)), packEnd(l(1, 2, 3, 4, 5), 7));

    Assert.assertEquals("Should pack 1,2,3 and 3,5",
        l(l(1, 2, 4), l(3, 5)), packEnd(l(1, 2, 3, 4, 5), 8));

    Assert.assertEquals("Should pack the first 3 values, last 2 values",
        l(l(1, 2, 3), l(4, 5)), packEnd(l(1, 2, 3, 4, 5), 9));

    Assert.assertEquals("Should pack 2,3 and 1,4,5",
        l(l(2, 3), l(1, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 10));

    Assert.assertEquals("Should pack 1,3 and 2,4,5",
        l(l(1, 3), l(2, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 11));

    Assert.assertEquals("Should pack 1,2 and 3,4,5",
        l(l(1, 2), l(3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 12));

    Assert.assertEquals("Should pack 1,2 and 3,4,5",
        l(l(2), l(1, 3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 13));

    Assert.assertEquals("Should pack the last 4 values",
        l(l(1), l(2, 3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 14));

    Assert.assertEquals("Should pack the first 5 values",
        l(l(1, 2, 3, 4, 5)), packEnd(l(1, 2, 3, 4, 5), 15));
  }

  @Test
  public void testBinPackingLookBack() {
    // lookback state:
    // 1. [5]
    // 2. [5, 1]
    // 3. [5, 1], [5]
    // 4. [5, 1, 1], [5]
    // 5. [5, 1, 1], [5], [5]
    // 6. [5, 1, 1, 1], [5], [5]
    Assert.assertEquals("Unlimited look-back: should merge ones into first bin",
        l(l(5, 1, 1, 1), l(5), l(5)), pack(l(5, 1, 5, 1, 5, 1), 8));

    // lookback state:
    // 1. [5]
    // 2. [5, 1]
    // 3. [5, 1], [5]
    // 4. [5, 1, 1], [5]
    // 5. [5], [5]          ([5, 1, 1] drops out of look-back)
    // 6. [5, 1], [5]
    Assert.assertEquals("2 bin look-back: should merge two ones into first bin",
        l(l(5, 1, 1), l(5, 1), l(5)), pack(l(5, 1, 5, 1, 5, 1), 8, 2));

    // lookback state:
    // 1. [5]
    // 2. [5, 1]
    // 3. [5]               ([5, 1] drops out of look-back)
    // 4. [5, 1]
    // 5. [5]               ([5, 1] #2 drops out of look-back)
    // 6. [5, 1]
    Assert.assertEquals("1 bin look-back: should merge ones with fives",
        l(l(5, 1), l(5, 1), l(5, 1)), pack(l(5, 1, 5, 1, 5, 1), 8, 1));

    Assert.assertEquals("2 bin look-back: should merge all ones with five when expensiveTaskFirst is enabled",
        l(l(5, 1, 1, 1), l(5, 5)), pack(l(5, 1, 5, 1, 5, 1), 10, 2, true));

    Assert.assertEquals("2 bin look-back: should merge until targetWeight by weight with expensiveTaskFirst is enabled",
        l(l(36, 36, 36), l(128), l(36, 65), l(65)),
        pack(l(36, 36, 36, 36, 65, 65, 128), 128, 2, true));

    Assert.assertEquals(
        "1 bin look-back: should order by weight descending and not order by number of items (at same weight)",
        l(l(128), l(64, 64), l(32, 32, 32, 32)),
        pack(l(128, 64, 64, 32, 32, 32, 32), 128, 1, true));
  }

  private List<List<Integer>> pack(List<Integer> items, long targetWeight) {
    return pack(items, targetWeight, Integer.MAX_VALUE);
  }

  private List<List<Integer>> pack(List<Integer> items, long targetWeight, int lookback) {
    return pack(items, targetWeight, lookback, false);
  }

  private List<List<Integer>> pack(List<Integer> items, long targetWeight, int lookback, boolean expensiveTaskFirst) {
    ListPacker<Integer> packer = new ListPacker<>(targetWeight, lookback, expensiveTaskFirst);
    return packer.pack(items, Integer::longValue);
  }

  private List<List<Integer>> packEnd(List<Integer> items, long targetWeight) {
    return packEnd(items, targetWeight, Integer.MAX_VALUE);
  }

  private List<List<Integer>> packEnd(List<Integer> items, long targetWeight, int lookback) {
    ListPacker<Integer> packer = new ListPacker<>(targetWeight, lookback, false);
    return packer.packEnd(items, Integer::longValue);
  }

  private <T> List<T> l(T... items) {
    return Lists.newArrayList(items);
  }
}
