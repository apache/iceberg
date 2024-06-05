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
package org.apache.iceberg.flink.sink.shuffle;

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.SortKey;
import org.junit.jupiter.api.Test;

public class TestSketchUtil {
  @Test
  public void testCoordinatorReservoirSize() {
    // adjusted to over min threshold of 10_000 and is divisible by number of partitions (3)
    assertThat(SketchUtil.determineCoordinatorReservoirSize(3)).isEqualTo(10_002);
    // adjust to multiplier of 100
    assertThat(SketchUtil.determineCoordinatorReservoirSize(123)).isEqualTo(123_00);
    // adjusted to below max threshold of 1_000_000 and is divisible by number of partitions (3)
    assertThat(SketchUtil.determineCoordinatorReservoirSize(10_123))
        .isEqualTo(1_000_000 - (1_000_000 % 10_123));
  }

  @Test
  public void testOperatorReservoirSize() {
    assertThat(SketchUtil.determineOperatorReservoirSize(5, 3))
        .isEqualTo((10_002 * SketchUtil.OPERATOR_OVER_SAMPLE_RATIO) / 5);
    assertThat(SketchUtil.determineOperatorReservoirSize(123, 123))
        .isEqualTo((123_00 * SketchUtil.OPERATOR_OVER_SAMPLE_RATIO) / 123);
    assertThat(SketchUtil.determineOperatorReservoirSize(256, 123))
        .isEqualTo(
            (int) Math.ceil((double) (123_00 * SketchUtil.OPERATOR_OVER_SAMPLE_RATIO) / 256));
    assertThat(SketchUtil.determineOperatorReservoirSize(5_120, 10_123))
        .isEqualTo(
            (int) Math.ceil((double) (992_054 * SketchUtil.OPERATOR_OVER_SAMPLE_RATIO) / 5_120));
  }

  @Test
  public void testRangeBoundsOneChannel() {
    assertThat(
            SketchUtil.determineBounds(
                1,
                Fixtures.SORT_ORDER_COMPARTOR,
                new SortKey[] {
                  CHAR_KEYS.get("a"),
                  CHAR_KEYS.get("b"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("d"),
                  CHAR_KEYS.get("e"),
                  CHAR_KEYS.get("f")
                }))
        .isEmpty();
  }

  @Test
  public void testRangeBoundsDivisible() {
    assertThat(
            SketchUtil.determineBounds(
                3,
                Fixtures.SORT_ORDER_COMPARTOR,
                new SortKey[] {
                  CHAR_KEYS.get("a"),
                  CHAR_KEYS.get("b"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("d"),
                  CHAR_KEYS.get("e"),
                  CHAR_KEYS.get("f")
                }))
        .containsExactly(CHAR_KEYS.get("b"), CHAR_KEYS.get("d"));
  }

  @Test
  public void testRangeBoundsNonDivisible() {
    // step is 3 = ceiling(11/4)
    assertThat(
            SketchUtil.determineBounds(
                4,
                Fixtures.SORT_ORDER_COMPARTOR,
                new SortKey[] {
                  CHAR_KEYS.get("a"),
                  CHAR_KEYS.get("b"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("d"),
                  CHAR_KEYS.get("e"),
                  CHAR_KEYS.get("f"),
                  CHAR_KEYS.get("g"),
                  CHAR_KEYS.get("h"),
                  CHAR_KEYS.get("i"),
                  CHAR_KEYS.get("j"),
                  CHAR_KEYS.get("k"),
                }))
        .containsExactly(CHAR_KEYS.get("c"), CHAR_KEYS.get("f"), CHAR_KEYS.get("i"));
  }

  @Test
  public void testRangeBoundsSkipDuplicates() {
    // step is 3 = ceiling(11/4)
    assertThat(
            SketchUtil.determineBounds(
                4,
                Fixtures.SORT_ORDER_COMPARTOR,
                new SortKey[] {
                  CHAR_KEYS.get("a"),
                  CHAR_KEYS.get("b"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("c"),
                  CHAR_KEYS.get("g"),
                  CHAR_KEYS.get("h"),
                  CHAR_KEYS.get("i"),
                  CHAR_KEYS.get("j"),
                  CHAR_KEYS.get("k"),
                }))
        // skipped duplicate c's
        .containsExactly(CHAR_KEYS.get("c"), CHAR_KEYS.get("g"), CHAR_KEYS.get("j"));
  }
}
