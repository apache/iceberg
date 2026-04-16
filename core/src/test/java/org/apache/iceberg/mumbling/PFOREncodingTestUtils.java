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
package org.apache.iceberg.mumbling;

import java.util.Random;

/** Shared data-generation utilities for {@link PFOREncoding} tests and benchmarks. */
class PFOREncodingTestUtils {

  private PFOREncodingTestUtils() {}

  /**
   * Generates {@code count} values drawn uniformly from {@code [0, maxValue]} using the given seed.
   */
  static int[] uniform(int count, long seed, int maxValue) {
    Random random = new Random(seed);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = random.nextInt(maxValue + 1);
    }
    return values;
  }

  /**
   * Generates {@code count} values where each value is drawn from {@code [0, 3]} except that each
   * position has a {@code exceptionPct}% chance of being replaced with a full-range value
   * {@code [0, 255]}.
   */
  static int[] sparse(int count, long seed, int exceptionPct) {
    Random random = new Random(seed);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = random.nextInt(100) < exceptionPct ? random.nextInt(256) : random.nextInt(4);
    }
    return values;
  }

  /**
   * Generates {@code count} values drawn uniformly from {@code [minValue, minValue + range]} using
   * the given seed.
   */
  static int[] withOffset(int count, long seed, int minValue, int range) {
    Random random = new Random(seed);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = minValue + random.nextInt(range + 1);
    }
    return values;
  }
}
