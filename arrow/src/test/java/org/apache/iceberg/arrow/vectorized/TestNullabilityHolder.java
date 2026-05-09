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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestNullabilityHolder {

  @Test
  void resetClearsIsNullMarkers() {
    // Regression test for #15808: reset() must clear the isNull marker
    // array, not only numNulls. Otherwise, callers reusing the holder
    // across batches observe stale null markers via isNullAt().
    NullabilityHolder holder = new NullabilityHolder(10);
    holder.setNull(3);

    assertThat(holder.hasNulls()).isTrue();
    assertThat(holder.isNullAt(3)).isEqualTo((byte) 1);

    holder.reset();

    assertThat(holder.hasNulls()).isFalse();
    assertThat(holder.numNulls()).isZero();
    // Before the fix, this assertion failed: the isNull[3] byte still
    // held the stale 1 from the previous batch.
    assertThat(holder.isNullAt(3))
        .as("isNull marker at index 3 must be cleared by reset()")
        .isEqualTo((byte) 0);

    // Spot-check a few other indices to confirm the whole array was cleared.
    for (int i = 0; i < holder.size(); i++) {
      assertThat(holder.isNullAt(i))
          .as("index %d should be 0 after reset()", i)
          .isEqualTo((byte) 0);
    }
  }

  @Test
  void resetClearsBulkSetNulls() {
    // Same property, but with the bulk setter setNulls(start, num).
    NullabilityHolder holder = new NullabilityHolder(8);
    holder.setNulls(2, 4);

    assertThat(holder.hasNulls()).isTrue();
    for (int i = 2; i < 6; i++) {
      assertThat(holder.isNullAt(i)).isEqualTo((byte) 1);
    }

    holder.reset();

    for (int i = 0; i < holder.size(); i++) {
      assertThat(holder.isNullAt(i)).isEqualTo((byte) 0);
    }
  }

  @Test
  void resetIsIdempotent() {
    NullabilityHolder holder = new NullabilityHolder(4);
    holder.setNull(0);
    holder.reset();
    holder.reset();

    assertThat(holder.hasNulls()).isFalse();
    assertThat(holder.numNulls()).isZero();
    for (int i = 0; i < holder.size(); i++) {
      assertThat(holder.isNullAt(i)).isEqualTo((byte) 0);
    }
  }
}
