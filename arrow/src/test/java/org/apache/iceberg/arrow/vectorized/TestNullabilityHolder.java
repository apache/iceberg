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
  void resetClearsNullArray() {
    NullabilityHolder holder = new NullabilityHolder(10);
    holder.setNull(3);
    holder.setNull(7);
    assertThat(holder.hasNulls()).isTrue();
    assertThat(holder.numNulls()).isEqualTo(2);
    assertThat(holder.isNullAt(3)).isEqualTo((byte) 1);
    assertThat(holder.isNullAt(7)).isEqualTo((byte) 1);

    holder.reset();

    assertThat(holder.hasNulls()).isFalse();
    assertThat(holder.numNulls()).isZero();
    for (int i = 0; i < holder.size(); i++) {
      assertThat(holder.isNullAt(i)).as("index %d should be non-null after reset", i).isZero();
    }
  }

  @Test
  void resetFollowedByNewNulls() {
    NullabilityHolder holder = new NullabilityHolder(10);
    holder.setNulls(0, 5);
    assertThat(holder.numNulls()).isEqualTo(5);

    holder.reset();
    holder.setNull(1);

    assertThat(holder.numNulls()).isEqualTo(1);
    assertThat(holder.isNullAt(0)).isZero();
    assertThat(holder.isNullAt(1)).isEqualTo((byte) 1);
    assertThat(holder.isNullAt(2)).isZero();
  }
}
