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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestTruncateUtil {
  @ParameterizedTest
  @ValueSource(ints = {1_500_000_000, 2_000_000_000, Integer.MAX_VALUE})
  void largeIntegerWidth(int width) {
    assertThat(TruncateUtil.truncateInt(width, width - 1)).isZero();
    assertThat(TruncateUtil.truncateInt(width, width)).isEqualTo(width);
    assertThat(TruncateUtil.truncateInt(width, -1)).isEqualTo(-width);
  }

  @Test
  void largeWidthForNarrowIntegers() {
    assertThat(TruncateUtil.truncateByte(Integer.MAX_VALUE, Byte.MAX_VALUE)).isZero();
    assertThat(TruncateUtil.truncateShort(Integer.MAX_VALUE, Short.MAX_VALUE)).isZero();
  }

  @Test
  public void testInvalidInputWidthBehavior() {
    assertThatNoException()
        .as("Invalid width input shouldn't necessarily throw an exception as it's not validated")
        .isThrownBy(() -> TruncateUtil.truncateInt(-1, 100));

    assertThatException()
        .as("Invalid width input can possibly throw an exception")
        .isThrownBy(() -> TruncateUtil.truncateInt(0, 100));
  }
}
