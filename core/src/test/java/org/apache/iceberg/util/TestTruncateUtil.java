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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestTruncateUtil {
  @Test
  public void testInvalidInputWidthBehavior() {
    assertThatNoException()
        .as("Invalid width input shouldn't necessarily throw an exception as it's not validated")
        .isThrownBy(() -> TruncateUtil.truncateInt(-1, 100));

    assertThatException()
        .as("Invalid width input can possibly throw an exception")
        .isThrownBy(() -> TruncateUtil.truncateInt(0, 100));
  }

  @Test
  public void testTruncateIntNearMinValue() {
    assertThatThrownBy(() -> TruncateUtil.truncateInt(10, Integer.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not fit in an int");
    assertThatThrownBy(() -> TruncateUtil.truncateInt(10, -2147483645))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(TruncateUtil.truncateInt(10, -2147483640)).isEqualTo(-2147483640);
    assertThat(TruncateUtil.truncateInt(10, -21)).isEqualTo(-30);
    assertThat(TruncateUtil.truncateInt(10, 5)).isEqualTo(0);
    assertThat(TruncateUtil.truncateInt(10, Integer.MAX_VALUE)).isEqualTo(2147483640);
  }

  @Test
  public void testTruncateLongNearMinValue() {
    assertThatThrownBy(() -> TruncateUtil.truncateLong(10, Long.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not fit in a long");
    assertThatThrownBy(() -> TruncateUtil.truncateLong(10, -9223372036854775805L))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(TruncateUtil.truncateLong(10, -9223372036854775800L))
        .isEqualTo(-9223372036854775800L);
    assertThat(TruncateUtil.truncateLong(10, -21L)).isEqualTo(-30L);
    assertThat(TruncateUtil.truncateLong(10, Long.MAX_VALUE)).isEqualTo(9223372036854775800L);
  }

  @Test
  public void testTruncateByteAndShortNearMinValue() {
    assertThatThrownBy(() -> TruncateUtil.truncateByte(10, Byte.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not fit in a byte");
    assertThat(TruncateUtil.truncateByte(10, (byte) -120)).isEqualTo((byte) -120);
    assertThat(TruncateUtil.truncateByte(10, (byte) -21)).isEqualTo((byte) -30);
    assertThatThrownBy(() -> TruncateUtil.truncateShort(10, Short.MIN_VALUE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not fit in a short");
    assertThat(TruncateUtil.truncateShort(10, (short) -32760)).isEqualTo((short) -32760);
  }
}
