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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class TestDecimalUtil {

  @Test
  void toReusedFixLengthBytesLeftPadsPositiveValueWithZeros() {
    byte[] reuseBuf = new byte[4];
    byte[] result = DecimalUtil.toReusedFixLengthBytes(5, 2, new BigDecimal("1.23"), reuseBuf);

    // unscaled value 123 fits in a single byte and is right-aligned in the buffer
    assertThat(result).isEqualTo(new byte[] {0x00, 0x00, 0x00, 0x7B});
    // the provided buffer is reused rather than a new array being allocated
    assertThat(result).isSameAs(reuseBuf);
  }

  @Test
  void toReusedFixLengthBytesLeftPadsNegativeValueWithOnes() {
    byte[] reuseBuf = new byte[4];
    byte[] result = DecimalUtil.toReusedFixLengthBytes(5, 2, new BigDecimal("-1.23"), reuseBuf);

    // negative values are sign-extended with 0xFF padding bytes
    assertThat(result).isEqualTo(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x85});
    assertThat(result).isSameAs(reuseBuf);
  }

  @Test
  void toReusedFixLengthBytesPadsZeroValueWithZeros() {
    byte[] reuseBuf = new byte[4];
    byte[] result = DecimalUtil.toReusedFixLengthBytes(5, 2, new BigDecimal("0.00"), reuseBuf);

    assertThat(result).isEqualTo(new byte[] {0x00, 0x00, 0x00, 0x00});
    assertThat(result).isSameAs(reuseBuf);
  }

  @Test
  void toReusedFixLengthBytesReturnsUnscaledBytesWhenLengthAlreadyMatches() {
    byte[] reuseBuf = new byte[2];
    // unscaled value 258 serializes to exactly two bytes, so no padding is needed
    byte[] result = DecimalUtil.toReusedFixLengthBytes(5, 2, new BigDecimal("2.58"), reuseBuf);

    assertThat(result).isEqualTo(new byte[] {0x01, 0x02});
    // the exact-length path returns the freshly serialized array, leaving the buffer untouched
    assertThat(result).isNotSameAs(reuseBuf);
    assertThat(reuseBuf).isEqualTo(new byte[] {0x00, 0x00});
  }

  @Test
  void toReusedFixLengthBytesRejectsWrongScale() {
    assertThatThrownBy(
            () -> DecimalUtil.toReusedFixLengthBytes(5, 2, new BigDecimal("1.234"), new byte[4]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("wrong scale");
  }

  @Test
  void toReusedFixLengthBytesRejectsValueLargerThanPrecision() {
    assertThatThrownBy(
            () -> DecimalUtil.toReusedFixLengthBytes(3, 2, new BigDecimal("1234.56"), new byte[4]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too large");
  }
}
