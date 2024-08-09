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
package org.apache.iceberg.arrow.vectorized.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class DecimalVectorUtilTest {

  @Test
  public void testPadBigEndianBytes() {
    BigInteger bigInt = new BigInteger("12345");
    byte[] bytes = bigInt.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertThat(paddedBytes).hasSize(16);
    BigInteger result = new BigInteger(paddedBytes);
    assertThat(result).isEqualTo(bigInt);
  }

  @Test
  public void testPadBigEndianBytesNegative() {
    BigInteger bigInt = new BigInteger("-12345");
    byte[] bytes = bigInt.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertThat(paddedBytes).hasSize(16);
    BigInteger result = new BigInteger(paddedBytes);
    assertThat(result).isEqualTo(bigInt);
  }

  @Test
  public void testPadBigEndianBytesZero() {
    byte[] bytes = BigInteger.ZERO.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertThat(paddedBytes).hasSize(16);
    BigInteger result = new BigInteger(paddedBytes);
    assertThat(result).isEqualTo(BigInteger.ZERO);

    bytes = new byte[0];
    paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertThat(paddedBytes).hasSize(16);
    result = new BigInteger(paddedBytes);
    assertThat(result).isEqualTo(BigInteger.ZERO);
  }

  @Test
  public void testPadBigEndianBytesOverflow() {
    byte[] bytes = new byte[17];
    assertThatThrownBy(() -> DecimalVectorUtil.padBigEndianBytes(bytes, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Buffer size of 17 is larger than requested size of 16");
  }
}
