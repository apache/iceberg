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

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class DecimalVectorUtilTest {

  @Test
  public void testPadBigEndianBytes() {
    BigInteger bigInt = new BigInteger("12345");
    byte[] bytes = bigInt.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertEquals(16, paddedBytes.length);
    BigInteger result = new BigInteger(paddedBytes);
    assertEquals(bigInt, result);
  }

  @Test
  public void testPadBigEndianBytesNegative() {
    BigInteger bigInt = new BigInteger("-12345");
    byte[] bytes = bigInt.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertEquals(16, paddedBytes.length);
    BigInteger result = new BigInteger(paddedBytes);
    assertEquals(bigInt, result);
  }

  @Test
  public void testPadBigEndianBytesZero() {
    byte[] bytes = BigInteger.ZERO.toByteArray();
    byte[] paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertEquals(16, paddedBytes.length);
    BigInteger result = new BigInteger(paddedBytes);
    assertEquals(BigInteger.ZERO, result);

    bytes = new byte[0];
    paddedBytes = DecimalVectorUtil.padBigEndianBytes(bytes, 16);

    assertEquals(16, paddedBytes.length);
    result = new BigInteger(paddedBytes);
    assertEquals(BigInteger.ZERO, result);
  }

  @Test
  public void testPadBigEndianBytesOverflow() {
    byte[] bytes = new byte[17];
    Assertions.assertThatThrownBy(() -> DecimalVectorUtil.padBigEndianBytes(bytes, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Buffer size of 17 is larger than requested size of 16");
  }
}
