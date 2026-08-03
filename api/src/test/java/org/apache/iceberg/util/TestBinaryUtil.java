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

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class TestBinaryUtil {

  @Test
  void truncateBinaryToShorterLength() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    ByteBuffer truncated = BinaryUtil.truncateBinary(input, 3);
    assertThat(truncated).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
  }

  @Test
  void truncateBinaryReturnsInputWhenLengthIsLarger() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3});
    // when the requested length covers the whole input, the same buffer is returned
    assertThat(BinaryUtil.truncateBinary(input, 5)).isSameAs(input);
    assertThat(BinaryUtil.truncateBinary(input, 3)).isSameAs(input);
  }

  @Test
  void truncateBinaryToZeroLength() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThat(BinaryUtil.truncateBinary(input, 0).remaining()).isEqualTo(0);
  }

  @Test
  void truncateBinaryRejectsNegativeLength() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertThatThrownBy(() -> BinaryUtil.truncateBinary(input, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncate length should be non-negative");
  }

  @Test
  void truncateBinaryUnsafeSharesBackingData() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
    ByteBuffer truncated = BinaryUtil.truncateBinaryUnsafe(input, 2);
    assertThat(truncated).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
    // the input buffer position and limit are not modified
    assertThat(input.position()).isEqualTo(0);
    assertThat(input.remaining()).isEqualTo(5);
  }

  @Test
  void truncateBinaryMaxIncrementsLastByte() {
    ByteBuffer input = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    ByteBuffer max = BinaryUtil.truncateBinaryMax(input, 2);
    assertThat(max).isEqualTo(ByteBuffer.wrap(new byte[] {1, 3}));
  }

  @Test
  void truncateBinaryMaxReturnsNullWhenAllBytesOverflow() {
    // 0xFF bytes overflow when incremented, so no greater bound of this length exists
    ByteBuffer input = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, 1});
    assertThat(BinaryUtil.truncateBinaryMax(input, 2)).isNull();
  }
}
