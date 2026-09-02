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
package org.apache.iceberg.variants;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;

class TestVariantTestUtil {
  private static final VariantMetadata EMPTY_METADATA = SerializedMetadata.EMPTY_V1_METADATA;
  private static final SerializedPrimitive NULL = SerializedPrimitive.from(new byte[] {0x00});
  private static final SerializedPrimitive TRUE = SerializedPrimitive.from(new byte[] {0b100});
  private static final SerializedPrimitive FALSE = SerializedPrimitive.from(new byte[] {0b1000});
  private static final SerializedPrimitive I34 = SerializedPrimitive.from(new byte[] {0b1100, 34});
  private static final SerializedPrimitive I1234 =
      SerializedPrimitive.from(new byte[] {0b10000, (byte) 0xD2, 0x04});
  private static final SerializedShortString A =
      SerializedShortString.from(new byte[] {0b101, 'a'});
  private static final SerializedShortString B =
      SerializedShortString.from(new byte[] {0b101, 'b'});
  private static final SerializedShortString C =
      SerializedShortString.from(new byte[] {0b101, 'c'});
  private static final SerializedShortString ICEBERG =
      SerializedShortString.from(new byte[] {0b11101, 'i', 'c', 'e', 'b', 'e', 'r', 'g'});

  @Test
  void serializedPrimitiveSizeMatchesBuffer() {
    assertThat(VariantTestUtil.sizeInBytes(NULL)).isEqualTo(NULL.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(TRUE)).isEqualTo(TRUE.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(I34)).isEqualTo(I34.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(I1234)).isEqualTo(I1234.sizeInBytes());
  }

  @Test
  void serializedShortStringSizeMatchesBuffer() {
    assertThat(VariantTestUtil.sizeInBytes(A)).isEqualTo(A.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(ICEBERG)).isEqualTo(ICEBERG.sizeInBytes());
  }

  @Test
  void serializedLongStringSizeMatchesBuffer() {
    String longString = RandomUtil.generateString(300, new Random(12345));
    VariantPrimitive<?> longVariant = VariantTestUtil.createString(longString);
    assertThat(VariantTestUtil.sizeInBytes(longVariant)).isEqualTo(longVariant.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(longVariant)).isEqualTo(5 + longString.length());
  }

  @Test
  void serializedArraySizeMatchesBuffer() {
    ByteBuffer buffer = VariantTestUtil.createArray(A, B, C, I34);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(VariantTestUtil.sizeInBytes(array)).isEqualTo(array.sizeInBytes());
    assertThat(VariantTestUtil.sizeInBytes(array)).isEqualTo(buffer.remaining());
  }

  @Test
  void createArrayMatchesSizeInBytes() {
    ByteBuffer buffer = VariantTestUtil.createArray(A, TRUE, ICEBERG, NULL, I1234, FALSE);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(array.sizeInBytes()).isEqualTo(buffer.remaining());
    assertThat(VariantTestUtil.sizeInBytes(array)).isEqualTo(buffer.remaining());
  }

  @Test
  void nestedArraySizeMatchesBuffer() {
    ByteBuffer innerBuffer = VariantTestUtil.createArray(A, B, C);
    SerializedArray inner = SerializedArray.from(EMPTY_METADATA, innerBuffer, innerBuffer.get(0));
    ByteBuffer outerBuffer = VariantTestUtil.createArray(inner, I34, ICEBERG);
    SerializedArray outer = SerializedArray.from(EMPTY_METADATA, outerBuffer, outerBuffer.get(0));

    assertThat(VariantTestUtil.sizeInBytes(outer)).isEqualTo(outerBuffer.remaining());
    assertThat(outer.sizeInBytes()).isEqualTo(outerBuffer.remaining());
  }

  @Test
  void sizeMatchesForSerializedPrimitivesByType() {
    // NULL: 1-byte header
    assertThat(VariantTestUtil.sizeInBytes(NULL)).isEqualTo(1);
    // BOOLEAN_TRUE / BOOLEAN_FALSE: 1-byte header
    assertThat(VariantTestUtil.sizeInBytes(TRUE)).isEqualTo(1);
    assertThat(VariantTestUtil.sizeInBytes(FALSE)).isEqualTo(1);
    // INT8: 1-byte header + 1-byte value
    assertThat(VariantTestUtil.sizeInBytes(I34)).isEqualTo(2);
    // INT16: 1-byte header + 2-byte value
    assertThat(VariantTestUtil.sizeInBytes(I1234)).isEqualTo(3);
    // short string: 1-byte header + utf8 length
    assertThat(VariantTestUtil.sizeInBytes(A)).isEqualTo(2);
    assertThat(VariantTestUtil.sizeInBytes(ICEBERG)).isEqualTo(1 + "iceberg".length());
  }

  @Test
  void sizeMatchesWrittenBytesForArray() {
    ByteBuffer arrayBuffer = VariantTestUtil.createArray(A, I34, ICEBERG, NULL, TRUE);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, arrayBuffer, arrayBuffer.get(0));

    // computed size must match the number of bytes actually written by writeTo
    ByteBuffer scratch =
        ByteBuffer.allocate(VariantTestUtil.sizeInBytes(array)).order(ByteOrder.LITTLE_ENDIAN);
    int written = array.writeTo(scratch, 0);
    assertThat(written).isEqualTo(VariantTestUtil.sizeInBytes(array));
  }
}
