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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSerializedArray {
  private static final VariantMetadata EMPTY_METADATA = SerializedMetadata.EMPTY_V1_METADATA;
  private static final SerializedPrimitive NULL = SerializedPrimitive.from(new byte[] {0x00});
  private static final SerializedPrimitive TRUE = SerializedPrimitive.from(new byte[] {0b100});
  private static final SerializedPrimitive FALSE = SerializedPrimitive.from(new byte[] {0b1000});
  private static final SerializedShortString STR =
      SerializedShortString.from(new byte[] {0b11101, 'i', 'c', 'e', 'b', 'e', 'r', 'g'});
  private static final SerializedShortString A =
      SerializedShortString.from(new byte[] {0b101, 'a'});
  private static final SerializedShortString B =
      SerializedShortString.from(new byte[] {0b101, 'b'});
  private static final SerializedShortString C =
      SerializedShortString.from(new byte[] {0b101, 'c'});
  private static final SerializedShortString D =
      SerializedShortString.from(new byte[] {0b101, 'd'});
  private static final SerializedShortString E =
      SerializedShortString.from(new byte[] {0b101, 'e'});
  private static final SerializedPrimitive I34 = SerializedPrimitive.from(new byte[] {0b1100, 34});
  private static final SerializedPrimitive I1234 =
      SerializedPrimitive.from(new byte[] {0b10000, (byte) 0xD2, 0x04});
  private static final SerializedPrimitive DATE =
      SerializedPrimitive.from(new byte[] {0b101100, (byte) 0xF4, 0x43, 0x00, 0x00});

  private final Random random = new Random(374513);

  @Test
  public void testEmptyArray() {
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, new byte[] {0b0011, 0x00});

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(0);
  }

  @Test
  public void testEmptyLargeArray() {
    SerializedArray array =
        SerializedArray.from(EMPTY_METADATA, new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00});

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(0);
  }

  @Test
  public void testStringArray() {
    ByteBuffer buffer = VariantTestUtil.createArray(A, B, C, D, E);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(5);
    VariantTestUtil.assertVariantString(array.get(0), "a");
    VariantTestUtil.assertVariantString(array.get(1), "b");
    VariantTestUtil.assertVariantString(array.get(2), "c");
    VariantTestUtil.assertVariantString(array.get(3), "d");
    VariantTestUtil.assertVariantString(array.get(4), "e");

    assertThatThrownBy(() -> array.get(5))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 5 out of bounds for length 5");
  }

  @Test
  public void testStringDifferentLengths() {
    ByteBuffer buffer = VariantTestUtil.createArray(A, B, C, STR, D, E);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(6);
    VariantTestUtil.assertVariantString(array.get(0), "a");
    VariantTestUtil.assertVariantString(array.get(1), "b");
    VariantTestUtil.assertVariantString(array.get(2), "c");
    VariantTestUtil.assertVariantString(array.get(3), "iceberg");
    VariantTestUtil.assertVariantString(array.get(4), "d");
    VariantTestUtil.assertVariantString(array.get(5), "e");

    assertThatThrownBy(() -> array.get(6))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 6 out of bounds for length 6");
  }

  @Test
  public void testArrayOfMixedTypes() {
    ByteBuffer nestedBuffer = VariantTestUtil.createArray(A, C, D);
    SerializedArray nested =
        SerializedArray.from(EMPTY_METADATA, nestedBuffer, nestedBuffer.get(0));
    ByteBuffer buffer =
        VariantTestUtil.createArray(DATE, I34, STR, NULL, E, B, FALSE, nested, TRUE, I1234);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(10);
    assertThat(array.get(0).type()).isEqualTo(PhysicalType.DATE);
    assertThat(array.get(0).asPrimitive().get()).isEqualTo(17396);
    assertThat(array.get(1).type()).isEqualTo(PhysicalType.INT8);
    assertThat(array.get(1).asPrimitive().get()).isEqualTo((byte) 34);
    VariantTestUtil.assertVariantString(array.get(2), "iceberg");
    assertThat(array.get(3).type()).isEqualTo(PhysicalType.NULL);
    assertThat(array.get(3).asPrimitive().get()).isEqualTo(null);
    VariantTestUtil.assertVariantString(array.get(4), "e");
    VariantTestUtil.assertVariantString(array.get(5), "b");
    assertThat(array.get(6).type()).isEqualTo(PhysicalType.BOOLEAN_FALSE);
    assertThat(array.get(6).asPrimitive().get()).isEqualTo(false);
    assertThat(array.get(8).type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    assertThat(array.get(8).asPrimitive().get()).isEqualTo(true);
    assertThat(array.get(9).type()).isEqualTo(PhysicalType.INT16);
    assertThat(array.get(9).asPrimitive().get()).isEqualTo((short) 1234);

    assertThatThrownBy(() -> array.get(10))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 10 out of bounds for length 10");

    assertThat(array.get(7).type()).isEqualTo(PhysicalType.ARRAY);
    SerializedArray actualNested = (SerializedArray) array.get(7);
    assertThat(actualNested.numElements()).isEqualTo(3);
    VariantTestUtil.assertVariantString(actualNested.get(0), "a");
    VariantTestUtil.assertVariantString(actualNested.get(1), "c");
    VariantTestUtil.assertVariantString(actualNested.get(2), "d");

    assertThatThrownBy(() -> actualNested.get(3))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 3 out of bounds for length 3");
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        300, // a big string larger than 255 bytes to push the value offset size above 1 byte to
        // test TwoByteOffsets
        70_000 // a really-big string larger than 65535 bytes to push the value offset size above 1
        // byte to test ThreeByteOffsets
      })
  public void testMultiByteOffsets(int multiByteOffset) {
    String randomString = RandomUtil.generateString(multiByteOffset, random);
    SerializedPrimitive bigString = VariantTestUtil.createString(randomString);

    ByteBuffer buffer = VariantTestUtil.createArray(bigString, A, B, C);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(4);
    VariantTestUtil.assertVariantString(array.get(0), randomString);
    VariantTestUtil.assertVariantString(array.get(1), "a");
    VariantTestUtil.assertVariantString(array.get(2), "b");
    VariantTestUtil.assertVariantString(array.get(3), "c");

    assertThatThrownBy(() -> array.get(4))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 4 out of bounds for length 4");
  }

  @Test
  public void testLargeArraySize() {
    SerializedArray array =
        SerializedArray.from(
            EMPTY_METADATA, new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00});

    assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(array.numElements()).isEqualTo(511);
  }

  @Test
  public void testNegativeArraySize() {
    assertThatThrownBy(
            () ->
                SerializedArray.from(
                    EMPTY_METADATA,
                    new byte[] {0b10011, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
        .isInstanceOf(NegativeArraySizeException.class)
        .hasMessage("-1");
  }
}
