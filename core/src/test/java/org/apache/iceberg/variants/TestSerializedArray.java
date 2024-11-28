/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.variants;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.iceberg.variants.Variants.PhysicalType;
import org.apache.iceberg.util.RandomUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSerializedArray {
  private static final SerializedMetadata EMPTY_METADATA =
      SerializedMetadata.from(SerializedMetadata.EMPTY_V1_BUFFER);
  private static final SerializedPrimitive vNull = SerializedPrimitive.from(new byte[] {0x00});
  private static final SerializedPrimitive vTrue = SerializedPrimitive.from(new byte[] {0b100});
  private static final SerializedPrimitive vFalse = SerializedPrimitive.from(new byte[] {0b1000});
  private static final SerializedShortString str =
      SerializedShortString.from(new byte[] {0b11101, 'i', 'c', 'e', 'b', 'e', 'r', 'g'});
  private static final SerializedShortString a = SerializedShortString.from(new byte[] {0b101, 'a'});
  private static final SerializedShortString b = SerializedShortString.from(new byte[] {0b101, 'b'});
  private static final SerializedShortString c = SerializedShortString.from(new byte[] {0b101, 'c'});
  private static final SerializedShortString d = SerializedShortString.from(new byte[] {0b101, 'd'});
  private static final SerializedShortString e = SerializedShortString.from(new byte[] {0b101, 'e'});
  private static final SerializedPrimitive i34 = SerializedPrimitive.from(new byte[] {0b1100, 34});
  private static final SerializedPrimitive i1234 =
      SerializedPrimitive.from(new byte[] {0b10000, (byte) 0xD2, 0x04});
  private static final SerializedPrimitive date =
      SerializedPrimitive.from(new byte[] {0b101100, (byte) 0xF4, 0x43, 0x00, 0x00});

  private final Random random = new Random(374513);

  @Test
  public void testEmptyArray() {
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, new byte[] {0b0011, 0x00});

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(0);
  }

  @Test
  public void testEmptyLargeArray() {
    SerializedArray array =
        SerializedArray.from(EMPTY_METADATA, new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00});

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(0);
  }

  @Test
  public void testStringArray() {
    ByteBuffer buffer = VariantTestUtil.createArray(a, b, c, d, e);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(5);
    Assertions.assertThat(array.get(0).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(0).asPrimitive().get()).isEqualTo("a");
    Assertions.assertThat(array.get(1).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(1).asPrimitive().get()).isEqualTo("b");
    Assertions.assertThat(array.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(2).asPrimitive().get()).isEqualTo("c");
    Assertions.assertThat(array.get(3).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(3).asPrimitive().get()).isEqualTo("d");
    Assertions.assertThat(array.get(4).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(4).asPrimitive().get()).isEqualTo("e");

    Assertions.assertThatThrownBy(() -> array.get(5))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 5 out of bounds for length 5");
  }

  @Test
  public void testStringDifferentLengths() {
    ByteBuffer buffer = VariantTestUtil.createArray(a, b, c, str, d, e);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(6);
    Assertions.assertThat(array.get(0).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(0).asPrimitive().get()).isEqualTo("a");
    Assertions.assertThat(array.get(1).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(1).asPrimitive().get()).isEqualTo("b");
    Assertions.assertThat(array.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(2).asPrimitive().get()).isEqualTo("c");
    Assertions.assertThat(array.get(3).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(3).asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(array.get(4).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(4).asPrimitive().get()).isEqualTo("d");
    Assertions.assertThat(array.get(5).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(5).asPrimitive().get()).isEqualTo("e");

    Assertions.assertThatThrownBy(() -> array.get(6))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 6 out of bounds for length 6");
  }

  @Test
  public void testArrayOfMixedTypes() {
    ByteBuffer nestedBuffer = VariantTestUtil.createArray(a, c, d);
    SerializedArray nested = SerializedArray.from(EMPTY_METADATA, nestedBuffer, nestedBuffer.get(0));
    ByteBuffer buffer = VariantTestUtil.createArray(date, i34, str, vNull, e, b, vFalse, nested, vTrue, i1234);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(10);
    Assertions.assertThat(array.get(0).type()).isEqualTo(PhysicalType.DATE);
    Assertions.assertThat(array.get(0).asPrimitive().get()).isEqualTo(17396);
    Assertions.assertThat(array.get(1).type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(array.get(1).asPrimitive().get()).isEqualTo((byte) 34);
    Assertions.assertThat(array.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(2).asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(array.get(3).type()).isEqualTo(PhysicalType.NULL);
    Assertions.assertThat(array.get(3).asPrimitive().get()).isEqualTo(null);
    Assertions.assertThat(array.get(4).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(4).asPrimitive().get()).isEqualTo("e");
    Assertions.assertThat(array.get(5).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(5).asPrimitive().get()).isEqualTo("b");
    Assertions.assertThat(array.get(6).type()).isEqualTo(PhysicalType.BOOLEAN_FALSE);
    Assertions.assertThat(array.get(6).asPrimitive().get()).isEqualTo(false);
    Assertions.assertThat(array.get(8).type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    Assertions.assertThat(array.get(8).asPrimitive().get()).isEqualTo(true);
    Assertions.assertThat(array.get(9).type()).isEqualTo(PhysicalType.INT16);
    Assertions.assertThat(array.get(9).asPrimitive().get()).isEqualTo((short) 1234);

    Assertions.assertThatThrownBy(() -> array.get(10))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 10 out of bounds for length 10");

    Assertions.assertThat(array.get(7).type()).isEqualTo(PhysicalType.ARRAY);
    SerializedArray actualNested = (SerializedArray) array.get(7);
    Assertions.assertThat(actualNested.numElements()).isEqualTo(3);
    Assertions.assertThat(actualNested.get(0).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(actualNested.get(0).asPrimitive().get()).isEqualTo("a");
    Assertions.assertThat(actualNested.get(1).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(actualNested.get(1).asPrimitive().get()).isEqualTo("c");
    Assertions.assertThat(actualNested.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(actualNested.get(2).asPrimitive().get()).isEqualTo("d");

    Assertions.assertThatThrownBy(() -> actualNested.get(3))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 3 out of bounds for length 3");
  }

  @Test
  public void testTwoByteOffsets() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(300, random);
    SerializedPrimitive bigString = VariantTestUtil.createString(randomString);

    ByteBuffer buffer = VariantTestUtil.createArray(bigString, a, b, c);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(4);
    Assertions.assertThat(array.get(0).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(0).asPrimitive().get()).isEqualTo(randomString);
    Assertions.assertThat(array.get(1).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(1).asPrimitive().get()).isEqualTo("a");
    Assertions.assertThat(array.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(2).asPrimitive().get()).isEqualTo("b");
    Assertions.assertThat(array.get(3).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(3).asPrimitive().get()).isEqualTo("c");

    Assertions.assertThatThrownBy(() -> array.get(4))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 4 out of bounds for length 4");
  }

  @Test
  public void testThreeByteOffsets() {
    // a string larger than 65535 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(70_000, random);
    SerializedPrimitive reallyBigString = VariantTestUtil.createString(randomString);

    ByteBuffer buffer = VariantTestUtil.createArray(reallyBigString, a, b, c);
    SerializedArray array = SerializedArray.from(EMPTY_METADATA, buffer, buffer.get(0));

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(4);
    Assertions.assertThat(array.get(0).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(0).asPrimitive().get()).isEqualTo(randomString);
    Assertions.assertThat(array.get(1).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(1).asPrimitive().get()).isEqualTo("a");
    Assertions.assertThat(array.get(2).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(2).asPrimitive().get()).isEqualTo("b");
    Assertions.assertThat(array.get(3).type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(array.get(3).asPrimitive().get()).isEqualTo("c");

    Assertions.assertThatThrownBy(() -> array.get(4))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 4 out of bounds for length 4");
  }

  @Test
  public void testLargeArraySize() {
    SerializedArray array =
        SerializedArray.from(
            EMPTY_METADATA, new byte[] {0b10011, (byte) 0xFF, (byte) 0x01, 0x00, 0x00});

    Assertions.assertThat(array.type()).isEqualTo(PhysicalType.ARRAY);
    Assertions.assertThat(array.numElements()).isEqualTo(511);
  }

  @Test
  public void testNegativeArraySize() {
    Assertions.assertThatThrownBy(
            () ->
                SerializedArray.from(
                    EMPTY_METADATA,
                    new byte[] {0b10011, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}))
        .isInstanceOf(NegativeArraySizeException.class)
        .hasMessage("-1");
  }
}
