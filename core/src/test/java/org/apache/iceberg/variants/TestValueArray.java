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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestValueArray {
  private static final VariantMetadata EMPTY_METADATA = Variants.emptyMetadata();
  private static final List<VariantValue> ELEMENTS =
      ImmutableList.of(
          Variants.of(34), Variants.of("iceberg"), Variants.of(new BigDecimal("12.21")));

  private final Random random = new Random(871925);

  @Test
  public void testElementAccess() {
    ValueArray arr = createArray(ELEMENTS);

    assertThat(arr.numElements()).isEqualTo(3);
    assertThat(arr.get(0)).isInstanceOf(VariantPrimitive.class);
    assertThat(arr.get(0).asPrimitive().get()).isEqualTo(34);
    assertThat(arr.get(1)).isInstanceOf(VariantPrimitive.class);
    assertThat(arr.get(1).asPrimitive().get()).isEqualTo("iceberg");
    assertThat(arr.get(2)).isInstanceOf(VariantPrimitive.class);
    assertThat(arr.get(2).asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testSerializationMinimalBuffer() {
    ValueArray arr = createArray(ELEMENTS);

    VariantValue value = roundTripMinimalBuffer(arr);

    assertThat(value).isInstanceOf(SerializedArray.class);
    SerializedArray actual = (SerializedArray) value;

    assertThat(actual.numElements()).isEqualTo(3);
    assertThat(actual.get(0)).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get(0).asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get(1), "iceberg");
    assertThat(actual.get(2)).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get(2).asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testSerializationLargeBuffer() {
    ValueArray arr = createArray(ELEMENTS);

    VariantValue value = roundTripLargeBuffer(arr);

    assertThat(value).isInstanceOf(SerializedArray.class);
    SerializedArray actual = (SerializedArray) value;

    assertThat(actual.numElements()).isEqualTo(3);
    assertThat(actual.get(0)).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get(0).asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get(1), "iceberg");
    assertThat(actual.get(2)).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get(2).asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @ParameterizedTest
  @ValueSource(ints = {300, 70_000, 16_777_300})
  public void testMultiByteOffsets(int len) {
    // Use a string exceeding 255 bytes to test value offset sizes of 2, 3, and 4 bytes
    String randomString = RandomUtil.generateString(len, random);
    VariantPrimitive<String> bigString = Variants.of(randomString);

    List<VariantValue> data = Lists.newArrayList();
    data.addAll(ELEMENTS);
    data.add(bigString);

    ValueArray shredded = createArray(data);
    VariantValue value = roundTripLargeBuffer(shredded);

    assertThat(value.type()).isEqualTo(PhysicalType.ARRAY);
    SerializedArray actualArray = (SerializedArray) value;
    assertThat(actualArray.numElements()).isEqualTo(4);

    assertThat(actualArray.get(0).type()).isEqualTo(PhysicalType.INT32);
    assertThat(actualArray.get(0).asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actualArray.get(1), "iceberg");
    assertThat(actualArray.get(2).type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(actualArray.get(2).asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
    VariantTestUtil.assertVariantString(actualArray.get(3), randomString);
  }

  @Test
  public void testLargeArray() {
    List<VariantValue> elements = Lists.newArrayList();
    for (int i = 0; i < 10_000; i += 1) {
      elements.add(Variants.of(RandomUtil.generateString(10, random)));
    }

    ValueArray arr = createArray(elements);
    VariantValue value = roundTripLargeBuffer(arr);

    assertThat(value.type()).isEqualTo(PhysicalType.ARRAY);
    SerializedArray actualArray = (SerializedArray) value;
    assertThat(actualArray.numElements()).isEqualTo(10_000);

    for (int i = 0; i < 10_000; i++) {
      VariantTestUtil.assertEqual(elements.get(i), actualArray.get(i));
    }
  }

  private static VariantValue roundTripMinimalBuffer(ValueArray arr) {
    ByteBuffer serialized = ByteBuffer.allocate(arr.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    arr.writeTo(serialized, 0);

    return Variants.value(EMPTY_METADATA, serialized);
  }

  private static VariantValue roundTripLargeBuffer(ValueArray arr) {
    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + arr.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    arr.writeTo(serialized, 300);

    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + arr.sizeInBytes());

    return Variants.value(EMPTY_METADATA, slice);
  }

  private static ValueArray createArray(List<VariantValue> elements) {
    ValueArray arr = new ValueArray();
    for (VariantValue element : elements) {
      arr.add(element);
    }

    return arr;
  }
}
