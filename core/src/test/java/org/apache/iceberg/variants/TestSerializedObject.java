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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.RandomUtil;
import org.apache.iceberg.variants.Variants.PhysicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSerializedObject {
  private static final SerializedMetadata EMPTY_METADATA =
      SerializedMetadata.from(SerializedMetadata.EMPTY_V1_BUFFER);
  private static final SerializedPrimitive I1 = SerializedPrimitive.from(new byte[] {0b1100, 1});
  private static final SerializedPrimitive I2 = SerializedPrimitive.from(new byte[] {0b1100, 2});
  private static final SerializedPrimitive I3 = SerializedPrimitive.from(new byte[] {0b1100, 3});
  private static final SerializedPrimitive NULL = SerializedPrimitive.from(new byte[] {0x00});
  private static final SerializedPrimitive TRUE = SerializedPrimitive.from(new byte[] {0b100});
  private static final SerializedPrimitive DATE =
      SerializedPrimitive.from(new byte[] {0b101100, (byte) 0xF4, 0x43, 0x00, 0x00});
  private static final byte[] UNSORTED_VALUES =
      new byte[] {
        0b10,
        0x03, // 3 item object
        0x00,
        0x01,
        0x02, // ascending key IDs (a, b, c)
        0x02,
        0x04,
        0x00,
        0x06, // values at offsets (2, 4, 0)
        0b1100,
        0x03, // c = 3 (int8)
        0b1100,
        0x01, // a = 1 (int8)
        0b1100,
        0x02 // b = 2 (int8)
      };

  private final Random random = new Random(198725);

  @Test
  public void testEmptyObject() {
    SerializedObject object = SerializedObject.from(EMPTY_METADATA, new byte[] {0b10, 0x00});

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testEmptyLargeObject() {
    SerializedObject object =
        SerializedObject.from(EMPTY_METADATA, new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00});

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testSimpleObject() {
    Map<String, VariantValue> data = ImmutableMap.of("a", I1, "b", I2, "c", I3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(3);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);

    assertThat(object.get("d")).isEqualTo(null);
  }

  @Test
  public void testUnsortedValues() {
    ByteBuffer meta =
        VariantTestUtil.createMetadata(Sets.newHashSet("a", "b", "c"), true /* sort names */);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, UNSORTED_VALUES);

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(3);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);

    assertThat(object.get("d")).isEqualTo(null);
  }

  @Test
  public void testOutOfOrderKeys() {
    Map<String, VariantValue> data = ImmutableMap.of("b", I2, "a", I1, "c", I3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), false /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(3);

    assertThat(object.get("d")).isEqualTo(null);

    assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
  }

  @Test
  public void testMixedValueTypes() {
    ByteBuffer meta =
        VariantTestUtil.createMetadata(
            ImmutableList.of("a", "b", "c", "d", "e", "f"), true /* sort names */);
    SerializedMetadata metadata = SerializedMetadata.from(meta);

    Map<String, VariantValue> inner = ImmutableMap.of("b", I2, "f", I3);
    ByteBuffer innerBuffer = VariantTestUtil.createObject(meta, inner);
    SerializedObject innerObject = SerializedObject.from(metadata, innerBuffer, innerBuffer.get(0));
    Map<String, VariantValue> data =
        ImmutableMap.of("a", I1, "b", DATE, "c", NULL, "d", TRUE, "e", innerObject);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(5);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.DATE);
    assertThat(((SerializedPrimitive) object.get("b")).get()).isEqualTo(17396);
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.NULL);
    assertThat(((SerializedPrimitive) object.get("c")).get()).isEqualTo(null);
    assertThat(object.get("d").type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    assertThat(((SerializedPrimitive) object.get("d")).get()).isEqualTo(true);

    assertThat(object.get("e").type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject actualInner = (SerializedObject) object.get("e").asObject();
    assertThat(actualInner.numElements()).isEqualTo(2);
    assertThat(actualInner.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(actualInner.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(actualInner.get("f").type()).isEqualTo(PhysicalType.INT8);
    assertThat(actualInner.get("f").asPrimitive().get()).isEqualTo((byte) 3);
  }

  @Test
  public void testTwoByteOffsets() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(300, random);
    SerializedPrimitive bigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, VariantValue> data = ImmutableMap.of("big", bigString, "a", I1, "b", I2, "c", I3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(4);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    assertThat(object.get("big").type()).isEqualTo(PhysicalType.STRING);
    assertThat(object.get("big").asPrimitive().get()).isEqualTo(randomString);
  }

  @Test
  public void testThreeByteOffsets() {
    // a string larger than 65535 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(70_000, random);
    SerializedPrimitive reallyBigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, VariantValue> data =
        ImmutableMap.of("really-big", reallyBigString, "a", I1, "b", I2, "c", I3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(4);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    assertThat(object.get("really-big").type()).isEqualTo(PhysicalType.STRING);
    assertThat(object.get("really-big").asPrimitive().get()).isEqualTo(randomString);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testLargeObject(boolean sortFieldNames) {
    Map<String, VariantPrimitive<String>> fields = Maps.newHashMap();
    for (int i = 0; i < 10_000; i += 1) {
      fields.put(
          RandomUtil.generateString(10, random),
          Variants.of(RandomUtil.generateString(10, random)));
    }

    ByteBuffer meta = VariantTestUtil.createMetadata(fields.keySet(), sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, (Map) fields);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(10_000);

    for (Map.Entry<String, VariantPrimitive<String>> entry : fields.entrySet()) {
      VariantValue fieldValue = object.get(entry.getKey());
      assertThat(fieldValue.type()).isEqualTo(Variants.PhysicalType.STRING);
      assertThat(fieldValue.asPrimitive().get()).isEqualTo(entry.getValue().get());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTwoByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 10_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, VariantValue> data = ImmutableMap.of("aa", I1, "AA", I2, "ZZ", I3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(3);

    assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("aa").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("AA").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo((byte) 3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testThreeByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 100_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, VariantValue> data = ImmutableMap.of("aa", I1, "AA", I2, "ZZ", I3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(object.numElements()).isEqualTo(3);

    assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("aa").asPrimitive().get()).isEqualTo((byte) 1);
    assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("AA").asPrimitive().get()).isEqualTo((byte) 2);
    assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo((byte) 3);
  }
}
