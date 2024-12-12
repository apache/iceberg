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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.RandomUtil;
import org.apache.iceberg.variants.Variants.PhysicalType;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSerializedObject {
  private static final SerializedMetadata EMPTY_METADATA =
      SerializedMetadata.from(SerializedMetadata.EMPTY_V1_BUFFER);
  private static final SerializedPrimitive i1 = SerializedPrimitive.from(new byte[] {0b1100, 1});
  private static final SerializedPrimitive i2 = SerializedPrimitive.from(new byte[] {0b1100, 2});
  private static final SerializedPrimitive i3 = SerializedPrimitive.from(new byte[] {0b1100, 3});
  private static final SerializedPrimitive vNull = SerializedPrimitive.from(new byte[] {0x00});
  private static final SerializedPrimitive vTrue = SerializedPrimitive.from(new byte[] {0b100});
  private static final SerializedPrimitive date =
      SerializedPrimitive.from(new byte[] {0b101100, (byte) 0xF4, 0x43, 0x00, 0x00});

  private final Random random = new Random(198725);

  @Test
  public void testEmptyObject() {
    SerializedObject object = SerializedObject.from(EMPTY_METADATA, new byte[] {0b10, 0x00});

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testEmptyLargeObject() {
    SerializedObject object =
        SerializedObject.from(EMPTY_METADATA, new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00});

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testSimpleObject() {
    Map<String, VariantValue> data = ImmutableMap.of("a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);

    Assertions.assertThat(object.get("d")).isEqualTo(null);
  }

  @Test
  public void testOutOfOrderKeys() {
    Map<String, VariantValue> data = ImmutableMap.of("b", i2, "a", i1, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), false /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("d")).isEqualTo(null);

    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
  }

  @Test
  public void testMixedValueTypes() {
    ByteBuffer meta =
        VariantTestUtil.createMetadata(
            ImmutableList.of("a", "b", "c", "d", "e", "f"), true /* sort names */);
    SerializedMetadata metadata = SerializedMetadata.from(meta);

    Map<String, VariantValue> inner = ImmutableMap.of("b", i2, "f", i3);
    ByteBuffer innerBuffer = VariantTestUtil.createObject(meta, inner);
    SerializedObject innerObject = SerializedObject.from(metadata, innerBuffer, innerBuffer.get(0));
    Map<String, VariantValue> data =
        ImmutableMap.of("a", i1, "b", date, "c", vNull, "d", vTrue, "e", innerObject);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(5);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.DATE);
    Assertions.assertThat(((SerializedPrimitive) object.get("b")).get()).isEqualTo(17396);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.NULL);
    Assertions.assertThat(((SerializedPrimitive) object.get("c")).get()).isEqualTo(null);
    Assertions.assertThat(object.get("d").type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    Assertions.assertThat(((SerializedPrimitive) object.get("d")).get()).isEqualTo(true);

    Assertions.assertThat(object.get("e").type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject actualInner = (SerializedObject) object.get("e").asObject();
    Assertions.assertThat(actualInner.numElements()).isEqualTo(2);
    Assertions.assertThat(actualInner.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(actualInner.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(actualInner.get("f").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(actualInner.get("f").asPrimitive().get()).isEqualTo((byte) 3);
  }

  @Test
  public void testTwoByteOffsets() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(300, random);
    SerializedPrimitive bigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, VariantValue> data = ImmutableMap.of("big", bigString, "a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    Assertions.assertThat(object.get("big").type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(object.get("big").asPrimitive().get()).isEqualTo(randomString);
  }

  @Test
  public void testThreeByteOffsets() {
    // a string larger than 65535 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(70_000, random);
    SerializedPrimitive reallyBigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, VariantValue> data =
        ImmutableMap.of("really-big", reallyBigString, "a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo((byte) 3);
    Assertions.assertThat(object.get("really-big").type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(object.get("really-big").asPrimitive().get()).isEqualTo(randomString);
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

    Assertions.assertThat(object.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(10_000);

    for (Map.Entry<String, VariantPrimitive<String>> entry : fields.entrySet()) {
      VariantValue fieldValue = object.get(entry.getKey());
      Assertions.assertThat(fieldValue.type()).isEqualTo(Variants.PhysicalType.STRING);
      Assertions.assertThat(fieldValue.asPrimitive().get()).isEqualTo(entry.getValue().get());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTwoByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 10_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, VariantValue> data = ImmutableMap.of("aa", i1, "AA", i2, "ZZ", i3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("aa").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("AA").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo((byte) 3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testThreeByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 100_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, VariantValue> data = ImmutableMap.of("aa", i1, "AA", i2, "ZZ", i3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    SerializedMetadata metadata = SerializedMetadata.from(meta);
    SerializedObject object = SerializedObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("aa").asPrimitive().get()).isEqualTo((byte) 1);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("AA").asPrimitive().get()).isEqualTo((byte) 2);
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo((byte) 3);
  }
}
