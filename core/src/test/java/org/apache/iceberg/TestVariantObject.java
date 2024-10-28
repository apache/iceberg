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

package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.Variants.PhysicalType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.RandomUtil;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestVariantObject {
  private static final VariantMetadata EMPTY_METADATA =
      VariantMetadata.from(VariantMetadata.EMPTY_V1_BUFFER);
  private static final VariantPrimitive i1 = VariantPrimitive.from(new byte[] {0b1100, 1});
  private static final VariantPrimitive i2 = VariantPrimitive.from(new byte[] {0b1100, 2});
  private static final VariantPrimitive i3 = VariantPrimitive.from(new byte[] {0b1100, 3});
  private static final VariantPrimitive vNull = VariantPrimitive.from(new byte[] {0x00});
  private static final VariantPrimitive vTrue = VariantPrimitive.from(new byte[] {0b100});
  private static final VariantPrimitive date =
      VariantPrimitive.from(new byte[] {0b101100, (byte) 0xF4, 0x43, 0x00, 0x00});

  private final Random random = new Random(198725);

  @Test
  public void testEmptyObject() {
    VariantObject object = VariantObject.from(EMPTY_METADATA, new byte[] {0b10, 0x00});

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testEmptyLargeObject() {
    VariantObject object =
        VariantObject.from(EMPTY_METADATA, new byte[] {0b1000010, 0x00, 0x00, 0x00, 0x00});

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(0);
  }

  @Test
  public void testSimpleObject() {
    Map<String, Variants.Serialized> data = ImmutableMap.of("a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("a")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("b")).get()).isEqualTo(2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("c")).get()).isEqualTo(3);

    Assertions.assertThat(object.get("d")).isEqualTo(null);
  }

  @Test
  public void testOutOfOrderKeys() {
    Map<String, Variants.Serialized> data = ImmutableMap.of("b", i2, "a", i1, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), false /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("d")).isEqualTo(null);

    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("c")).get()).isEqualTo(3);
    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("a")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("b")).get()).isEqualTo(2);
  }

  @Test
  public void testMixedValueTypes() {
    ByteBuffer meta =
        VariantTestUtil.createMetadata(
            ImmutableList.of("a", "b", "c", "d", "e", "f"), true /* sort names */);
    VariantMetadata metadata = VariantMetadata.from(meta);

    Map<String, Variants.Serialized> inner = ImmutableMap.of("b", i2, "f", i3);
    ByteBuffer innerBuffer = VariantTestUtil.createObject(meta, inner);
    VariantObject innerObject = VariantObject.from(metadata, innerBuffer, innerBuffer.get(0));
    Map<String, Variants.Serialized> data =
        ImmutableMap.of("a", i1, "b", date, "c", vNull, "d", vTrue, "e", innerObject);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(5);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("a")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.DATE);
    Assertions.assertThat(((VariantPrimitive) object.get("b")).get()).isEqualTo(17396);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.NULL);
    Assertions.assertThat(((VariantPrimitive) object.get("c")).get()).isEqualTo(null);
    Assertions.assertThat(object.get("d").type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    Assertions.assertThat(((VariantPrimitive) object.get("d")).get()).isEqualTo(true);

    Assertions.assertThat(object.get("e").type()).isEqualTo(PhysicalType.OBJECT);
    VariantObject actualInner = (VariantObject) object.get("e");
    Assertions.assertThat(actualInner.numElements()).isEqualTo(2);
    Assertions.assertThat(actualInner.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) actualInner.get("b")).get()).isEqualTo(2);
    Assertions.assertThat(actualInner.get("f").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) actualInner.get("f")).get()).isEqualTo(3);
  }

  @Test
  public void testTwoByteOffsets() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(300, random);
    VariantPrimitive bigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, Variants.Serialized> data =
        ImmutableMap.of("big", bigString, "a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("a")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("b")).get()).isEqualTo(2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("c")).get()).isEqualTo(3);
    Assertions.assertThat(object.get("big").type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(((VariantPrimitive) object.get("big")).get()).isEqualTo(randomString);
  }

  @Test
  public void testThreeByteOffsets() {
    // a string larger than 65535 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(70_000, random);
    VariantPrimitive reallyBigString = VariantTestUtil.createString(randomString);

    // note that order doesn't matter. fields are sorted by name
    Map<String, Variants.Serialized> data =
        ImmutableMap.of("really-big", reallyBigString, "a", i1, "b", i2, "c", i3);
    ByteBuffer meta = VariantTestUtil.createMetadata(data.keySet(), true /* sort names */);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("a")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("b").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("b")).get()).isEqualTo(2);
    Assertions.assertThat(object.get("c").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("c")).get()).isEqualTo(3);
    Assertions.assertThat(object.get("really-big").type()).isEqualTo(PhysicalType.STRING);
    Assertions.assertThat(((VariantPrimitive) object.get("really-big")).get())
        .isEqualTo(randomString);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTwoByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 10_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, Variants.Serialized> data = ImmutableMap.of("aa", i1, "AA", i2, "ZZ", i3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("aa")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("AA")).get()).isEqualTo(2);
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("ZZ")).get()).isEqualTo(3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testThreeByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 100_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, Variants.Serialized> data = ImmutableMap.of("aa", i1, "AA", i2, "ZZ", i3);

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    ByteBuffer meta = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    ByteBuffer value = VariantTestUtil.createObject(meta, data);

    VariantMetadata metadata = VariantMetadata.from(meta);
    VariantObject object = VariantObject.from(metadata, value, value.get(0));

    Assertions.assertThat(object.type()).isEqualTo(PhysicalType.OBJECT);
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("aa")).get()).isEqualTo(1);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("AA")).get()).isEqualTo(2);
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.INT8);
    Assertions.assertThat(((VariantPrimitive) object.get("ZZ")).get()).isEqualTo(3);
  }
}
