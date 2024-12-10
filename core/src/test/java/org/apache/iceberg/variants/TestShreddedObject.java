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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.RandomUtil;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestShreddedObject {
  private static final Map<String, VariantValue> FIELDS =
      ImmutableMap.of(
          "a",
          Variants.of(34),
          "b",
          Variants.of("iceberg"),
          "c",
          Variants.of(new BigDecimal("12.21")));

  private final Random random = new Random(871925);

  @Test
  public void testShreddedFields() {
    ShreddedObject object = createShreddedObject(FIELDS).second();

    Assertions.assertThat(object.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationMinimalBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    VariantValue value = roundTripMinimalBuffer(object, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationLargeBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    VariantValue value = roundTripLargeBuffer(object, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationMinimalBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createUnshreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    VariantValue value = roundTripMinimalBuffer(object, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationLargeBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createUnshreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    VariantValue value = roundTripLargeBuffer(object, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testPartiallyShreddedObjectReplacement() {
    ShreddedObject partial = createUnshreddedObject(FIELDS).second();

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));

    Assertions.assertThat(partial.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(partial.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(partial.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(partial.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(partial.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(partial.get("c").type()).isEqualTo(Variants.PhysicalType.DATE);
    Assertions.assertThat(partial.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @Test
  public void testPartiallyShreddedObjectGetMissingField() {
    ShreddedObject partial = createUnshreddedObject(FIELDS).second();

    // missing fields are returned as null
    Assertions.assertThat(partial.get("d")).isNull();
  }

  @Test
  public void testPartiallyShreddedObjectPutMissingFieldFailure() {
    ShreddedObject partial = createUnshreddedObject(FIELDS).second();

    // d is not defined in the variant metadata and will fail
    Assertions.assertThatThrownBy(() -> partial.put("d", Variants.ofIsoDate("2024-10-12")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field name in metadata: d");
  }

  @Test
  public void testPartiallyShreddedObjectSerializationMinimalBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createUnshreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject partial = pair.second();

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));

    VariantValue value = roundTripMinimalBuffer(partial, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").type()).isEqualTo(Variants.PhysicalType.DATE);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @Test
  public void testPartiallyShreddedObjectSerializationLargeBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createUnshreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject partial = pair.second();

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));

    VariantValue value = roundTripLargeBuffer(partial, metadata);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").type()).isEqualTo(Variants.PhysicalType.DATE);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @Test
  public void testTwoByteOffsets() {
    // a string larger than 255 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(300, random);
    SerializedPrimitive bigString = VariantTestUtil.createString(randomString);

    Map<String, VariantValue> data = Maps.newHashMap();
    data.putAll(FIELDS);
    data.put("big", bigString);

    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(data);
    VariantValue value = roundTripLargeBuffer(pair.second(), pair.first());

    Assertions.assertThat(value.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(Variants.PhysicalType.INT32);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("b").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("c").type()).isEqualTo(Variants.PhysicalType.DECIMAL4);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
    Assertions.assertThat(object.get("big").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("big").asPrimitive().get()).isEqualTo(randomString);
  }

  @Test
  public void testThreeByteOffsets() {
    // a string larger than 65535 bytes to push the value offset size above 1 byte
    String randomString = RandomUtil.generateString(70_000, random);
    SerializedPrimitive reallyBigString = VariantTestUtil.createString(randomString);

    Map<String, VariantValue> data = Maps.newHashMap();
    data.putAll(FIELDS);
    data.put("really-big", reallyBigString);

    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(data);
    VariantValue value = roundTripLargeBuffer(pair.second(), pair.first());

    Assertions.assertThat(value.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    Assertions.assertThat(object.numElements()).isEqualTo(4);

    Assertions.assertThat(object.get("a").type()).isEqualTo(Variants.PhysicalType.INT32);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("b").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("c").type()).isEqualTo(Variants.PhysicalType.DECIMAL4);
    Assertions.assertThat(object.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
    Assertions.assertThat(object.get("really-big").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("really-big").asPrimitive().get()).isEqualTo(randomString);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testLargeObject(boolean sortFieldNames) {
    Map<String, VariantPrimitive<String>> fields = Maps.newHashMap();
    for (int i = 0; i < 10_000; i += 1) {
      fields.put(RandomUtil.generateString(10, random), Variants.of(RandomUtil.generateString(10, random)));
    }

    SerializedMetadata metadata =
        SerializedMetadata.from(VariantTestUtil.createMetadata(fields.keySet(), sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, (Map) fields);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    Assertions.assertThat(value.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
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

    Map<String, VariantValue> data =
        ImmutableMap.of("aa", FIELDS.get("a"), "AA", FIELDS.get("b"), "ZZ", FIELDS.get("c"));

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    SerializedMetadata metadata =
        SerializedMetadata.from(VariantTestUtil.createMetadata(keySet, sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, data);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    Assertions.assertThat(value.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(Variants.PhysicalType.INT32);
    Assertions.assertThat(object.get("aa").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("AA").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(Variants.PhysicalType.DECIMAL4);
    Assertions.assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testThreeByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    for (int i = 0; i < 100_000; i += 1) {
      keySet.add(RandomUtil.generateString(10, random));
    }

    Map<String, VariantValue> data =
        ImmutableMap.of("aa", FIELDS.get("a"), "AA", FIELDS.get("b"), "ZZ", FIELDS.get("c"));

    // create metadata from the large key set and the actual keys
    keySet.addAll(data.keySet());
    SerializedMetadata metadata =
        SerializedMetadata.from(VariantTestUtil.createMetadata(keySet, sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, data);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    Assertions.assertThat(value.type()).isEqualTo(Variants.PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    Assertions.assertThat(object.numElements()).isEqualTo(3);

    Assertions.assertThat(object.get("aa").type()).isEqualTo(Variants.PhysicalType.INT32);
    Assertions.assertThat(object.get("aa").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("AA").type()).isEqualTo(Variants.PhysicalType.STRING);
    Assertions.assertThat(object.get("AA").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("ZZ").type()).isEqualTo(Variants.PhysicalType.DECIMAL4);
    Assertions.assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  static VariantValue roundTripMinimalBuffer(ShreddedObject object, SerializedMetadata metadata) {
    ByteBuffer serialized =
        ByteBuffer.allocate(object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 0);

    return Variants.from(metadata, serialized);
  }

  static VariantValue roundTripLargeBuffer(ShreddedObject object, SerializedMetadata metadata) {
    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 300);

    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + object.sizeInBytes());

    return Variants.from(metadata, slice);
  }

  private static ShreddedObject createShreddedObject(
      SerializedMetadata metadata, Map<String, VariantValue> fields) {
    ShreddedObject object = new ShreddedObject(metadata);
    for (Map.Entry<String, VariantValue> field : fields.entrySet()) {
      object.put(field.getKey(), field.getValue());
    }

    return object;
  }

  private static Pair<SerializedMetadata, ShreddedObject> createShreddedObject(
      Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    SerializedMetadata metadata = SerializedMetadata.from(metadataBuffer);
    return Pair.of(metadata, createShreddedObject(metadata, fields));
  }

  private static Pair<SerializedMetadata, ShreddedObject> createUnshreddedObject(
      Map<String, VariantValue> fields) {
    SerializedObject serialized = createSerializedObject(fields);
    return Pair.of(serialized.metadata(), new ShreddedObject(serialized));
  }

  private static SerializedObject createSerializedObject(Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    return (SerializedObject)
        Variants.from(metadataBuffer, VariantTestUtil.createObject(metadataBuffer, fields));
  }
}
