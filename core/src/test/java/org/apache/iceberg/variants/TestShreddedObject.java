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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.RandomUtil;
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
    ShreddedObject object = createShreddedObject(FIELDS);

    assertThat(object.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    assertThat(object.get("b")).isInstanceOf(VariantPrimitive.class);
    assertThat(object.get("b").asPrimitive().get()).isEqualTo("iceberg");
    assertThat(object.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationMinimalBuffer() {
    ShreddedObject object = createShreddedObject(FIELDS);
    VariantMetadata metadata = object.metadata();

    VariantValue value = roundTripMinimalBuffer(object, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.numFields()).isEqualTo(3);
    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get("b"), "iceberg");
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationLargeBuffer() {
    ShreddedObject object = createShreddedObject(FIELDS);
    VariantMetadata metadata = object.metadata();

    VariantValue value = roundTripLargeBuffer(object, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.numFields()).isEqualTo(3);
    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get("b"), "iceberg");
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationMinimalBuffer() {
    ShreddedObject object = createUnshreddedObject(FIELDS);
    VariantMetadata metadata = object.metadata();

    VariantValue value = roundTripMinimalBuffer(object, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.numFields()).isEqualTo(3);
    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get("b"), "iceberg");
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationLargeBuffer() {
    ShreddedObject object = createUnshreddedObject(FIELDS);
    VariantMetadata metadata = object.metadata();

    VariantValue value = roundTripLargeBuffer(object, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.numFields()).isEqualTo(3);
    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(actual.get("b"), "iceberg");
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testPartiallyShreddedObjectReplacement() {
    ShreddedObject partial = createUnshreddedObject(FIELDS);

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));
    assertThat(partial.numFields()).isEqualTo(3);

    partial.remove("b");
    assertThat(partial.numFields()).isEqualTo(2);

    assertThat(partial.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(partial.get("a").asPrimitive().get()).isEqualTo(34);
    assertThat(partial.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(partial.get("c").type()).isEqualTo(PhysicalType.DATE);
    assertThat(partial.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @Test
  public void testPartiallyShreddedObjectGetMissingField() {
    ShreddedObject partial = createUnshreddedObject(FIELDS);

    // missing fields are returned as null
    assertThat(partial.get("d")).isNull();
  }

  @Test
  public void testPartiallyShreddedObjectPutMissingFieldFailure() {
    ShreddedObject partial = createUnshreddedObject(FIELDS);

    // d is not defined in the variant metadata and will fail
    assertThatThrownBy(() -> partial.put("d", Variants.ofIsoDate("2024-10-12")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field name in metadata: d");
  }

  @Test
  public void testPartiallyShreddedObjectSerializationMinimalBuffer() {
    ShreddedObject partial = createUnshreddedObject(FIELDS);
    VariantMetadata metadata = partial.metadata();

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));
    partial.remove("b");

    VariantValue value = roundTripMinimalBuffer(partial, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").type()).isEqualTo(PhysicalType.DATE);
    assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @Test
  public void testPartiallyShreddedObjectSerializationLargeBuffer() {
    ShreddedObject partial = createUnshreddedObject(FIELDS);
    VariantMetadata metadata = partial.metadata();

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));
    partial.remove("b");

    VariantValue value = roundTripLargeBuffer(partial, metadata);

    assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    assertThat(actual.get("c").type()).isEqualTo(PhysicalType.DATE);
    assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(DateTimeUtil.isoDateToDays("2024-10-12"));
  }

  @ParameterizedTest
  @ValueSource(ints = {300, 70_000, 16_777_300})
  public void testMultiByteOffsets(int len) {
    // Use a string exceeding 255 bytes to test value offset sizes of 2, 3, and 4 bytes
    String randomString = RandomUtil.generateString(len, random);
    VariantPrimitive<String> bigString = Variants.of(randomString);

    Map<String, VariantValue> data = Maps.newHashMap();
    data.putAll(FIELDS);
    data.put("big", bigString);

    ShreddedObject shredded = createShreddedObject(data);
    VariantValue value = roundTripLargeBuffer(shredded, shredded.metadata());

    assertThat(value.type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    assertThat(object.numFields()).isEqualTo(4);

    assertThat(object.get("a").type()).isEqualTo(PhysicalType.INT32);
    assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(object.get("b"), "iceberg");
    assertThat(object.get("c").type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(object.get("c").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
    VariantTestUtil.assertVariantString(object.get("big"), randomString);
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

    VariantMetadata metadata =
        Variants.metadata(VariantTestUtil.createMetadata(fields.keySet(), sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, (Map) fields);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    assertThat(value.type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    assertThat(object.numFields()).isEqualTo(10_000);

    for (Map.Entry<String, VariantPrimitive<String>> entry : fields.entrySet()) {
      VariantValue fieldValue = object.get(entry.getKey());
      VariantTestUtil.assertVariantString(fieldValue, entry.getValue().get());
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
    VariantMetadata metadata =
        Variants.metadata(VariantTestUtil.createMetadata(keySet, sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, data);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    assertThat(value.type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    assertThat(object.numFields()).isEqualTo(3);

    assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT32);
    assertThat(object.get("aa").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(object.get("AA"), "iceberg");
    assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
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
    VariantMetadata metadata =
        Variants.metadata(VariantTestUtil.createMetadata(keySet, sortFieldNames));

    ShreddedObject shredded = createShreddedObject(metadata, data);
    VariantValue value = roundTripLargeBuffer(shredded, metadata);

    assertThat(value.type()).isEqualTo(PhysicalType.OBJECT);
    SerializedObject object = (SerializedObject) value;
    assertThat(object.numFields()).isEqualTo(3);

    assertThat(object.get("aa").type()).isEqualTo(PhysicalType.INT32);
    assertThat(object.get("aa").asPrimitive().get()).isEqualTo(34);
    VariantTestUtil.assertVariantString(object.get("AA"), "iceberg");
    assertThat(object.get("ZZ").type()).isEqualTo(PhysicalType.DECIMAL4);
    assertThat(object.get("ZZ").asPrimitive().get()).isEqualTo(new BigDecimal("12.21"));
  }

  private static VariantValue roundTripMinimalBuffer(
      ShreddedObject object, VariantMetadata metadata) {
    ByteBuffer serialized =
        ByteBuffer.allocate(object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 0);

    return Variants.value(metadata, serialized);
  }

  private static VariantValue roundTripLargeBuffer(
      ShreddedObject object, VariantMetadata metadata) {
    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 300);

    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + object.sizeInBytes());

    return Variants.value(metadata, slice);
  }

  /** Creates a ShreddedObject with fields in its shredded map, using the given metadata */
  private static ShreddedObject createShreddedObject(
      VariantMetadata metadata, Map<String, VariantValue> fields) {
    ShreddedObject object = new ShreddedObject(metadata);
    for (Map.Entry<String, VariantValue> field : fields.entrySet()) {
      object.put(field.getKey(), field.getValue());
    }

    return object;
  }

  /** Creates a ShreddedObject with fields in its shredded map */
  private static ShreddedObject createShreddedObject(Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    VariantMetadata metadata = Variants.metadata(metadataBuffer);
    return createShreddedObject(metadata, fields);
  }

  /** Creates a ShreddedObject with fields in its unshredded base object */
  private static ShreddedObject createUnshreddedObject(Map<String, VariantValue> fields) {
    SerializedObject serialized = createSerializedObject(fields);
    return new ShreddedObject(serialized.metadata(), serialized);
  }

  private static SerializedObject createSerializedObject(Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    return (SerializedObject)
        Variants.value(
            Variants.metadata(metadataBuffer),
            VariantTestUtil.createObject(metadataBuffer, fields));
  }
}
