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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestShreddedObject {
  private static final Map<String, VariantValue> FIELDS =
      ImmutableMap.of(
          "a",
          Variants.of(34),
          "b",
          Variants.of("iceberg"),
          "c",
          Variants.of(new BigDecimal("12.21")));

  @Test
  public void testShreddedFields() {
    ShreddedObject object = createShreddedObject(FIELDS).second();

    Assertions.assertThat(object.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(object.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(object.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(object.get("c").asPrimitive().get())
        .isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationMinimalBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    ByteBuffer serialized =
        ByteBuffer.allocate(object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 0);
    SerializedObject actual = SerializedObject.from(metadata, serialized, serialized.get(0));

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testShreddedSerializationLargeBuffer() {
    Pair<SerializedMetadata, ShreddedObject> pair = createShreddedObject(FIELDS);
    SerializedMetadata metadata = pair.first();
    ShreddedObject object = pair.second();

    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + object.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    object.writeTo(serialized, 300);
    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + object.sizeInBytes());

    VariantValue value = Variants.from(metadata, slice);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationMinimalBuffer() {
    SerializedObject unshredded = createUnshreddedObject(FIELDS);

    ByteBuffer serialized =
        ByteBuffer.allocate(unshredded.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    unshredded.writeTo(serialized, 0);
    SerializedObject actual =
        SerializedObject.from(unshredded.metadata(), serialized, serialized.get(0));

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testUnshreddedObjectSerializationLargeBuffer() {
    SerializedObject unshredded = createUnshreddedObject(FIELDS);

    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + unshredded.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    unshredded.writeTo(serialized, 300);
    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + unshredded.sizeInBytes());

    VariantValue value = Variants.from(unshredded.metadata(), slice);

    Assertions.assertThat(value).isInstanceOf(SerializedObject.class);
    SerializedObject actual = (SerializedObject) value;

    Assertions.assertThat(actual.numElements()).isEqualTo(3);
    Assertions.assertThat(actual.get("a")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("a").asPrimitive().get()).isEqualTo(34);
    Assertions.assertThat(actual.get("b")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("b").asPrimitive().get()).isEqualTo("iceberg");
    Assertions.assertThat(actual.get("c")).isInstanceOf(VariantPrimitive.class);
    Assertions.assertThat(actual.get("c").asPrimitive().get())
        .isEqualTo(new BigDecimal("12.21"));
  }

  @Test
  public void testPartiallyShreddedObjectReplacement() {
    ShreddedObject partial = new ShreddedObject(createUnshreddedObject(FIELDS));

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
    ShreddedObject partial = new ShreddedObject(createUnshreddedObject(FIELDS));

    // missing fields are returned as null
    Assertions.assertThat(partial.get("d")).isNull();
  }

  @Test
  public void testPartiallyShreddedObjectPutMissingFieldFailure() {
    ShreddedObject partial = new ShreddedObject(createUnshreddedObject(FIELDS));

    // d is not defined in the variant metadata and will fail
    Assertions.assertThatThrownBy(() -> partial.put("d", Variants.ofIsoDate("2024-10-12")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field name in metadata: d");
  }

  @Test
  public void testPartiallyShreddedObjectSerializationMinimalBuffer() {
    SerializedObject unshredded = createUnshreddedObject(FIELDS);
    ShreddedObject partial = new ShreddedObject(unshredded);

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));

    ByteBuffer serialized =
        ByteBuffer.allocate(partial.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    partial.writeTo(serialized, 0);
    SerializedObject actual =
        SerializedObject.from(unshredded.metadata(), serialized, serialized.get(0));

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
    SerializedObject unshredded = createUnshreddedObject(FIELDS);
    ShreddedObject partial = new ShreddedObject(unshredded);

    // replace field c with a new value
    partial.put("c", Variants.ofIsoDate("2024-10-12"));

    ByteBuffer serialized =
        ByteBuffer.allocate(1000 + unshredded.sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    partial.writeTo(serialized, 300);
    ByteBuffer slice = serialized.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    slice.position(300);
    slice.limit(300 + unshredded.sizeInBytes());

    VariantValue value = Variants.from(unshredded.metadata(), slice);

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

  private Pair<SerializedMetadata, ShreddedObject> createShreddedObject(
      Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    SerializedMetadata metadata = SerializedMetadata.from(metadataBuffer);

    ShreddedObject object = new ShreddedObject(metadata);
    for (Map.Entry<String, VariantValue> field : fields.entrySet()) {
      object.put(field.getKey(), field.getValue());
    }

    return Pair.of(metadata, object);
  }

  private SerializedObject createUnshreddedObject(Map<String, VariantValue> fields) {
    ByteBuffer metadataBuffer = VariantTestUtil.createMetadata(fields.keySet(), false);
    return (SerializedObject)
        Variants.from(metadataBuffer, VariantTestUtil.createObject(metadataBuffer, fields));
  }
}
