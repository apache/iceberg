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
package org.apache.iceberg.data.vortex;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

class TestGenericVortexVariantReaders {
  @Test
  void readShreddedPrimitive() {
    Field variantField =
        variantField(
            List.of(
                binaryField("metadata", false),
                field("typed_value", true, new ArrowType.Int(Integer.SIZE, true))));

    try (RootAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) variantField.createVector(allocator)) {
      vector.allocateNew();
      vector
          .getChild("metadata", VarBinaryVector.class)
          .setSafe(0, bytes(Variants.emptyMetadata()));
      ((IntVector) vector.getChild("typed_value")).setSafe(0, 34);
      vector.setIndexDefined(0);
      vector.setValueCount(1);

      Variant actual = GenericVortexReaders.variants().read(vector, 0);

      assertThat(actual.value().type()).isEqualTo(PhysicalType.INT32);
      assertThat(actual.value().asPrimitive().get()).isEqualTo(34);
    }
  }

  @Test
  void readPartiallyShreddedObject() {
    VariantMetadata metadata = Variants.metadata("a", "b", "c");
    ShreddedObject serialized = Variants.object(metadata);
    serialized.put("a", Variants.of(1));
    serialized.put("b", Variants.of(2));

    Field typedObject =
        new Field(
            "typed_value",
            new FieldType(true, ArrowType.Struct.INSTANCE, null),
            List.of(
                wrapper("a", field("typed_value", true, new ArrowType.Int(Integer.SIZE, true))),
                wrapper("b", null),
                wrapper("c", field("typed_value", true, ArrowType.Utf8.INSTANCE))));
    Field variantField =
        variantField(
            List.of(binaryField("metadata", false), binaryField("value", true), typedObject));

    try (RootAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) variantField.createVector(allocator)) {
      vector.allocateNew();
      vector.getChild("metadata", VarBinaryVector.class).setSafe(0, bytes(metadata));
      vector.getChild("value", VarBinaryVector.class).setSafe(0, bytes(serialized));

      StructVector typed = vector.getChild("typed_value", StructVector.class);
      StructVector aa = typed.getChild("a", StructVector.class);
      ((IntVector) aa.getChild("typed_value")).setSafe(0, 10);
      aa.setIndexDefined(0);
      typed.getChild("b", StructVector.class).setIndexDefined(0);
      StructVector ac = typed.getChild("c", StructVector.class);
      ((VarCharVector) ac.getChild("typed_value"))
          .setSafe(0, "new".getBytes(StandardCharsets.UTF_8));
      ac.setIndexDefined(0);
      typed.setIndexDefined(0);
      vector.setIndexDefined(0);
      vector.setValueCount(1);

      Variant actual = GenericVortexReaders.variants().read(vector, 0);

      assertThat(actual.value().type()).isEqualTo(PhysicalType.OBJECT);
      assertThat(actual.value().asObject().get("a").asPrimitive().get()).isEqualTo(10);
      assertThat(actual.value().asObject().get("b")).isNull();
      assertThat(actual.value().asObject().get("c").asPrimitive().get()).isEqualTo("new");
    }
  }

  @Test
  void readShreddedArray() {
    Field element =
        wrapper("element", field("typed_value", true, new ArrowType.Int(Integer.SIZE, true)));
    Field typedArray =
        new Field(
            "typed_value", new FieldType(true, ArrowType.List.INSTANCE, null), List.of(element));
    Field variantField = variantField(List.of(binaryField("metadata", false), typedArray));

    try (RootAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) variantField.createVector(allocator)) {
      vector.allocateNew();
      vector
          .getChild("metadata", VarBinaryVector.class)
          .setSafe(0, bytes(Variants.emptyMetadata()));
      ListVector typed = (ListVector) vector.getChild("typed_value");
      int start = typed.startNewValue(0);
      StructVector elements = (StructVector) typed.getDataVector();
      ((IntVector) elements.getChild("typed_value")).setSafe(start, 7);
      elements.setIndexDefined(start);
      elements.setNull(start + 1);
      typed.endValue(0, 2);
      vector.setIndexDefined(0);
      vector.setValueCount(1);

      Variant actual = GenericVortexReaders.variants().read(vector, 0);

      assertThat(actual.value().type()).isEqualTo(PhysicalType.ARRAY);
      assertThat(actual.value().asArray().numElements()).isEqualTo(2);
      assertThat(actual.value().asArray().get(0).asPrimitive().get()).isEqualTo(7);
      assertThat(actual.value().asArray().get(1).type()).isEqualTo(PhysicalType.NULL);
    }
  }

  private static Field variantField(List<Field> children) {
    return new Field("variant", new FieldType(true, ArrowType.Struct.INSTANCE, null), children);
  }

  private static Field wrapper(String name, Field typedValue) {
    List<Field> children =
        typedValue == null
            ? List.of(binaryField("value", true))
            : List.of(binaryField("value", true), typedValue);
    return new Field(name, new FieldType(false, ArrowType.Struct.INSTANCE, null), children);
  }

  private static Field binaryField(String name, boolean nullable) {
    return field(name, nullable, ArrowType.Binary.INSTANCE);
  }

  private static Field field(String name, boolean nullable, ArrowType type) {
    return new Field(name, new FieldType(nullable, type, null), null);
  }

  private static byte[] bytes(VariantMetadata metadata) {
    byte[] bytes = new byte[metadata.sizeInBytes()];
    metadata.writeTo(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }

  private static byte[] bytes(VariantValue value) {
    byte[] bytes = new byte[value.sizeInBytes()];
    value.writeTo(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }
}
