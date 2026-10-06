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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestVortexSchemas {
  private static final Map<String, String> VARIANT_METADATA =
      Map.of(
          ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
          VortexSchemas.VARIANT_EXTENSION_NAME,
          ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA,
          "");

  // A struct nested inside a struct, plus a list, so the conversion exercises every recursive
  // branch and id is forced to stay unique across siblings, nested structs, and list elements.
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(
              2,
              "location",
              Types.StructType.of(
                  required(3, "lat", Types.DoubleType.get()),
                  required(4, "long", Types.DoubleType.get()))),
          optional(5, "tags", Types.ListType.ofRequired(6, Types.StringType.get())),
          optional(
              7,
              "nested",
              Types.StructType.of(
                  required(
                      8,
                      "inner",
                      Types.StructType.of(required(9, "x", Types.IntegerType.get()))))));

  private static final Schema MAP_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(
              2,
              "props",
              Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.IntegerType.get())));

  @Test
  void convertLocalArrowStructTypes() {
    assertStructRoundTrip(VortexSchemas.convert(VortexSchemas.toArrowSchema(SCHEMA)));
  }

  @Test
  void convertRelocatedArrowStructTypes() {
    assertStructRoundTrip(VortexSchemas.convert(VortexSchemas.toVortexArrowSchema(SCHEMA)));
  }

  @Test
  void convertLargeAndFixedSizeLists() {
    // toArrowSchema only emits ArrowType.List for Iceberg lists, so build the Arrow schema directly
    // to exercise the LargeList and FixedSizeList branches a real Vortex file can produce.
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            List.of(
                listField(
                    "big", ArrowType.LargeList.INSTANCE, new ArrowType.Int(Integer.SIZE, true)),
                listField(
                    "vec",
                    new ArrowType.FixedSizeList(3),
                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));

    Schema converted = VortexSchemas.convert(arrowSchema);

    assertThat(converted.findField("big").type()).isInstanceOf(Types.ListType.class);
    assertThat(converted.findField("big").isOptional()).isTrue();
    assertThat(converted.findType("big.element")).isEqualTo(Types.IntegerType.get());
    assertThat(converted.findField("vec").type()).isInstanceOf(Types.ListType.class);
    assertThat(converted.findType("vec.element")).isEqualTo(Types.FloatType.get());
    assertThat(TypeUtil.indexById(converted.asStruct())).hasSize(4);
  }

  @Test
  void variantToArrowUsesCanonicalUnshreddedStorage() {
    Schema icebergSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "v", Types.VariantType.get()));

    Field variant = VortexSchemas.toArrowSchema(icebergSchema).findField("v");

    assertThat(VortexSchemas.isVariantField(variant)).isTrue();
    assertThat(variant.isNullable()).isTrue();
    assertThat(variant.getType()).isEqualTo(ArrowType.Struct.INSTANCE);
    assertThat(variant.getChildren()).hasSize(2);

    Field metadata = variant.getChildren().get(0);
    assertThat(metadata.getName()).isEqualTo("metadata");
    assertThat(metadata.isNullable()).isFalse();
    assertThat(metadata.getType()).isEqualTo(ArrowType.Binary.INSTANCE);

    Field value = variant.getChildren().get(1);
    assertThat(value.getName()).isEqualTo("value");
    assertThat(value.isNullable()).isTrue();
    assertThat(value.getType()).isEqualTo(ArrowType.Binary.INSTANCE);
  }

  @Test
  void requiredVariantToVortexArrowKeepsValueChildNullable() {
    Schema icebergSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "v", Types.VariantType.get()));

    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field variant =
        VortexSchemas.toVortexArrowSchema(icebergSchema).findField("v");
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field value =
        variant.getChildren().get(1);

    assertThat(variant.isNullable()).isFalse();
    assertThat(value.getName()).isEqualTo("value");
    assertThat(value.isNullable()).isTrue();
  }

  @Test
  void variantFromArrowAcceptsTypedValueOnlyStorage() {
    Field variant =
        variantField(
            "v",
            true,
            List.of(
                binaryField("metadata", false),
                new Field(
                    "typed_value",
                    new FieldType(true, new ArrowType.Int(Integer.SIZE, true), null),
                    null)));

    Schema converted =
        VortexSchemas.convert(new org.apache.arrow.vector.types.pojo.Schema(List.of(variant)));

    assertThat(converted.columns()).containsExactly(optional(0, "v", Types.VariantType.get()));
  }

  @Test
  void variantFromArrowRequiresMetadataChild() {
    Field variant = variantField("v", true, List.of(binaryField("value", true)));

    assertThatThrownBy(
            () ->
                VortexSchemas.convert(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(variant))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metadata");
  }

  @Test
  void variantFromArrowRequiresValueOrTypedValueChild() {
    Field variant = variantField("v", true, List.of(binaryField("metadata", false)));

    assertThatThrownBy(
            () ->
                VortexSchemas.convert(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(variant))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("value or typed_value");
  }

  @Test
  void variantFromArrowRequiresNullableValueChild() {
    Field variant =
        variantField(
            "v", true, List.of(binaryField("metadata", false), binaryField("value", false)));

    assertThatThrownBy(
            () ->
                VortexSchemas.convert(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(variant))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("value child must be nullable");
  }

  @Test
  void variantFromArrowRequiresNullableTypedValueChild() {
    Field variant =
        variantField(
            "v",
            true,
            List.of(
                binaryField("metadata", false),
                new Field(
                    "typed_value",
                    new FieldType(false, new ArrowType.Int(Integer.SIZE, true), null),
                    null)));

    assertThatThrownBy(
            () ->
                VortexSchemas.convert(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of(variant))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("typed_value child must be nullable");
  }

  @Test
  void mapToArrowUsesCanonicalEntriesLayout() {
    Field map = VortexSchemas.toArrowSchema(MAP_SCHEMA).findField("props");

    assertThat(map.isNullable()).isTrue();
    assertThat(map.getType()).isEqualTo(new ArrowType.Map(false));
    assertThat(map.getChildren()).hasSize(1);

    Field entries = map.getChildren().get(0);
    assertThat(entries.getName()).isEqualTo(VortexSchemas.MAP_ENTRIES_NAME);
    assertThat(entries.isNullable()).isFalse();
    assertThat(entries.getType()).isEqualTo(ArrowType.Struct.INSTANCE);

    Field key = entries.getChildren().get(0);
    assertThat(key.getName()).isEqualTo(VortexSchemas.MAP_KEY_NAME);
    assertThat(key.isNullable()).isFalse();
    assertThat(key.getType()).isEqualTo(ArrowType.Utf8.INSTANCE);

    Field value = entries.getChildren().get(1);
    assertThat(value.getName()).isEqualTo(VortexSchemas.MAP_VALUE_NAME);
    assertThat(value.isNullable()).isTrue();
    assertThat(value.getType()).isEqualTo(new ArrowType.Int(Integer.SIZE, true));
  }

  @Test
  void mapToVortexArrowUsesCanonicalEntriesLayout() {
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field map =
        VortexSchemas.toVortexArrowSchema(MAP_SCHEMA).findField("props");

    assertThat(map.getType())
        .isEqualTo(
            new dev.vortex.relocated.org.apache.arrow.vector.types.pojo.ArrowType.Map(false));

    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field entries =
        map.getChildren().get(0);
    assertThat(entries.getName()).isEqualTo(VortexSchemas.MAP_ENTRIES_NAME);
    assertThat(entries.isNullable()).isFalse();
    assertThat(entries.getChildren().get(0).getName()).isEqualTo(VortexSchemas.MAP_KEY_NAME);
    assertThat(entries.getChildren().get(0).isNullable()).isFalse();
    assertThat(entries.getChildren().get(1).getName()).isEqualTo(VortexSchemas.MAP_VALUE_NAME);
    assertThat(entries.getChildren().get(1).isNullable()).isTrue();
  }

  @Test
  void convertMapBackToIceberg() {
    for (Schema roundTrip :
        List.of(
            VortexSchemas.convert(VortexSchemas.toArrowSchema(MAP_SCHEMA)),
            VortexSchemas.convert(VortexSchemas.toVortexArrowSchema(MAP_SCHEMA)))) {
      assertThat(roundTrip.findField("props").type()).isInstanceOf(Types.MapType.class);
      assertThat(roundTrip.findField("props").isOptional()).isTrue();
      assertThat(roundTrip.findType("props.key")).isEqualTo(Types.StringType.get());
      assertThat(roundTrip.findType("props.value")).isEqualTo(Types.IntegerType.get());
      assertThat(roundTrip.findField("props").type().asMapType().isValueOptional()).isTrue();
      // id, props, key, value
      assertThat(TypeUtil.indexById(roundTrip.asStruct())).hasSize(4);
    }
  }

  @Test
  void convertRequiredMapValueBackToIceberg() {
    Schema icebergSchema =
        new Schema(
            optional(
                1,
                "props",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.IntegerType.get())));

    Schema roundTrip = VortexSchemas.convert(VortexSchemas.toArrowSchema(icebergSchema));

    assertThat(roundTrip.findField("props").type().asMapType().isValueRequired()).isTrue();
  }

  @Test
  void unknownColumnsAreNotWrittenToTheFile() {
    // Unknown holds nothing but nulls, so the column is left out of the file and the reader fills
    // it back in. Struct children are dropped the same way.
    Schema icebergSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "u", Types.UnknownType.get()),
            optional(
                3,
                "nested",
                Types.StructType.of(
                    required(4, "kept", Types.IntegerType.get()),
                    optional(5, "dropped", Types.UnknownType.get()))));

    org.apache.arrow.vector.types.pojo.Schema arrow = VortexSchemas.toArrowSchema(icebergSchema);
    assertThat(arrow.getFields()).extracting(Field::getName).containsExactly("id", "nested");
    assertThat(arrow.findField("nested").getChildren())
        .extracting(Field::getName)
        .containsExactly("kept");

    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexArrow =
        VortexSchemas.toVortexArrowSchema(icebergSchema);
    assertThat(vortexArrow.getFields())
        .extracting(dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Field::getName)
        .containsExactly("id", "nested");
  }

  @Test
  void unknownInsideListsAndMapsIsAnArrowNullColumn() {
    // A list element or map value has no slot to drop, so it is stored as an Arrow null column
    // rather than omitted the way a struct field is.
    Schema listOfUnknown =
        new Schema(optional(1, "l", Types.ListType.ofOptional(2, Types.UnknownType.get())));
    assertThat(
            VortexSchemas.toArrowSchema(listOfUnknown)
                .findField("l")
                .getChildren()
                .get(0)
                .getType())
        .isEqualTo(ArrowType.Null.INSTANCE);

    Schema mapOfUnknown =
        new Schema(
            optional(
                1,
                "m",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.UnknownType.get())));
    Field entries = VortexSchemas.toArrowSchema(mapOfUnknown).findField("m").getChildren().get(0);
    assertThat(entries.getChildren().get(1).getType()).isEqualTo(ArrowType.Null.INSTANCE);
  }

  @Test
  void fixedIsRefusedWithANamedError() {
    // Vortex rejects Arrow FixedSizeBinary unless it carries the arrow.uuid extension, and the
    // rejection surfaces as an opaque native error when the writer is created. Refusing the schema
    // up front names the column and the reason instead.
    Schema icebergSchema = new Schema(required(1, "f", Types.FixedType.ofLength(7)));

    assertThatThrownBy(() -> VortexSchemas.toArrowSchema(icebergSchema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("FIXED column f")
        .hasMessageContaining("no fixed-width binary type");
    assertThatThrownBy(() -> VortexSchemas.toVortexArrowSchema(icebergSchema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("FIXED column f");
  }

  private static Field listField(String name, ArrowType listType, ArrowType elementType) {
    Field element = new Field("element", new FieldType(false, elementType, null), null);
    return new Field(name, new FieldType(true, listType, null), List.of(element));
  }

  private static Field variantField(String name, boolean nullable, List<Field> children) {
    return new Field(
        name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null, VARIANT_METADATA), children);
  }

  private static Field binaryField(String name, boolean nullable) {
    return new Field(name, new FieldType(nullable, ArrowType.Binary.INSTANCE, null), null);
  }

  private static void assertStructRoundTrip(Schema roundTrip) {
    // Names and types survive the round trip through Arrow (binding is by name).
    assertThat(roundTrip.findField("location").type()).isInstanceOf(Types.StructType.class);
    assertThat(roundTrip.findField("location").isOptional()).isTrue();
    assertThat(roundTrip.findType("location.lat")).isEqualTo(Types.DoubleType.get());
    assertThat(roundTrip.findField("location.lat").isRequired()).isTrue();
    assertThat(roundTrip.findType("nested.inner.x")).isEqualTo(Types.IntegerType.get());
    assertThat(roundTrip.findType("tags.element")).isEqualTo(Types.StringType.get());

    // Every field (including nested struct fields and the list element) gets a unique synthesized
    // id. indexById builds an id->field map and throws on a collision, so a successful index with
    // one entry per field proves the ids are valid.
    assertThat(TypeUtil.indexById(roundTrip.asStruct())).hasSize(9);
  }
}
