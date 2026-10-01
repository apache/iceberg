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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestPruneColumns {
  @Test
  public void testMapKeyValueName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testListElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                ListType.ofOptional(
                    3,
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testStructElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(3)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(7)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(8)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(9)
                            .named("z"))
                    .id(6)
                    .named("struct_name_2"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                2,
                "struct_name_1",
                StructType.of(
                    NestedField.required(4, "y", DoubleType.get()),
                    NestedField.required(5, "z", DoubleType.get()))),
            NestedField.optional(6, "struct_name_2", StructType.of()));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(Types.buildGroup(Type.Repetition.OPTIONAL).id(6).named("struct_name_2"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testVariant() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .addField(buildVariantType(3, "variant_2"))
            .named("table");

    Schema projection =
        new Schema(
            ImmutableList.of(
                NestedField.required(1, "id", IntegerType.get()),
                NestedField.required(2, "variant_1", VariantType.get())));
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  private static MessageType deepNestedFileSchema() {
    return Types.buildMessage()
        .addField(
            Types.primitive(PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).id(1).named("id"))
        .addField(
            Types.buildGroup(Type.Repetition.REQUIRED)
                .addField(
                    Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                        .as(LogicalTypeAnnotation.stringType())
                        .id(3)
                        .named("own"))
                .addField(
                    Types.buildGroup(Type.Repetition.REQUIRED)
                        .addField(
                            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                .as(LogicalTypeAnnotation.stringType())
                                .id(5)
                                .named("x"))
                        .addField(
                            Types.buildGroup(Type.Repetition.REQUIRED)
                                .addField(
                                    Types.primitive(
                                            PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                                        .id(7)
                                        .named("leaf"))
                                .addField(
                                    Types.primitive(
                                            PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .id(8)
                                        .named("big"))
                                .id(6)
                                .named("l3"))
                        .id(4)
                        .named("l2"))
                .id(2)
                .named("l1"))
        .named("table");
  }

  @Test
  public void testDeeplyNestedStructProjection() {
    MessageType fileSchema = deepNestedFileSchema();

    // project the deepest leaf only: intermediate structs must not widen back to their full type
    Schema leafProjection =
        new Schema(
            NestedField.required(
                2,
                "l1",
                StructType.of(
                    NestedField.required(
                        4,
                        "l2",
                        StructType.of(
                            NestedField.required(
                                6,
                                "l3",
                                StructType.of(
                                    NestedField.required(7, "leaf", LongType.get()))))))));

    MessageType leafExpected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.buildGroup(Type.Repetition.REQUIRED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                                            .id(7)
                                            .named("leaf"))
                                    .id(6)
                                    .named("l3"))
                            .id(4)
                            .named("l2"))
                    .id(2)
                    .named("l1"))
            .named("table");

    MessageType leafActual = ParquetSchemaUtil.pruneColumns(fileSchema, leafProjection);
    assertThat(leafActual)
        .as("Deep projection should not widen intermediate structs")
        .isEqualTo(leafExpected);
  }

  @Test
  public void testDeeplyNestedStructWhole() {
    MessageType fileSchema = deepNestedFileSchema();

    // project a nested struct itself: the whole struct is still read
    Schema structProjection =
        new Schema(
            NestedField.required(
                2,
                "l1",
                StructType.of(
                    NestedField.required(
                        4,
                        "l2",
                        StructType.of(
                            NestedField.required(
                                6,
                                "l3",
                                StructType.of(
                                    NestedField.required(7, "leaf", LongType.get()),
                                    NestedField.required(8, "big", StringType.get()))),
                            NestedField.required(5, "x", StringType.get()))))));

    MessageType structExpected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(5)
                                    .named("x"))
                            .addField(
                                Types.buildGroup(Type.Repetition.REQUIRED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                                            .id(7)
                                            .named("leaf"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                            .as(LogicalTypeAnnotation.stringType())
                                            .id(8)
                                            .named("big"))
                                    .id(6)
                                    .named("l3"))
                            .id(4)
                            .named("l2"))
                    .id(2)
                    .named("l1"))
            .named("table");

    MessageType structActual = ParquetSchemaUtil.pruneColumns(fileSchema, structProjection);
    assertThat(structActual)
        .as("Projecting a nested struct keeps the whole struct")
        .isEqualTo(structExpected);
  }

  @Test
  public void testDeeplyNestedStructMixed() {
    MessageType fileSchema = deepNestedFileSchema();

    // project two leaves from different branches: both are kept, siblings are dropped
    Schema mixedProjection =
        new Schema(
            NestedField.required(
                2,
                "l1",
                StructType.of(
                    NestedField.required(3, "own", StringType.get()),
                    NestedField.required(
                        4,
                        "l2",
                        StructType.of(
                            NestedField.required(
                                6,
                                "l3",
                                StructType.of(
                                    NestedField.required(7, "leaf", LongType.get()))))))));

    MessageType mixedExpected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                            .as(LogicalTypeAnnotation.stringType())
                            .id(3)
                            .named("own"))
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.buildGroup(Type.Repetition.REQUIRED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
                                            .id(7)
                                            .named("leaf"))
                                    .id(6)
                                    .named("l3"))
                            .id(4)
                            .named("l2"))
                    .id(2)
                    .named("l1"))
            .named("table");

    MessageType mixedActual = ParquetSchemaUtil.pruneColumns(fileSchema, mixedProjection);
    assertThat(mixedActual)
        .as("Mixed projection keeps only the selected leaves")
        .isEqualTo(mixedExpected);
  }

  @Test
  public void testDeeplyNestedStructPartiallyProjectedBeforeFullyProjected() {
    // a fully projected sibling (name) must not undo pruning of an earlier sibling (contact)
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(3)
                                    .named("email"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(4)
                                    .named("phone"))
                            .id(2)
                            .named("contact"))
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(6)
                                    .named("first"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(7)
                                    .named("last"))
                            .id(5)
                            .named("name"))
                    .id(1)
                    .named("event"))
            .named("table");

    Schema projection =
        new Schema(
            NestedField.required(
                1,
                "event",
                StructType.of(
                    NestedField.required(
                        2,
                        "contact",
                        StructType.of(NestedField.required(3, "email", StringType.get()))),
                    NestedField.required(
                        5,
                        "name",
                        StructType.of(
                            NestedField.required(6, "first", StringType.get()),
                            NestedField.required(7, "last", StringType.get()))))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(3)
                                    .named("email"))
                            .id(2)
                            .named("contact"))
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(6)
                                    .named("first"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(7)
                                    .named("last"))
                            .id(5)
                            .named("name"))
                    .id(1)
                    .named("event"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual)
        .as("Inner pruning of an earlier sibling must survive a fully-projected later sibling")
        .isEqualTo(expected);
  }

  @Test
  public void testDeeplyNestedStructInsideList() {
    Schema schema =
        new Schema(
            NestedField.optional(
                1,
                "events",
                ListType.ofOptional(
                    2,
                    StructType.of(
                        NestedField.optional(3, "id", LongType.get()),
                        NestedField.optional(
                            4,
                            "payload",
                            StructType.of(
                                NestedField.optional(5, "a", StringType.get()),
                                NestedField.optional(6, "b", StringType.get())))))));
    MessageType fileSchema = ParquetSchemaUtil.convert(schema, "table");

    // project events.payload.a: the element's id and payload.b are dropped
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "events",
                ListType.ofOptional(
                    2,
                    StructType.of(
                        NestedField.optional(
                            4,
                            "payload",
                            StructType.of(NestedField.optional(5, "a", StringType.get())))))));
    MessageType expected = ParquetSchemaUtil.convert(projection, "table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("List element must prune to the projected subtree").isEqualTo(expected);

    // projecting the list itself keeps the whole element
    MessageType wholeActual = ParquetSchemaUtil.pruneColumns(fileSchema, schema);
    assertThat(wholeActual).as("Whole-list projection is unchanged").isEqualTo(fileSchema);
  }

  @Test
  public void testDeeplyNestedStructInsideMap() {
    Schema schema =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(
                        NestedField.optional(4, "va", StringType.get()),
                        NestedField.optional(5, "vb", StringType.get())))));
    MessageType fileSchema = ParquetSchemaUtil.convert(schema, "table");

    // project value.va inside the map value: value.vb must be dropped
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(NestedField.optional(4, "va", StringType.get())))));
    MessageType expected = ParquetSchemaUtil.convert(projection, "table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Map value must prune to the projected subtree").isEqualTo(expected);

    // projecting the map itself keeps the whole value
    MessageType wholeActual = ParquetSchemaUtil.pruneColumns(fileSchema, schema);
    assertThat(wholeActual).as("Whole-map projection is unchanged").isEqualTo(fileSchema);
  }

  private static Type buildVariantType(int id, String name) {
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("value"))
        .id(id)
        .named(name);
  }
}
