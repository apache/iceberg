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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestPruneColumns {

  private static final Schema MULTI_LEVEL_ICEBERG_SCHEMA =
      new Schema(
          ImmutableList.of(
              NestedField.of(1, false, "a1", org.apache.iceberg.types.Types.BinaryType.get()),
              NestedField.of(2, false, "a2", org.apache.iceberg.types.Types.BinaryType.get()),
              NestedField.of(
                  3,
                  false,
                  "a3",
                  StructType.of(
                      NestedField.of(
                          4, false, "b1", org.apache.iceberg.types.Types.BinaryType.get()),
                      NestedField.of(
                          5, false, "b2", org.apache.iceberg.types.Types.BinaryType.get()))),
              NestedField.of(
                  6,
                  false,
                  "a4",
                  StructType.of(
                      NestedField.of(
                          7,
                          false,
                          "b1",
                          StructType.of(
                              NestedField.of(
                                  8, false, "c1", org.apache.iceberg.types.Types.BinaryType.get()),
                              NestedField.of(
                                  9,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.BinaryType.get()))),
                      NestedField.of(
                          10,
                          false,
                          "b2",
                          StructType.of(
                              NestedField.of(
                                  11, false, "c1", org.apache.iceberg.types.Types.BinaryType.get()),
                              NestedField.of(
                                  12,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.BinaryType.get()))))),
              NestedField.of(
                  13,
                  false,
                  "a5",
                  StructType.of(
                      NestedField.of(
                          14,
                          false,
                          "b1",
                          StructType.of(
                              NestedField.of(
                                  15,
                                  false,
                                  "c1",
                                  StructType.of(
                                      NestedField.of(
                                          16,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.BinaryType.get()),
                                      NestedField.of(
                                          17,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.BinaryType.get()))),
                              NestedField.of(
                                  18,
                                  false,
                                  "c2",
                                  StructType.of(
                                      NestedField.of(
                                          19,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.BinaryType.get()),
                                      NestedField.of(
                                          20,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.BinaryType.get()))))),
                      NestedField.of(
                          21,
                          false,
                          "b2",
                          StructType.of(
                              NestedField.of(
                                  22,
                                  false,
                                  "c1",
                                  StructType.of(
                                      NestedField.of(
                                          23,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.BinaryType.get()),
                                      NestedField.of(
                                          24,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.BinaryType.get()))),
                              NestedField.of(
                                  25,
                                  false,
                                  "c2",
                                  StructType.of(
                                      NestedField.of(
                                          26,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.BinaryType.get()),
                                      NestedField.of(
                                          27,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.BinaryType.get())))))))));

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
  public void testTwoLevelOneChildPruneColumns() {
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .id(8)
                                    .named("c1"))
                            .id(7)
                            .named("b1"))
                    .id(6)
                    .named("a4"))
            .named("schema");
    Type actual =
        TypeWithSchemaVisitor.visit(
            MULTI_LEVEL_ICEBERG_SCHEMA.asStruct(),
            ParquetSchemaUtil.convert(MULTI_LEVEL_ICEBERG_SCHEMA, "schema"),
            new PruneColumns(ImmutableSet.of(6, 7, 8)));
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testThreeLevelFullChildPruneColumns() {
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .id(8)
                                    .named("c1"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .id(9)
                                    .named("c2"))
                            .id(7)
                            .named("b1"))
                    .id(6)
                    .named("a4"))
            .named("schema");
    Type actual =
        TypeWithSchemaVisitor.visit(
            MULTI_LEVEL_ICEBERG_SCHEMA.asStruct(),
            ParquetSchemaUtil.convert(MULTI_LEVEL_ICEBERG_SCHEMA, "schema"),
            new PruneColumns(ImmutableSet.of(6, 7, 8, 9)));
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testThreeLevelEmptyChildPruneColumns() {
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .id(8)
                                    .named("c1"))
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .id(9)
                                    .named("c2"))
                            .id(7)
                            .named("b1"))
                    .id(6)
                    .named("a4"))
            .named("schema");
    Type actual =
        TypeWithSchemaVisitor.visit(
            MULTI_LEVEL_ICEBERG_SCHEMA.asStruct(),
            ParquetSchemaUtil.convert(MULTI_LEVEL_ICEBERG_SCHEMA, "schema"),
            new PruneColumns(ImmutableSet.of(6, 7)));
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testFourLevelFullChildPruneColumns() {
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.buildGroup(Type.Repetition.REQUIRED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                            .id(16)
                                            .named("d1"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                            .id(17)
                                            .named("d2"))
                                    .id(15)
                                    .named("c1"))
                            .id(14)
                            .named("b1"))
                    .id(13)
                    .named("a5"))
            .named("schema");
    Type actual =
        TypeWithSchemaVisitor.visit(
            MULTI_LEVEL_ICEBERG_SCHEMA.asStruct(),
            ParquetSchemaUtil.convert(MULTI_LEVEL_ICEBERG_SCHEMA, "schema"),
            new PruneColumns(ImmutableSet.of(13, 14, 15)));
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testFourLevelOneChildPruneColumns() {
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.REQUIRED)
                    .addField(
                        Types.buildGroup(Type.Repetition.REQUIRED)
                            .addField(
                                Types.buildGroup(Type.Repetition.REQUIRED)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                            .id(17)
                                            .named("d2"))
                                    .id(15)
                                    .named("c1"))
                            .id(14)
                            .named("b1"))
                    .id(13)
                    .named("a5"))
            .named("schema");
    Type actual =
        TypeWithSchemaVisitor.visit(
            MULTI_LEVEL_ICEBERG_SCHEMA.asStruct(),
            ParquetSchemaUtil.convert(MULTI_LEVEL_ICEBERG_SCHEMA, "schema"),
            new PruneColumns(ImmutableSet.of(13, 14, 15, 17)));
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }
}
