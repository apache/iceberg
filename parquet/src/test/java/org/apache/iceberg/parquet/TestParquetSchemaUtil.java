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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.apache.parquet.schema.Types.ListBuilder;
import org.apache.parquet.schema.Types.MapBuilder;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.junit.jupiter.api.Test;

public class TestParquetSchemaUtil {
  private static final Types.StructType SUPPORTED_PRIMITIVES =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts", Types.TimestampType.withZone()),
          required(110, "s", Types.StringType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
          );

  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          ImmutableList.of(
              org.apache.iceberg.types.Types.NestedField.of(
                  1, false, "a1", org.apache.iceberg.types.Types.StringType.get()),
              org.apache.iceberg.types.Types.NestedField.of(
                  2, false, "a2", org.apache.iceberg.types.Types.StringType.get()),
              org.apache.iceberg.types.Types.NestedField.of(
                  3,
                  false,
                  "a3",
                  org.apache.iceberg.types.Types.StructType.of(
                      org.apache.iceberg.types.Types.NestedField.of(
                          4, false, "b1", org.apache.iceberg.types.Types.StringType.get()),
                      org.apache.iceberg.types.Types.NestedField.of(
                          5, false, "b2", org.apache.iceberg.types.Types.StringType.get()))),
              org.apache.iceberg.types.Types.NestedField.of(
                  6,
                  false,
                  "a4",
                  org.apache.iceberg.types.Types.StructType.of(
                      org.apache.iceberg.types.Types.NestedField.of(
                          7,
                          false,
                          "b1",
                          org.apache.iceberg.types.Types.StructType.of(
                              org.apache.iceberg.types.Types.NestedField.of(
                                  8, false, "c1", org.apache.iceberg.types.Types.StringType.get()),
                              org.apache.iceberg.types.Types.NestedField.of(
                                  9,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.StringType.get()))),
                      org.apache.iceberg.types.Types.NestedField.of(
                          10,
                          false,
                          "b2",
                          org.apache.iceberg.types.Types.StructType.of(
                              org.apache.iceberg.types.Types.NestedField.of(
                                  11, false, "c1", org.apache.iceberg.types.Types.StringType.get()),
                              org.apache.iceberg.types.Types.NestedField.of(
                                  12,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.StringType.get()))))),
              org.apache.iceberg.types.Types.NestedField.of(
                  13,
                  false,
                  "a5",
                  org.apache.iceberg.types.Types.StructType.of(
                      org.apache.iceberg.types.Types.NestedField.of(
                          14,
                          false,
                          "b1",
                          org.apache.iceberg.types.Types.StructType.of(
                              org.apache.iceberg.types.Types.NestedField.of(
                                  15,
                                  false,
                                  "c1",
                                  org.apache.iceberg.types.Types.StructType.of(
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          16,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.StringType.get()),
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          17,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.StringType.get()))),
                              org.apache.iceberg.types.Types.NestedField.of(
                                  18,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.StructType.of(
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          19,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.StringType.get()),
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          20,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.StringType.get()))))),
                      org.apache.iceberg.types.Types.NestedField.of(
                          21,
                          false,
                          "b2",
                          org.apache.iceberg.types.Types.StructType.of(
                              org.apache.iceberg.types.Types.NestedField.of(
                                  22,
                                  false,
                                  "c1",
                                  org.apache.iceberg.types.Types.StructType.of(
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          23,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.StringType.get()),
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          24,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.StringType.get()))),
                              org.apache.iceberg.types.Types.NestedField.of(
                                  25,
                                  false,
                                  "c2",
                                  org.apache.iceberg.types.Types.StructType.of(
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          26,
                                          false,
                                          "d1",
                                          org.apache.iceberg.types.Types.StringType.get()),
                                      org.apache.iceberg.types.Types.NestedField.of(
                                          27,
                                          false,
                                          "d2",
                                          org.apache.iceberg.types.Types.StringType.get())))))))),
          ImmutableSet.of(1));

  @Test
  public void testAssignIdsByNameMapping() {
    Types.StructType structType =
        Types.StructType.of(
            required(0, "id", Types.LongType.get()),
            optional(
                1,
                "list_of_maps",
                Types.ListType.ofOptional(
                    2,
                    Types.MapType.ofOptional(3, 4, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            optional(
                5,
                "map_of_lists",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
            required(
                9,
                "list_of_lists",
                Types.ListType.ofOptional(10, Types.ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
            required(
                12,
                "map_of_maps",
                Types.MapType.ofOptional(
                    13,
                    14,
                    Types.StringType.get(),
                    Types.MapType.ofOptional(
                        15, 16, Types.StringType.get(), SUPPORTED_PRIMITIVES))),
            required(
                17,
                "list_of_struct_of_nested_types",
                Types.ListType.ofOptional(
                    19,
                    Types.StructType.of(
                        Types.NestedField.required(
                            20,
                            "m1",
                            Types.MapType.ofOptional(
                                21, 22, Types.StringType.get(), SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            23, "l1", Types.ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.required(
                            25, "l2", Types.ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
                        Types.NestedField.optional(
                            27,
                            "m2",
                            Types.MapType.ofOptional(
                                28, 29, Types.StringType.get(), SUPPORTED_PRIMITIVES))))));

    Schema schema =
        new Schema(
            TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
                .asStructType()
                .fields());
    NameMapping nameMapping = MappingUtil.create(schema);
    MessageType messageTypeWithIds = ParquetSchemaUtil.convert(schema, "parquet_type");
    MessageType messageTypeWithIdsFromNameMapping =
        ParquetSchemaUtil.applyNameMapping(RemoveIds.removeIds(messageTypeWithIds), nameMapping);

    assertThat(messageTypeWithIdsFromNameMapping).isEqualTo(messageTypeWithIds);
  }

  @Test
  public void testSchemaConversionWithoutAssigningIds() {
    MessageType messageType =
        new MessageType(
            "test",
            primitive(1, "int_col", PrimitiveTypeName.INT32, Repetition.REQUIRED),
            primitive(2, "double_col", PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL),
            primitive(null, "long_col", PrimitiveTypeName.INT64, Repetition.OPTIONAL),
            struct(
                3,
                "struct_col_1",
                Repetition.REQUIRED,
                primitive(4, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(null, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL),
                primitive(5, "n3", PrimitiveTypeName.INT64, Repetition.OPTIONAL)),
            struct(
                6,
                "struct_col_2",
                Repetition.OPTIONAL,
                primitive(null, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(null, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL),
                primitive(null, "n3", PrimitiveTypeName.INT64, Repetition.OPTIONAL)),
            list(
                null,
                "list_col_1",
                Repetition.REQUIRED,
                primitive(7, "i", PrimitiveTypeName.INT32, Repetition.OPTIONAL)),
            list(
                8,
                "list_col_2",
                Repetition.REQUIRED,
                primitive(null, "i", PrimitiveTypeName.INT32, Repetition.OPTIONAL)),
            list(
                9,
                "list_col_3",
                Repetition.OPTIONAL,
                struct(
                    null,
                    "s",
                    Repetition.REQUIRED,
                    primitive(10, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                    primitive(11, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL))),
            list(
                12,
                "list_col_4",
                Repetition.REQUIRED,
                struct(
                    13,
                    "s",
                    Repetition.REQUIRED,
                    primitive(null, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                    primitive(null, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL))),
            list(
                14,
                "list_col_5",
                Repetition.OPTIONAL,
                struct(
                    15,
                    "s",
                    Repetition.REQUIRED,
                    primitive(16, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                    primitive(17, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL))),
            map(
                null,
                "map_col_1",
                Repetition.REQUIRED,
                primitive(18, "k", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(19, "v", PrimitiveTypeName.INT32, Repetition.REQUIRED)),
            map(
                20,
                "map_col_2",
                Repetition.OPTIONAL,
                primitive(null, "k", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(21, "v", PrimitiveTypeName.INT32, Repetition.REQUIRED)),
            map(
                22,
                "map_col_3",
                Repetition.REQUIRED,
                primitive(null, "k", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(null, "v", PrimitiveTypeName.INT32, Repetition.REQUIRED)),
            map(
                23,
                "map_col_4",
                Repetition.OPTIONAL,
                primitive(24, "k", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                struct(
                    25,
                    "s",
                    Repetition.REQUIRED,
                    primitive(null, "n1", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                    primitive(26, "n2", PrimitiveTypeName.INT64, Repetition.OPTIONAL),
                    primitive(null, "n3", PrimitiveTypeName.INT64, Repetition.OPTIONAL))),
            map(
                27,
                "map_col_5",
                Repetition.REQUIRED,
                primitive(28, "k", PrimitiveTypeName.INT32, Repetition.REQUIRED),
                primitive(29, "v", PrimitiveTypeName.INT32, Repetition.REQUIRED)));

    Schema expectedSchema =
        new Schema(
            required(1, "int_col", Types.IntegerType.get()),
            optional(2, "double_col", Types.DoubleType.get()),
            required(
                3,
                "struct_col_1",
                Types.StructType.of(
                    required(4, "n1", Types.IntegerType.get()),
                    optional(5, "n3", Types.LongType.get()))),
            optional(
                14,
                "list_col_5",
                Types.ListType.ofRequired(
                    15,
                    Types.StructType.of(
                        required(16, "n1", Types.IntegerType.get()),
                        optional(17, "n2", Types.LongType.get())))),
            optional(
                23,
                "map_col_4",
                Types.MapType.ofRequired(
                    24,
                    25,
                    Types.IntegerType.get(),
                    Types.StructType.of(optional(26, "n2", Types.LongType.get())))),
            required(
                27,
                "map_col_5",
                Types.MapType.ofRequired(
                    28, 29, Types.IntegerType.get(), Types.IntegerType.get())));

    Schema actualSchema = ParquetSchemaUtil.convertAndPrune(messageType);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testSchemaConversionForHiveStyleLists() {
    String parquetSchemaString =
        "message spark_schema {\n"
            + "  optional group col1 (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional group array {\n"
            + "        required int32 col2;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}\n";
    MessageType messageType = MessageTypeParser.parseMessageType(parquetSchemaString);

    Schema expectedSchema =
        new Schema(
            optional(
                1,
                "col1",
                Types.ListType.ofOptional(
                    2, Types.StructType.of(required(3, "col2", Types.IntegerType.get())))));
    NameMapping nameMapping = MappingUtil.create(expectedSchema);
    MessageType messageTypeWithIds = ParquetSchemaUtil.applyNameMapping(messageType, nameMapping);
    Schema actualSchema = ParquetSchemaUtil.convertAndPrune(messageTypeWithIds);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testLegacyTwoLevelListTypeWithPrimitiveElement() {
    String parquetSchemaString =
        "message spark_schema {\n"
            + "  optional group arraybytes (LIST) {\n"
            + "    repeated binary array;\n"
            + "  }\n"
            + "}\n";
    MessageType messageType = MessageTypeParser.parseMessageType(parquetSchemaString);

    Schema expectedSchema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(1000, Types.BinaryType.get())));

    Schema actualSchema = ParquetSchemaUtil.convert(messageType);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testLegacyTwoLevelListTypeWithGroupTypeElementWithTwoFields() {
    String messageType =
        "message root {"
            + "  required group f0 {"
            + "    required group f00 (LIST) {"
            + "      repeated group element {"
            + "        required int32 f000;"
            + "        optional int64 f001;"
            + "      }"
            + "    }"
            + "  }"
            + "}";

    MessageType parquetScehma = MessageTypeParser.parseMessageType(messageType);
    Schema expectedSchema =
        new Schema(
            required(
                1,
                "f0",
                Types.StructType.of(
                    required(
                        1003,
                        "f00",
                        Types.ListType.ofRequired(
                            1002,
                            Types.StructType.of(
                                required(1000, "f000", Types.IntegerType.get()),
                                optional(1001, "f001", Types.LongType.get())))))));

    Schema actualSchema = ParquetSchemaUtil.convert(parquetScehma);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testLegacyTwoLevelListGenByParquetAvro() {
    String messageType =
        "message root {"
            + "  optional group my_list (LIST) {"
            + "    repeated group array {"
            + "      required binary str (UTF8);"
            + "    }"
            + "  }"
            + "}";

    MessageType parquetScehma = MessageTypeParser.parseMessageType(messageType);
    Schema expectedSchema =
        new Schema(
            optional(
                1,
                "my_list",
                Types.ListType.ofRequired(
                    1001, Types.StructType.of(required(1000, "str", Types.StringType.get())))));

    Schema actualSchema = ParquetSchemaUtil.convert(parquetScehma);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testLegacyTwoLevelListGenByParquetThrift() {
    String messageType =
        "message root {"
            + "  optional group my_list (LIST) {"
            + "    repeated group my_list_tuple {"
            + "      required binary str (UTF8);"
            + "    }"
            + "  }"
            + "}";

    MessageType parquetScehma = MessageTypeParser.parseMessageType(messageType);
    Schema expectedSchema =
        new Schema(
            optional(
                1,
                "my_list",
                Types.ListType.ofRequired(
                    1001, Types.StructType.of(required(1000, "str", Types.StringType.get())))));

    Schema actualSchema = ParquetSchemaUtil.convert(parquetScehma);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testLegacyTwoLevelListGenByParquetThrift1() {
    String messageType =
        "message root {"
            + "  optional group my_list (LIST) {"
            + "    repeated group my_list_tuple (LIST) {"
            + "      repeated int32 my_list_tuple_tuple;"
            + "    }"
            + "  }"
            + "}";

    MessageType parquetScehma = MessageTypeParser.parseMessageType(messageType);
    Schema expectedSchema =
        new Schema(
            optional(
                1,
                "my_list",
                Types.ListType.ofRequired(
                    1001, Types.ListType.ofRequired(1000, Types.IntegerType.get()))));

    Schema actualSchema = ParquetSchemaUtil.convert(parquetScehma);
    assertThat(actualSchema.asStruct())
        .as("Schema must match")
        .isEqualTo(expectedSchema.asStruct());
  }

  private Type primitive(
      Integer id, String name, PrimitiveTypeName typeName, Repetition repetition) {
    PrimitiveBuilder<PrimitiveType> builder =
        org.apache.parquet.schema.Types.primitive(typeName, repetition);
    if (id != null) {
      builder.id(id);
    }
    return builder.named(name);
  }

  private Type struct(Integer id, String name, Repetition repetition, Type... types) {
    GroupBuilder<GroupType> builder = org.apache.parquet.schema.Types.buildGroup(repetition);
    builder.addFields(types);
    if (id != null) {
      builder.id(id);
    }
    return builder.named(name);
  }

  private Type list(Integer id, String name, Repetition repetition, Type elementType) {
    ListBuilder<GroupType> builder = org.apache.parquet.schema.Types.list(repetition);
    builder.element(elementType);
    if (id != null) {
      builder.id(id);
    }
    return builder.named(name);
  }

  private Type map(Integer id, String name, Repetition repetition, Type keyType, Type valueType) {
    MapBuilder<GroupType> builder = org.apache.parquet.schema.Types.map(repetition);
    builder.key(keyType);
    builder.value(valueType);
    if (id != null) {
      builder.id(id);
    }
    return builder.named(name);
  }

  @Test
  public void testPruneColumns1() {
    MessageType messageType = ParquetSchemaUtil.convert(ICEBERG_SCHEMA, "schema");
    Type visit =
        TypeWithSchemaVisitor.visit(
            ICEBERG_SCHEMA.asStruct(), messageType, new PruneColumns(ImmutableSet.of(6, 7, 8)));
    String expectedType =
        "message schema {\n"
            + "  required group a4 = 6 {\n"
            + "    required group b1 = 7 {\n"
            + "      required binary c1 (STRING) = 8;\n"
            + "    }\n"
            + "  }\n"
            + "}";
    MessageType expectedSchema = MessageTypeParser.parseMessageType(expectedType);
    assertThat(visit).as("schema not match").isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneColumns2() {
    MessageType messageType = ParquetSchemaUtil.convert(ICEBERG_SCHEMA, "schema");
    Type visit =
        TypeWithSchemaVisitor.visit(
            ICEBERG_SCHEMA.asStruct(), messageType, new PruneColumns(ImmutableSet.of(6, 7, 8, 9)));
    String expectedType =
        "message schema {\n"
            + "  required group a4 = 6 {\n"
            + "    required group b1 = 7 {\n"
            + "      required binary c1 (STRING) = 8;\n"
            + "      required binary c2 (STRING) = 9;\n"
            + "    }\n"
            + "  }\n"
            + "}";
    MessageType expectedSchema = MessageTypeParser.parseMessageType(expectedType);
    assertThat(visit).as("schema not match").isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneColumns3() {
    MessageType messageType = ParquetSchemaUtil.convert(ICEBERG_SCHEMA, "schema");
    Type visit =
        TypeWithSchemaVisitor.visit(
            ICEBERG_SCHEMA.asStruct(), messageType, new PruneColumns(ImmutableSet.of(6, 7)));
    String expectedType =
        "message schema {\n"
            + "  required group a4 = 6 {\n"
            + "    required group b1 = 7 {\n"
            + "      required binary c1 (STRING) = 8;\n"
            + "      required binary c2 (STRING) = 9;\n"
            + "    }\n"
            + "  }\n"
            + "}";
    MessageType expectedSchema = MessageTypeParser.parseMessageType(expectedType);
    assertThat(visit).as("schema not match").isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneColumns4() {
    MessageType messageType = ParquetSchemaUtil.convert(ICEBERG_SCHEMA, "schema");
    Type visit =
        TypeWithSchemaVisitor.visit(
            ICEBERG_SCHEMA.asStruct(), messageType, new PruneColumns(ImmutableSet.of(13, 14, 15)));
    String expectedType =
        "message schema {\n"
            + "  required group a5 = 13 {\n"
            + "    required group b1 = 14 {\n"
            + "      required group c1 = 15 {\n"
            + "        required binary d1 (STRING) = 16;\n"
            + "        required binary d2 (STRING) = 17;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    MessageType expectedSchema = MessageTypeParser.parseMessageType(expectedType);
    assertThat(visit).as("schema not match").isEqualTo(expectedSchema);
  }

  @Test
  public void testPruneColumns5() {
    MessageType messageType = ParquetSchemaUtil.convert(ICEBERG_SCHEMA, "schema");
    Type visit =
        TypeWithSchemaVisitor.visit(
            ICEBERG_SCHEMA.asStruct(),
            messageType,
            new PruneColumns(ImmutableSet.of(13, 14, 15, 17)));
    String expectedType =
        "message schema {\n"
            + "  required group a5 = 13 {\n"
            + "    required group b1 = 14 {\n"
            + "      required group c1 = 15 {\n"
            + "        required binary d2 (STRING) = 17;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
    MessageType expectedSchema = MessageTypeParser.parseMessageType(expectedType);
    assertThat(visit).as("schema not match").isEqualTo(expectedSchema);
  }
}
