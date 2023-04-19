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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
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
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals(messageTypeWithIds, messageTypeWithIdsFromNameMapping);
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
    Assert.assertEquals("Schema must match", expectedSchema.asStruct(), actualSchema.asStruct());
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
}
