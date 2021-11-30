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

import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestParquetTypeVisitor {
  MessageType mapSchema = Types.buildMessage()
      .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
          .addField(Types.buildGroup(Type.Repetition.REPEATED)
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                  .as(LogicalTypeAnnotation.stringType())
                  .id(2)
                  .named("key"))
              .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(4)
                      .named("x"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(5)
                      .named("y"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(6)
                      .named("z"))
                  .id(3)
                  .named("value"))
              .named("custom_key_value_name"))
          .as(LogicalTypeAnnotation.mapType())
          .id(1)
          .named("m"))
      .named("table");

  MessageType mapSchemaWithoutIds = Types.buildMessage()
      .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
          .addField(Types.buildGroup(Type.Repetition.REPEATED)
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                  .as(LogicalTypeAnnotation.stringType())
                  .named("key"))
              .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("x"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("y"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("z"))
                  .named("value"))
              .named("custom_key_value_name"))
          .as(LogicalTypeAnnotation.mapType())
          .named("m"))
      .named("table");

  MessageType listSchema = Types.buildMessage()
      .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
          .addField(Types.buildGroup(Type.Repetition.REPEATED)
              .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(4)
                      .named("x"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(5)
                      .named("y"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .id(6)
                      .named("z"))
                  .id(3)
                  .named("element"))
              .named("custom_repeated_name"))
          .as(LogicalTypeAnnotation.listType())
          .id(1)
          .named("m"))
      .named("table");

  MessageType listSchemaWithoutIds = Types.buildMessage()
      .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
          .addField(Types.buildGroup(Type.Repetition.REPEATED)
              .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("x"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("y"))
                  .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                      .named("z"))
                  .named("element"))
              .named("custom_repeated_name"))
          .as(LogicalTypeAnnotation.listType())
          .named("m"))
      .named("table");

  @Test
  public void testPruneColumnsMap() {
    // project map.value.x and map.value.y
    Schema projection = new Schema(
        NestedField.optional(1, "m", MapType.ofOptional(2, 3,
            StringType.get(),
            StructType.of(
                NestedField.required(4, "x", DoubleType.get()),
                NestedField.required(5, "y", DoubleType.get())
            )
        ))
    );

    MessageType expected = Types.buildMessage()
        .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
            .addField(Types.buildGroup(Type.Repetition.REPEATED)
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                    .as(LogicalTypeAnnotation.stringType())
                    .id(2)
                    .named("key"))
                .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                        .id(4)
                        .named("x"))
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                        .id(5)
                        .named("y"))
                    .id(3)
                    .named("value"))
                .named("custom_key_value_name"))
            .as(LogicalTypeAnnotation.mapType())
            .id(1)
            .named("m"))
        .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(mapSchema, projection);
    Assert.assertEquals("Pruned schema should not rename repeated struct", expected, actual);
  }

  @Test
  public void testPruneColumnsList() {
    // project list.element.x and list.element.y
    Schema projection = new Schema(
        NestedField.optional(1, "m", ListType.ofOptional(
            3,
            StructType.of(
                NestedField.required(4, "x", DoubleType.get()),
                NestedField.required(5, "y", DoubleType.get())
            )
        ))
    );

    MessageType expected = Types.buildMessage()
        .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
            .addField(Types.buildGroup(Type.Repetition.REPEATED)
                .addField(Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                        .id(4)
                        .named("x"))
                    .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                        .id(5)
                        .named("y"))
                    .id(3)
                    .named("element"))
                .named("custom_repeated_name"))
            .as(LogicalTypeAnnotation.listType())
            .id(1)
            .named("m"))
        .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(listSchema, projection);
    Assert.assertEquals("Pruned schema should not rename repeated struct", expected, actual);
  }

  @Test
  public void testApplyNameMappingMap() {
    Schema icebergSchema = new Schema(
        NestedField.optional(1, "m", MapType.ofOptional(2, 3,
            StringType.get(),
            StructType.of(
                NestedField.required(4, "x", DoubleType.get()),
                NestedField.required(5, "y", DoubleType.get()),
                NestedField.required(6, "z", DoubleType.get())
            )
        ))
    );

    NameMapping nameMapping = MappingUtil.create(icebergSchema);
    MessageType actual = ParquetSchemaUtil.applyNameMapping(mapSchemaWithoutIds, nameMapping);
    Assert.assertEquals("ApplyNameMapping should not rename repeated struct", mapSchema, actual);
  }

  @Test
  public void testApplyNameMappingList() {
    Schema icebergSchema = new Schema(
        NestedField.optional(1, "m",
            ListType.ofOptional(
                3,
                StructType.of(
                    NestedField.required(4, "x", DoubleType.get()),
                    NestedField.required(5, "y", DoubleType.get()),
                    NestedField.required(6, "z", DoubleType.get())
                )
            )
        )
    );

    NameMapping nameMapping = MappingUtil.create(icebergSchema);
    MessageType actual = ParquetSchemaUtil.applyNameMapping(listSchemaWithoutIds, nameMapping);
    Assert.assertEquals("ApplyNameMapping should not rename repeated struct", listSchema, actual);
  }

  @Test
  public void testRemoveIdsMap() {
    MessageType actual = RemoveIds.removeIds(mapSchema);
    Assert.assertEquals("RemoveIds should not rename repeated struct", mapSchemaWithoutIds, actual);
  }

  @Test
  public void testRemoveIdsList() {
    MessageType actual = RemoveIds.removeIds(listSchema);
    Assert.assertEquals("RemoveIds should not rename repeated struct", listSchemaWithoutIds, actual);
  }
}
