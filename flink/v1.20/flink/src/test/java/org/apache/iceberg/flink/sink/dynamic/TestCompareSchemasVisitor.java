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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

class TestCompareSchemasVisitor {

  private static final boolean DROP_COLUMNS = true;
  private static final boolean PRESERVE_COLUMNS = false;

  @Test
  void testSchema() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get(), "comment"),
                    optional(2, "data", StringType.get()),
                    optional(3, "extra", StringType.get())),
                new Schema(
                    optional(1, "id", IntegerType.get(), "comment"),
                    optional(2, "data", StringType.get()),
                    optional(3, "extra", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testSchemaDifferentId() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "data", StringType.get()),
                    optional(2, "extra", StringType.get())),
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(2, "data", StringType.get()),
                    optional(3, "extra", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testSchemaDifferent() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "data", StringType.get()),
                    optional(2, "extra", StringType.get())),
                new Schema(
                    optional(0, "id", IntegerType.get()), optional(1, "data", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testSchemaWithMoreColumns() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(0, "id", IntegerType.get()), optional(1, "data", StringType.get())),
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "data", StringType.get()),
                    optional(2, "extra", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
  }

  @Test
  void testDifferentType() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", LongType.get()), optional(2, "extra", StringType.get())),
                new Schema(
                    optional(1, "id", IntegerType.get()), optional(2, "extra", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testCompatibleType() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get()), optional(2, "extra", StringType.get())),
                new Schema(
                    optional(1, "id", LongType.get()), optional(2, "extra", StringType.get()))))
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
  }

  @Test
  void testRequiredChangeForMatchingField() {
    Schema dataSchema =
        new Schema(optional(1, "id", IntegerType.get()), optional(2, "extra", StringType.get()));
    Schema tableSchema =
        new Schema(required(1, "id", IntegerType.get()), optional(2, "extra", StringType.get()));
    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
    assertThat(CompareSchemasVisitor.visit(tableSchema, dataSchema))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testRequiredChangeForNonMatchingField() {
    Schema dataSchema = new Schema(optional(1, "id", IntegerType.get()));
    Schema tableSchema =
        new Schema(optional(1, "id", IntegerType.get()), required(2, "extra", StringType.get()));
    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
    assertThat(CompareSchemasVisitor.visit(tableSchema, dataSchema))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testNoRequiredChangeForNonMatchingField() {
    Schema dataSchema = new Schema(required(1, "id", IntegerType.get()));
    Schema tableSchema =
        new Schema(required(1, "id", IntegerType.get()), optional(2, "extra", StringType.get()));
    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema))
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
  }

  @Test
  void testStructDifferentId() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(2, "struct1", StructType.of(optional(3, "extra", IntegerType.get())))),
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(
                        1, "struct1", StructType.of(optional(2, "extra", IntegerType.get()))))))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testStructChanged() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "struct1", StructType.of(optional(2, "extra", LongType.get())))),
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(
                        2, "struct1", StructType.of(optional(3, "extra", IntegerType.get()))))))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testMapDifferentId() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(
                        2, "map1", MapType.ofOptional(3, 4, IntegerType.get(), StringType.get()))),
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(
                        1, "map1", MapType.ofOptional(2, 3, IntegerType.get(), StringType.get())))))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testMapChanged() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(
                        2, "map1", MapType.ofOptional(3, 4, LongType.get(), StringType.get()))),
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(
                        2, "map1", MapType.ofOptional(3, 4, IntegerType.get(), StringType.get())))))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testListDifferentId() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(2, "list1", ListType.ofOptional(3, IntegerType.get()))),
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "list1", ListType.ofOptional(2, IntegerType.get())))))
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testListChanged() {
    assertThat(
            CompareSchemasVisitor.visit(
                new Schema(
                    optional(0, "id", IntegerType.get()),
                    optional(1, "list1", ListType.ofOptional(2, LongType.get()))),
                new Schema(
                    optional(1, "id", IntegerType.get()),
                    optional(2, "list1", ListType.ofOptional(3, IntegerType.get())))))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testDropUnusedColumnsEnabled() {
    Schema dataSchema = new Schema(optional(1, "id", IntegerType.get()));
    Schema tableSchema =
        new Schema(
            optional(1, "id", IntegerType.get()),
            optional(2, "data", StringType.get()),
            optional(3, "extra", StringType.get()));

    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema, true, DROP_COLUMNS))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testDropUnusedColumnsWithRequiredField() {
    Schema dataSchema = new Schema(optional(1, "id", IntegerType.get()));
    Schema tableSchema =
        new Schema(optional(1, "id", IntegerType.get()), required(2, "data", StringType.get()));

    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema, true, DROP_COLUMNS))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testDropUnusedColumnsWhenInputHasMoreFields() {
    Schema dataSchema =
        new Schema(
            optional(1, "id", IntegerType.get()),
            optional(2, "data", StringType.get()),
            optional(3, "extra", StringType.get()));
    Schema tableSchema = new Schema(optional(1, "id", IntegerType.get()));

    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema, true, DROP_COLUMNS))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);
  }

  @Test
  void testDropUnusedColumnsInNestedStruct() {
    Schema dataSchema =
        new Schema(
            optional(1, "id", IntegerType.get()),
            optional(2, "struct1", StructType.of(optional(3, "field1", StringType.get()))));
    Schema tableSchema =
        new Schema(
            optional(1, "id", IntegerType.get()),
            optional(
                2,
                "struct1",
                StructType.of(
                    optional(3, "field1", StringType.get()),
                    optional(4, "field2", IntegerType.get()))));

    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema, true, DROP_COLUMNS))
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

    assertThat(CompareSchemasVisitor.visit(dataSchema, tableSchema, true, PRESERVE_COLUMNS))
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
  }
}
