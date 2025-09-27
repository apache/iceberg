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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDeleteFilterProjection {

  private static final Schema NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          optional(
              3,
              "structData",
              Types.StructType.of(
                  optional(100, "nestedField", Types.StringType.get()),
                  optional(101, "anotherField", Types.IntegerType.get()))));

  private static final Schema DEEP_NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "level1",
              Types.StructType.of(
                  optional(
                      200,
                      "level2",
                      Types.StructType.of(optional(300, "level3", Types.StringType.get()))))));

  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "listField",
              Types.ListType.ofOptional(
                  100, Types.StructType.of(optional(101, "listElement", Types.StringType.get())))),
          optional(
              3,
              "mapField",
              Types.MapType.ofOptional(
                  200,
                  201,
                  Types.StringType.get(),
                  Types.StructType.of(optional(202, "mapValue", Types.IntegerType.get())))));

  private static Schema invokeFileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes)
      throws Exception {
    Method method =
        DeleteFilter.class.getDeclaredMethod(
            "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    method.setAccessible(true);
    return (Schema) method.invoke(null, tableSchema, requestedSchema, posDeletes, eqDeletes, false);
  }

  private static DeleteFile mockEqDelete(Integer... fieldIds) {
    DeleteFile delete = Mockito.mock(DeleteFile.class);
    Mockito.when(delete.equalityFieldIds()).thenReturn(ImmutableList.copyOf(fieldIds));
    return delete;
  }

  @Test
  public void testProjectionWithSingleNestedField() throws Exception {
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(100)));

    assertThat(projectedSchema.findField(100))
        .as("Should include nested field from equality delete")
        .isNotNull();
    assertThat(projectedSchema.findField(100).name()).isEqualTo("nestedField");
    assertThat(projectedSchema.findField(1)).as("Should include requested field id").isNotNull();
    assertThat(projectedSchema.findField(2)).as("Should include requested field data").isNotNull();
  }

  @Test
  public void testProjectionWithMultipleNestedFields() throws Exception {
    Schema requestedSchema = NESTED_SCHEMA.select("id");
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(100, 101)));

    assertThat(projectedSchema.findField(100)).as("Should include first nested field").isNotNull();
    assertThat(projectedSchema.findField(101)).as("Should include second nested field").isNotNull();
  }

  @Test
  public void testProjectionWithDeepNesting() throws Exception {
    Schema requestedSchema = DEEP_NESTED_SCHEMA.select("id");
    Schema projectedSchema =
        invokeFileProjection(
            DEEP_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(300)));

    assertThat(projectedSchema.findField(300)).as("Should find deeply nested field").isNotNull();
    assertThat(projectedSchema.findField(300).name()).isEqualTo("level3");
  }

  @Test
  public void testProjectionWithListElementField() throws Exception {
    Schema requestedSchema = COMPLEX_SCHEMA.select("id");
    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(101)));

    assertThat(projectedSchema.findField(101)).as("Should include list element field").isNotNull();
    assertThat(projectedSchema.findField(101).name()).isEqualTo("listElement");
  }

  @Test
  public void testProjectionWithMapValueField() throws Exception {
    Schema requestedSchema = COMPLEX_SCHEMA.select("id");
    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(202)));

    assertThat(projectedSchema.findField(202)).as("Should include map value field").isNotNull();
    assertThat(projectedSchema.findField(202).name()).isEqualTo("mapValue");
  }

  @Test
  public void testProjectionWithMixedFields() throws Exception {
    Schema requestedSchema = COMPLEX_SCHEMA.select("id");
    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(101, 202)));

    assertThat(projectedSchema.findField(101)).as("Should include list field").isNotNull();
    assertThat(projectedSchema.findField(202)).as("Should include map field").isNotNull();
  }

  @Test
  public void testProjectionWithNoDeleteFiles() throws Exception {
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of());

    assertThat(projectedSchema.columns()).hasSize(2);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
  }

  @Test
  public void testProjectionWithAllFieldsRequested() throws Exception {
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA, NESTED_SCHEMA, ImmutableList.of(), ImmutableList.of(mockEqDelete(100)));

    assertThat(projectedSchema.columns()).hasSize(3);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
    assertThat(projectedSchema.findField(3)).isNotNull();
  }

  @Test
  public void testProjectionWithMetadataColumns() throws Exception {
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");

    // Invoke with position deletes (requires ROW_POSITION metadata column)
    Method method =
        DeleteFilter.class.getDeclaredMethod(
            "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    method.setAccessible(true);

    DeleteFile posDelete = Mockito.mock(DeleteFile.class);
    Schema projectedSchema =
        (Schema)
            method.invoke(
                null,
                NESTED_SCHEMA,
                requestedSchema,
                ImmutableList.of(posDelete),
                ImmutableList.of(),
                true);

    assertThat(projectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()))
        .as("Should include ROW_POSITION metadata column for position deletes")
        .isNotNull();
    assertThat(projectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()).name())
        .isEqualTo("_pos");
    assertThat(projectedSchema.findField(1)).as("Should include requested field id").isNotNull();
    assertThat(projectedSchema.findField(2)).as("Should include requested field data").isNotNull();
  }
}
