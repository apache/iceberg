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
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDeleteFilterProjection {

  // Schema with nested fields similar to the one in DeleteReadTests
  private static final Schema NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          optional(
              3,
              "structData",
              Types.StructType.of(
                  optional(100, "structInnerData", Types.StringType.get()),
                  optional(101, "anotherInnerField", Types.IntegerType.get()))));

  // Schema with multiple layers of nesting for comprehensive testing
  private static final Schema COMPLEX_NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          // Multi-layer struct nesting: struct -> struct -> struct
          optional(
              3,
              "level1",
              Types.StructType.of(
                  optional(
                      200,
                      "level2",
                      Types.StructType.of(
                          optional(
                              300,
                              "level3",
                              Types.StructType.of(
                                  optional(400, "deepField", Types.StringType.get()),
                                  optional(410, "anotherDeepField", Types.IntegerType.get()))))))),
          // List with nested struct elements
          optional(
              4,
              "listOfStructs",
              Types.ListType.ofOptional(
                  500,
                  Types.StructType.of(
                      optional(501, "listElementField", Types.StringType.get()),
                      optional(502, "listElementInt", Types.IntegerType.get())))),
          // Map with nested struct values
          optional(
              5,
              "mapWithStructValues",
              Types.MapType.ofOptional(
                  600,
                  601,
                  Types.StringType.get(),
                  Types.StructType.of(
                      optional(602, "mapValueField", Types.StringType.get()),
                      optional(603, "mapValueInt", Types.IntegerType.get())))),
          // Map with nested struct keys (complex case)
          optional(
              6,
              "mapWithStructKeys",
              Types.MapType.ofOptional(
                  700,
                  701,
                  Types.StructType.of(optional(702, "mapKeyField", Types.StringType.get())),
                  Types.StringType.get())),
          // Complex combination: List of Maps with struct values
          optional(
              7,
              "listOfMapsWithStructs",
              Types.ListType.ofOptional(
                  800,
                  Types.MapType.ofOptional(
                      801,
                      802,
                      Types.StringType.get(),
                      Types.StructType.of(
                          optional(803, "complexNestedField", Types.StringType.get()))))));

  // Simple schema for regression testing (no nesting)
  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "name", Types.StringType.get()),
          optional(3, "age", Types.IntegerType.get()),
          optional(4, "active", Types.BooleanType.get()));

  private Method getFileProjectionMethod() throws Exception {
    Method method =
        DeleteFilter.class.getDeclaredMethod(
            "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    method.setAccessible(true);
    return method;
  }

  private Schema invokeFileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes)
      throws Exception {
    return (Schema)
        getFileProjectionMethod()
            .invoke(null, tableSchema, requestedSchema, posDeletes, eqDeletes, false);
  }

  @Test
  public void testFileProjectionWithNestedFields() throws Exception {
    // This test verifies that nested fields are properly added with their full path preserved
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");

    // Create a mock delete file that references the nested field (structInnerData)
    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(100)); // nested field ID

    List<DeleteFile> eqDeletes = ImmutableList.of(mockEqDelete);
    List<DeleteFile> posDeletes = ImmutableList.of();

    Schema projectedSchema =
        invokeFileProjection(NESTED_SCHEMA, requestedSchema, posDeletes, eqDeletes);

    // Verify the projection includes the original requested fields
    assertThat(projectedSchema.findField(1))
        .as("Projected schema should include id field")
        .isNotNull();

    assertThat(projectedSchema.findField(2))
        .as("Projected schema should include data field")
        .isNotNull();

    // Verify the parent struct field exists at top level (not promoted)
    Types.NestedField structDataField = projectedSchema.asStruct().field("structData");
    assertThat(structDataField)
        .as("Parent struct 'structData' should exist at top level")
        .isNotNull();
    assertThat(structDataField.fieldId()).isEqualTo(3);
    assertThat(structDataField.type().isStructType()).isTrue();

    // Verify the nested field is within the parent struct, not promoted to top level
    Types.StructType structType = structDataField.type().asStructType();
    Types.NestedField nestedField = structType.field("structInnerData");
    assertThat(nestedField)
        .as("Nested field should be within parent struct, not at top level")
        .isNotNull();
    assertThat(nestedField.fieldId()).isEqualTo(100);
    assertThat(nestedField.name()).isEqualTo("structInnerData");

    // Also verify via findField
    assertThat(projectedSchema.findField(100))
        .as("Projected schema should include nested field via findField")
        .isNotNull();

    // Verify structInnerData is NOT promoted to top level
    Types.NestedField promotedField = projectedSchema.asStruct().field("structInnerData");
    assertThat(promotedField).as("Nested field should NOT be promoted to top level").isNull();
  }

  @Test
  public void testFileProjectionWithMultipleNestedFields() throws Exception {
    Schema requestedSchema = NESTED_SCHEMA.select("id");

    // Create a mock delete file that references multiple nested fields
    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(100, 101)); // both nested fields

    List<DeleteFile> eqDeletes = ImmutableList.of(mockEqDelete);
    List<DeleteFile> posDeletes = ImmutableList.of();

    // Use reflection to access the private fileProjection method
    Method fileProjectionMethod =
        DeleteFilter.class.getDeclaredMethod(
            "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    fileProjectionMethod.setAccessible(true);

    // This should not throw an exception
    Schema projectedSchema =
        (Schema)
            fileProjectionMethod.invoke(
                null, NESTED_SCHEMA, requestedSchema, posDeletes, eqDeletes, false);

    // Verify both nested fields are included
    assertThat(projectedSchema.findField(100))
        .as("Projected schema should include first nested field")
        .isNotNull();

    assertThat(projectedSchema.findField(101))
        .as("Projected schema should include second nested field")
        .isNotNull();
  }

  @Test
  public void testSimpleNestedFieldsReturnCorrectDetails() throws Exception {
    // Test that nested fields are correctly returned with proper details and structure
    Schema requestedSchema = NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(100));

    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the nested field is returned with correct details
    Types.NestedField nestedField = projectedSchema.findField(100);
    assertThat(nestedField).isNotNull();
    assertThat(nestedField.name()).isEqualTo("structInnerData");
    assertThat(nestedField.type()).isEqualTo(Types.StringType.get());
    assertThat(nestedField.fieldId()).isEqualTo(100);
    assertThat(nestedField.isOptional()).isTrue();

    // Verify the structure is preserved (parent exists at top level)
    Types.NestedField structDataField = projectedSchema.asStruct().field("structData");
    assertThat(structDataField).as("Parent struct should be preserved").isNotNull();
    assertThat(structDataField.type().isStructType()).isTrue();

    // Verify nested field is within parent, not at top level
    Types.StructType structType = structDataField.type().asStructType();
    assertThat(structType.field("structInnerData"))
        .as("Nested field should be within parent struct")
        .isNotNull();
  }

  @Test
  public void testMultipleLayerNesting() throws Exception {
    // Test deeply nested fields: level1.level2.level3.deepField
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(400)); // deepField

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify the deeply nested field is found
    Types.NestedField deepField = projectedSchema.findField(400);
    assertThat(deepField).as("Should find deeply nested field (level 4)").isNotNull();
    assertThat(deepField.name()).isEqualTo("deepField");
    assertThat(deepField.type()).isEqualTo(Types.StringType.get());

    // Verify the full nested structure is preserved
    Types.NestedField level1 = projectedSchema.asStruct().field("level1");
    assertThat(level1).as("Level 1 struct should exist").isNotNull();
    assertThat(level1.type().isStructType()).isTrue();

    Types.NestedField level2 = level1.type().asStructType().field("level2");
    assertThat(level2).as("Level 2 struct should exist within level1").isNotNull();
    assertThat(level2.type().isStructType()).isTrue();

    Types.NestedField level3 = level2.type().asStructType().field("level3");
    assertThat(level3).as("Level 3 struct should exist within level2").isNotNull();
    assertThat(level3.type().isStructType()).isTrue();

    Types.NestedField deepFieldInStruct = level3.type().asStructType().field("deepField");
    assertThat(deepFieldInStruct)
        .as("Deep field should exist within level3 struct, not promoted")
        .isNotNull();
    assertThat(deepFieldInStruct.fieldId()).isEqualTo(400);
  }

  @Test
  public void testMultipleFieldsAtSameLevel() throws Exception {
    // Test referencing multiple fields at the same nesting level
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(400, 410)); // both deep fields at same level

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify both fields at the same level are found
    assertThat(projectedSchema.findField(400)).as("First deep field should be found").isNotNull();
    assertThat(projectedSchema.findField(410)).as("Second deep field should be found").isNotNull();
  }

  @Test
  public void testListElementFields() throws Exception {
    // Test nested fields within list elements
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(501)); // field in list element

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify the list element field is found
    Types.NestedField listElementField = projectedSchema.findField(501);
    assertThat(listElementField).as("List element field should be found").isNotNull();
    assertThat(listElementField.name()).isEqualTo("listElementField");
  }

  @Test
  public void testMultipleListElementFields() throws Exception {
    // Test multiple fields within the same list element struct
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(501, 502)); // both list element fields

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify both list element fields are found
    assertThat(projectedSchema.findField(501))
        .as("First list element field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(502))
        .as("Second list element field should be found")
        .isNotNull();
  }

  @Test
  public void testMapValueFields() throws Exception {
    // Test nested fields within map values
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(602)); // field in map value

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify the map value field is found
    Types.NestedField mapValueField = projectedSchema.findField(602);
    assertThat(mapValueField).as("Map value field should be found").isNotNull();
    assertThat(mapValueField.name()).isEqualTo("mapValueField");
  }

  @Test
  public void testComplexCombination() throws Exception {
    // Test the most complex case: List of Maps with struct values
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(803)); // field in map value in list

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify the deeply complex nested field is found
    Types.NestedField complexField = projectedSchema.findField(803);
    assertThat(complexField)
        .as("Complex nested field (in map value in list) should be found")
        .isNotNull();
    assertThat(complexField.name()).isEqualTo("complexNestedField");
  }

  @Test
  public void testMixedNestedAndTopLevelFields() throws Exception {
    // Test combination of nested fields from different structures
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(
            ImmutableList.of(
                400, 501, 602, 803)); // fields from struct, list, map, and complex combo

    Schema projectedSchema =
        invokeFileProjection(
            COMPLEX_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete));

    // Verify all different types of nested fields are found
    assertThat(projectedSchema.findField(400)).as("Deep struct field should be found").isNotNull();
    assertThat(projectedSchema.findField(501)).as("List element field should be found").isNotNull();
    assertThat(projectedSchema.findField(602)).as("Map value field should be found").isNotNull();
    assertThat(projectedSchema.findField(803))
        .as("Complex combination field should be found")
        .isNotNull();
  }

  @Test
  public void testRegressionSimpleTopLevelFields() throws Exception {
    // Regression test: ensure simple top-level fields continue to work
    Schema requestedSchema = SIMPLE_SCHEMA.select("id", "name");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(3, 4)); // age and active fields

    Schema projectedSchema =
        invokeFileProjection(
            SIMPLE_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify all requested and required fields are present
    assertThat(projectedSchema.findField(1))
        .as("Requested field 'id' should be present")
        .isNotNull();
    assertThat(projectedSchema.findField(2))
        .as("Requested field 'name' should be present")
        .isNotNull();
    assertThat(projectedSchema.findField(3))
        .as("Required field 'age' for equality delete should be present")
        .isNotNull();
    assertThat(projectedSchema.findField(4))
        .as("Required field 'active' for equality delete should be present")
        .isNotNull();

    // Verify field details are correct
    assertThat(projectedSchema.findField(3).name()).isEqualTo("age");
    assertThat(projectedSchema.findField(4).name()).isEqualTo("active");
  }

  @Test
  public void testRegressionNoDeleteFiles() throws Exception {
    // Regression test: when no delete files are present, should return requested schema
    Schema requestedSchema = SIMPLE_SCHEMA.select("id", "name");

    Schema projectedSchema =
        invokeFileProjection(
            SIMPLE_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of());

    // Should return the requested schema unchanged
    assertThat(projectedSchema.columns()).hasSize(2);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
    assertThat(projectedSchema.findField(3)).isNull();
    assertThat(projectedSchema.findField(4)).isNull();
  }

  @Test
  public void testRegressionAllFieldsAlreadyInRequested() throws Exception {
    // Regression test: when all required fields are already in requested schema
    Schema requestedSchema = SIMPLE_SCHEMA; // All fields already requested

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(3, 4)); // fields already in requested

    Schema projectedSchema =
        invokeFileProjection(
            SIMPLE_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Should return the requested schema (which already contains everything)
    assertThat(projectedSchema.columns()).hasSize(4);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
    assertThat(projectedSchema.findField(3)).isNotNull();
    assertThat(projectedSchema.findField(4)).isNotNull();
  }
}
