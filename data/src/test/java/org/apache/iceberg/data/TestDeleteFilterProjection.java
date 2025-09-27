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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
                  Types.StructType.of(
                      optional(702, "mapKeyField", Types.StringType.get())),
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
    Method method = DeleteFilter.class.getDeclaredMethod(
        "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    method.setAccessible(true);
    return method;
  }

  private Schema invokeFileProjection(Schema tableSchema, Schema requestedSchema,
                                     List<DeleteFile> posDeletes, List<DeleteFile> eqDeletes) throws Exception {
    return (Schema) getFileProjectionMethod().invoke(
        null, tableSchema, requestedSchema, posDeletes, eqDeletes, false);
  }

  @Test
  public void testFileProjectionWithNestedFields() throws Exception {
    // This test verifies that the TODO "support adding nested columns" has been fixed
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");

    // Create a mock delete file that references the nested field (structInnerData)
    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(100)); // nested field ID

    List<DeleteFile> eqDeletes = ImmutableList.of(mockEqDelete);
    List<DeleteFile> posDeletes = ImmutableList.of();

    // Use reflection to access the private fileProjection method
    Method fileProjectionMethod = DeleteFilter.class.getDeclaredMethod(
        "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    fileProjectionMethod.setAccessible(true);

    // Before the fix, this would throw:
    // "Cannot find required field for ID 100" when trying to access nested field
    Schema projectedSchema = (Schema) fileProjectionMethod.invoke(
        null, NESTED_SCHEMA, requestedSchema, posDeletes, eqDeletes, false);

    // Verify the projection includes the nested field
    assertThat(projectedSchema.findField(100))
        .as("Projected schema should include nested field structInnerData")
        .isNotNull();

    assertThat(projectedSchema.findField(100).name())
        .as("Nested field should have correct name")
        .isEqualTo("structInnerData");

    // Verify the projection also includes the original requested fields
    assertThat(projectedSchema.findField(1))
        .as("Projected schema should include id field")
        .isNotNull();

    assertThat(projectedSchema.findField(2))
        .as("Projected schema should include data field")
        .isNotNull();

    // Note: This minimal fix allows finding nested fields using tableSchema.findField()
    // Parent structures are not automatically included in the projection
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
    Method fileProjectionMethod = DeleteFilter.class.getDeclaredMethod(
        "fileProjection", Schema.class, Schema.class, List.class, List.class, boolean.class);
    fileProjectionMethod.setAccessible(true);

    // This should not throw an exception
    Schema projectedSchema = (Schema) fileProjectionMethod.invoke(
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
    // Test that nested fields are correctly returned with proper details
    Schema requestedSchema = NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(100));

    Schema projectedSchema = invokeFileProjection(
        NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the nested field is returned with correct details
    Types.NestedField nestedField = projectedSchema.findField(100);
    assertThat(nestedField).isNotNull();
    assertThat(nestedField.name()).isEqualTo("structInnerData");
    assertThat(nestedField.type()).isEqualTo(Types.StringType.get());
    assertThat(nestedField.fieldId()).isEqualTo(100);
    assertThat(nestedField.isOptional()).isTrue();

    // Note: With the current simple implementation, parent structures are not automatically included
    // The nested field is accessible directly through the schema's findField method
  }

  @Test
  public void testMultipleLayerNesting() throws Exception {
    // Test deeply nested fields: level1.level2.level3.deepField
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(400)); // deepField

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the deeply nested field is found
    Types.NestedField deepField = projectedSchema.findField(400);
    assertThat(deepField)
        .as("Should find deeply nested field (level 4)")
        .isNotNull();
    assertThat(deepField.name()).isEqualTo("deepField");
    assertThat(deepField.type()).isEqualTo(Types.StringType.get());

    // Note: With the current implementation, only the specific requested field is added
    // Parent structures are not automatically included in the projection
  }

  @Test
  public void testMultipleFieldsAtSameLevel() throws Exception {
    // Test referencing multiple fields at the same nesting level
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(400, 410)); // both deep fields at same level

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify both fields at the same level are found
    assertThat(projectedSchema.findField(400))
        .as("First deep field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(410))
        .as("Second deep field should be found")
        .isNotNull();
  }

  @Test
  public void testListElementFields() throws Exception {
    // Test nested fields within list elements
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(501)); // field in list element

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the list element field is found
    Types.NestedField listElementField = projectedSchema.findField(501);
    assertThat(listElementField)
        .as("List element field should be found")
        .isNotNull();
    assertThat(listElementField.name()).isEqualTo("listElementField");

    // Note: With the current implementation, parent list structures are not automatically included
    // Only the specific nested field within the list element is added
  }

  @Test
  public void testMultipleListElementFields() throws Exception {
    // Test multiple fields within the same list element struct
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(501, 502)); // both list element fields

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

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
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(602)); // field in map value

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the map value field is found
    Types.NestedField mapValueField = projectedSchema.findField(602);
    assertThat(mapValueField)
        .as("Map value field should be found")
        .isNotNull();
    assertThat(mapValueField.name()).isEqualTo("mapValueField");

    // Note: With the current implementation, parent map structures are not automatically included
    // Only the specific nested field within the map value is added
  }

  @Test
  public void testMapKeyFields() throws Exception {
    // Test nested fields within map keys (complex case)
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id", "data");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(702)); // field in map key

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the map key field is found
    Types.NestedField mapKeyField = projectedSchema.findField(702);
    assertThat(mapKeyField)
        .as("Map key field should be found")
        .isNotNull();
    assertThat(mapKeyField.name()).isEqualTo("mapKeyField");

    // Note: With the current implementation, parent map structures are not automatically included
    // Only the specific nested field within the map key is added
  }

  @Test
  public void testMapKeyAndValueFields() throws Exception {
    // Test both map key and value fields together
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(602, 702)); // value and key fields

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify both map key and value fields are found
    assertThat(projectedSchema.findField(602))
        .as("Map value field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(702))
        .as("Map key field should be found")
        .isNotNull();
  }

  @Test
  public void testComplexCombination() throws Exception {
    // Test the most complex case: List of Maps with struct values
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(803)); // field in map value in list

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify the deeply complex nested field is found
    Types.NestedField complexField = projectedSchema.findField(803);
    assertThat(complexField)
        .as("Complex nested field (in map value in list) should be found")
        .isNotNull();
    assertThat(complexField.name()).isEqualTo("complexNestedField");

    // Note: With the current implementation, parent structures are not automatically included
    // Only the specific nested field within the complex structure is added
  }

  @Test
  public void testMixedNestedAndTopLevelFields() throws Exception {
    // Test combination of nested fields from different structures
    Schema requestedSchema = COMPLEX_NESTED_SCHEMA.select("id");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds())
        .thenReturn(ImmutableList.of(400, 501, 602, 803)); // fields from struct, list, map, and complex combo

    Schema projectedSchema = invokeFileProjection(
        COMPLEX_NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Verify all different types of nested fields are found
    assertThat(projectedSchema.findField(400))
        .as("Deep struct field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(501))
        .as("List element field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(602))
        .as("Map value field should be found")
        .isNotNull();
    assertThat(projectedSchema.findField(803))
        .as("Complex combination field should be found")
        .isNotNull();
  }

  @Test
  public void testRegressionSimpleTopLevelFields() throws Exception {
    // Regression test: ensure simple top-level fields continue to work
    Schema requestedSchema = SIMPLE_SCHEMA.select("id", "name");

    DeleteFile mockEqDelete = Mockito.mock(DeleteFile.class);
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(3, 4)); // age and active fields

    Schema projectedSchema = invokeFileProjection(
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

    Schema projectedSchema = invokeFileProjection(
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
    Mockito.when(mockEqDelete.equalityFieldIds()).thenReturn(ImmutableList.of(3, 4)); // fields already in requested

    Schema projectedSchema = invokeFileProjection(
        SIMPLE_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of(mockEqDelete));

    // Should return the requested schema (which already contains everything)
    assertThat(projectedSchema.columns()).hasSize(4);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
    assertThat(projectedSchema.findField(3)).isNotNull();
    assertThat(projectedSchema.findField(4)).isNotNull();
  }
}
