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

  private static final Schema VERY_DEEP_NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "level1",
              Types.StructType.of(
                  optional(
                      10,
                      "level2",
                      Types.StructType.of(
                          optional(
                              20,
                              "level3",
                              Types.StructType.of(
                                  optional(
                                      30,
                                      "level4",
                                      Types.StructType.of(
                                          optional(
                                              40, "deepValue", Types.StringType.get()))))))))));

  private static final Schema ARRAY_OF_STRUCTS_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "arrayOfStructs",
              Types.ListType.ofOptional(
                  10,
                  Types.StructType.of(
                      optional(20, "structField1", Types.StringType.get()),
                      optional(
                          21,
                          "nestedStruct",
                          Types.StructType.of(
                              optional(30, "deepField", Types.IntegerType.get())))))));

  private static final Schema MAP_WITH_STRUCT_VALUES_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "mapWithStructValues",
              Types.MapType.ofOptional(
                  10,
                  11,
                  Types.StringType.get(),
                  Types.StructType.of(
                      optional(20, "valueField1", Types.IntegerType.get()),
                      optional(
                          21,
                          "nestedValueStruct",
                          Types.StructType.of(
                              optional(30, "deepValueField", Types.StringType.get())))))));

  private static final Schema MIXED_COMPLEX_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(
              2,
              "struct1",
              Types.StructType.of(
                  optional(10, "field1", Types.StringType.get()),
                  optional(11, "field2", Types.IntegerType.get()))),
          optional(
              3,
              "struct2",
              Types.StructType.of(
                  optional(20, "field3", Types.StringType.get()),
                  optional(21, "field4", Types.IntegerType.get()))),
          optional(
              4,
              "arrayField",
              Types.ListType.ofOptional(
                  30, Types.StructType.of(optional(31, "arrayElement", Types.StringType.get())))),
          optional(
              5,
              "mapField",
              Types.MapType.ofOptional(
                  40,
                  41,
                  Types.StringType.get(),
                  Types.StructType.of(optional(42, "mapValue", Types.IntegerType.get())))));

  private static Schema invokeFileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes) {
    return DeleteFilter.fileProjection(tableSchema, requestedSchema, posDeletes, eqDeletes, false);
  }

  private static DeleteFile mockEqDelete(Integer... fieldIds) {
    DeleteFile delete = Mockito.mock(DeleteFile.class);
    Mockito.when(delete.equalityFieldIds()).thenReturn(ImmutableList.copyOf(fieldIds));
    return delete;
  }

  @Test
  public void testProjectionWithSingleNestedField() {
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
  public void testProjectionWithMultipleNestedFields() {
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
  public void testProjectionWithDeepNesting() {
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
  public void testProjectionWithListElementField() {
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
  public void testProjectionWithMapValueField() {
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
  public void testProjectionWithMixedFields() {
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
  public void testProjectionWithNoDeleteFiles() {
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA, requestedSchema, ImmutableList.of(), ImmutableList.of());

    assertThat(projectedSchema.columns()).hasSize(2);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
  }

  @Test
  public void testProjectionWithAllFieldsRequested() {
    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA, NESTED_SCHEMA, ImmutableList.of(), ImmutableList.of(mockEqDelete(100)));

    assertThat(projectedSchema.columns()).hasSize(3);
    assertThat(projectedSchema.findField(1)).isNotNull();
    assertThat(projectedSchema.findField(2)).isNotNull();
    assertThat(projectedSchema.findField(3)).isNotNull();
  }

  @Test
  public void testProjectionWithMetadataColumns() {
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");

    // Invoke with position deletes (requires ROW_POSITION metadata column)
    DeleteFile posDelete = Mockito.mock(DeleteFile.class);
    Schema projectedSchema =
        DeleteFilter.fileProjection(
            NESTED_SCHEMA, requestedSchema, ImmutableList.of(posDelete), ImmutableList.of(), true);

    assertThat(projectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()))
        .as("Should include ROW_POSITION metadata column for position deletes")
        .isNotNull();
    assertThat(projectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()).name())
        .isEqualTo("_pos");
    assertThat(projectedSchema.findField(1)).as("Should include requested field id").isNotNull();
    assertThat(projectedSchema.findField(2)).as("Should include requested field data").isNotNull();
  }

  @Test
  public void testNestedStructureIsPreserved() {
    // This test verifies that when a nested field is added via equality delete,
    // the parent struct is included to preserve the nested structure
    Schema requestedSchema = NESTED_SCHEMA.select("id", "data");

    Schema projectedSchema =
        invokeFileProjection(
            NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(100)));

    // Verify that field 100 (nestedField) is accessible
    assertThat(projectedSchema.findField(100))
        .as("Should include nested field from equality delete")
        .isNotNull();
    assertThat(projectedSchema.findField(100).name()).isEqualTo("nestedField");

    // THIS IS THE KEY ASSERTION: The parent struct (field 3) must be present
    // to preserve the nested structure
    assertThat(projectedSchema.findField(3))
        .as(
            "Should include parent struct field to preserve nested structure. "
                + "Field 100 (nestedField) should be nested inside field 3 (structData), "
                + "not added as a top-level field.")
        .isNotNull();

    // Verify the parent is a struct type
    Types.NestedField structField = projectedSchema.findField(3);
    assertThat(structField.type().isStructType()).as("Field 3 should be a struct type").isTrue();

    // Verify the struct contains the nested field
    Types.StructType structType = structField.type().asStructType();
    assertThat(structType.field(100))
        .as("Struct field 3 should contain nested field 100")
        .isNotNull();
  }

  @Test
  public void testVeryDeepNestedStructure() {
    // Test 4-level deep nesting: level1.level2.level3.level4.deepValue
    Schema requestedSchema = VERY_DEEP_NESTED_SCHEMA.select("id");

    Schema projectedSchema =
        invokeFileProjection(
            VERY_DEEP_NESTED_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(40)));

    assertThat(projectedSchema.findField(40)).as("Should find deeply nested field 40").isNotNull();

    assertThat(projectedSchema.findField(2))
        .as("Should include top-level parent struct (level1) to preserve 4-level nesting")
        .isNotNull();
  }

  @Test
  public void testArrayOfStructsNestedField() {
    // Test array element field: arrayOfStructs.element.nestedStruct.deepField
    Schema requestedSchema = ARRAY_OF_STRUCTS_SCHEMA.select("id");

    Schema projectedSchema =
        invokeFileProjection(
            ARRAY_OF_STRUCTS_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(30)));

    assertThat(projectedSchema.findField(30))
        .as("Should find nested field within array element")
        .isNotNull();

    assertThat(projectedSchema.findField(2))
        .as("Should include array field to preserve structure")
        .isNotNull();

    // Verify it's still an array type
    Types.NestedField arrayField = projectedSchema.findField(2);
    assertThat(arrayField.type().isListType()).as("Field 2 should still be a list type").isTrue();
  }

  @Test
  public void testMapWithStructValuesNestedField() {
    // Test map value field: mapWithStructValues.value.nestedValueStruct.deepValueField
    Schema requestedSchema = MAP_WITH_STRUCT_VALUES_SCHEMA.select("id");

    Schema projectedSchema =
        invokeFileProjection(
            MAP_WITH_STRUCT_VALUES_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(30)));

    assertThat(projectedSchema.findField(30))
        .as("Should find nested field within map value")
        .isNotNull();

    assertThat(projectedSchema.findField(2))
        .as("Should include map field to preserve structure")
        .isNotNull();

    // Verify it's still a map type
    Types.NestedField mapField = projectedSchema.findField(2);
    assertThat(mapField.type().isMapType()).as("Field 2 should still be a map type").isTrue();
  }

  @Test
  public void testMultipleNestedFieldsFromDifferentParents() {
    // Test multiple nested fields from different parent structs
    Schema requestedSchema = MIXED_COMPLEX_SCHEMA.select("id");

    Schema projectedSchema =
        invokeFileProjection(
            MIXED_COMPLEX_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(10, 21, 31, 42)));

    // All nested fields should be found
    assertThat(projectedSchema.findField(10)).as("Should find field 10").isNotNull();
    assertThat(projectedSchema.findField(21)).as("Should find field 21").isNotNull();
    assertThat(projectedSchema.findField(31)).as("Should find field 31").isNotNull();
    assertThat(projectedSchema.findField(42)).as("Should find field 42").isNotNull();

    // All parent fields should be present
    assertThat(projectedSchema.findField(2)).as("Should include struct1 parent").isNotNull();
    assertThat(projectedSchema.findField(3)).as("Should include struct2 parent").isNotNull();
    assertThat(projectedSchema.findField(4)).as("Should include arrayField parent").isNotNull();
    assertThat(projectedSchema.findField(5)).as("Should include mapField parent").isNotNull();
  }

  @Test
  public void testArrayElementStructFieldOnly() {
    // Test selecting only one field from array element struct
    Schema requestedSchema = ARRAY_OF_STRUCTS_SCHEMA.select("id");

    Schema projectedSchema =
        invokeFileProjection(
            ARRAY_OF_STRUCTS_SCHEMA,
            requestedSchema,
            ImmutableList.of(),
            ImmutableList.of(mockEqDelete(20)));

    assertThat(projectedSchema.findField(20)).as("Should find structField1").isNotNull();

    assertThat(projectedSchema.findField(2)).as("Should include arrayOfStructs parent").isNotNull();

    // The array element struct should contain field 20 but may or may not contain field 21
    // depending on implementation - the key is field 20 must be accessible
  }
}
