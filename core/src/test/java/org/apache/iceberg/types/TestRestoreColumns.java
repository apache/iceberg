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
package org.apache.iceberg.types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestRestoreColumns {
  private static final Types.StructType POINT_STRUCT =
      Types.StructType.of(
          required(20, "x", Types.IntegerType.get()), required(21, "y", Types.IntegerType.get()));

  private static final Types.StructType ADDRESS_STRUCT =
      Types.StructType.of(
          optional(30, "street", Types.StringType.get()),
          optional(31, "city", Types.StringType.get()),
          optional(32, "zip", Types.StringType.get()));

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(
              3,
              "struct",
              Types.StructType.of(
                  required(4, "x", Types.IntegerType.get()),
                  required(5, "y", Types.IntegerType.get()))),
          optional(6, "list", Types.ListType.ofOptional(7, Types.IntegerType.get())),
          optional(8, "points", Types.ListType.ofOptional(9, POINT_STRUCT)),
          optional(
              10,
              "properties",
              Types.MapType.ofOptional(11, 12, Types.StringType.get(), Types.StringType.get())),
          optional(
              13,
              "addresses",
              Types.MapType.ofOptional(14, 15, Types.StringType.get(), ADDRESS_STRUCT)));

  private static final Schema EMPTY_PROJECTION = new Schema();

  // a partial projection that drops at least one field from every struct type in SCHEMA, as well
  // as the "data", "list", and "properties" fields entirely
  private static final Types.StructType PARTIAL_POINT_STRUCT =
      Types.StructType.of(required(20, "x", Types.IntegerType.get()));

  private static final Types.StructType PARTIAL_ADDRESS_STRUCT =
      Types.StructType.of(
          optional(30, "street", Types.StringType.get()),
          optional(31, "city", Types.StringType.get()));

  private static final Schema PARTIAL_PROJECTION =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))),
          optional(8, "points", Types.ListType.ofOptional(9, PARTIAL_POINT_STRUCT)),
          optional(
              13,
              "addresses",
              Types.MapType.ofOptional(14, 15, Types.StringType.get(), PARTIAL_ADDRESS_STRUCT)));

  // ---- no restoration ----

  @Test
  void emptyProjectionStaysEmpty() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of());
    assertThat(actual.asStruct()).isEqualTo(EMPTY_PROJECTION.asStruct());
  }

  @Test
  void fullProjectionStaysFull() {
    Schema actual = RestoreColumns.restore(SCHEMA, SCHEMA, ImmutableSet.of());
    assertThat(actual.asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  @Test
  void partialProjectionStaysPartial() {
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of());
    assertThat(actual.asStruct()).isEqualTo(PARTIAL_PROJECTION.asStruct());
  }

  // ---- restore against an empty projection: the full type is projected for any restored ID ----

  @Test
  void restoreTopLevelPrimitive() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(2));
    Schema expected = new Schema(optional(2, "data", Types.StringType.get()));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreWholeStructField() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(3));
    Schema expected =
        new Schema(
            optional(
                3,
                "struct",
                Types.StructType.of(
                    required(4, "x", Types.IntegerType.get()),
                    required(5, "y", Types.IntegerType.get()))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreOneStructField() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(5));
    Schema expected =
        new Schema(
            optional(3, "struct", Types.StructType.of(required(5, "y", Types.IntegerType.get()))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreWholeListField() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(6));
    Schema expected =
        new Schema(optional(6, "list", Types.ListType.ofOptional(7, Types.IntegerType.get())));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreListElementId() {
    // restoring the element ID has the same effect as restoring the list field itself
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(7));
    Schema expected =
        new Schema(optional(6, "list", Types.ListType.ofOptional(7, Types.IntegerType.get())));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreListOfStructsElementId() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(9));
    Schema expected = new Schema(optional(8, "points", Types.ListType.ofOptional(9, POINT_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreOneFieldOfListOfStructsElement() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(20));
    Schema expected =
        new Schema(
            optional(
                8,
                "points",
                Types.ListType.ofOptional(
                    9, Types.StructType.of(required(20, "x", Types.IntegerType.get())))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreWholeMapField() {
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(13));
    Schema expected =
        new Schema(
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreOneFieldOfMapValueStruct() {
    // uses the original key because keys are required and fully projected
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(32));
    Schema expected =
        new Schema(
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(
                    14,
                    15,
                    Types.StringType.get(),
                    Types.StructType.of(optional(32, "zip", Types.StringType.get())))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restorePrimitiveMapValue() {
    // uses the original key because keys are required and fully projected
    Schema actual = RestoreColumns.restore(SCHEMA, EMPTY_PROJECTION, ImmutableSet.of(12));
    Schema expected =
        new Schema(
            optional(
                10,
                "properties",
                Types.MapType.ofOptional(11, 12, Types.StringType.get(), Types.StringType.get())));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  // ---- restore against a partial projection: restoring a dropped field re-expands the rest of
  // that already-partial struct, while leaving unrelated partial structs untouched ----

  @Test
  void restoreDroppedStructFieldExpandsStruct() {
    // restore 5: struct.y
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(5));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "struct",
                Types.StructType.of(
                    required(4, "x", Types.IntegerType.get()),
                    required(5, "y", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, PARTIAL_POINT_STRUCT)),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), PARTIAL_ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreDroppedListElementFieldExpandsElementStruct() {
    // restore 21: points.element.y
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(21));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, POINT_STRUCT)),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), PARTIAL_ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreDroppedMapValueFieldExpandsValueStruct() {
    // restore 32: addresses.value.zip
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(32));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, PARTIAL_POINT_STRUCT)),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreFieldDroppedEntirelyKeepsOtherPartialStructs() {
    // restore 2: data, leaving other partial projections unchanged
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(2));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, PARTIAL_POINT_STRUCT)),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), PARTIAL_ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreMapDroppedEntirelyKeepsOtherPartialStructs() {
    // restore 12: properties.value (projects whole map)
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(12));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, PARTIAL_POINT_STRUCT)),
            optional(
                10,
                "properties",
                Types.MapType.ofOptional(11, 12, Types.StringType.get(), Types.StringType.get())),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), PARTIAL_ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void restoreAllPartialFieldsExpandsEveryStruct() {
    Schema actual = RestoreColumns.restore(SCHEMA, PARTIAL_PROJECTION, ImmutableSet.of(5, 21, 32));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "struct",
                Types.StructType.of(
                    required(4, "x", Types.IntegerType.get()),
                    required(5, "y", Types.IntegerType.get()))),
            optional(8, "points", Types.ListType.ofOptional(9, POINT_STRUCT)),
            optional(
                13,
                "addresses",
                Types.MapType.ofOptional(14, 15, Types.StringType.get(), ADDRESS_STRUCT)));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void structToStructProjectionNoMatchingFields() {
    // when there are no field changes, the projection is used as-is
    Schema original =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "struct", Types.StructType.of(required(5, "y", Types.IntegerType.get()))));

    Schema partial =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));

    Schema actual = RestoreColumns.restore(original, partial, ImmutableSet.of());
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void emptyStructToStructProjection() {
    // when there are no field changes, the projection is used as-is
    Schema original =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "struct", Types.StructType.of()));

    Schema partial =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));

    Schema actual = RestoreColumns.restore(original, partial, ImmutableSet.of());
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }

  @Test
  void unknownToStructProjection() {
    // when comparing unknown to a projected struct, the projection is returned
    Schema original =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "struct", Types.UnknownType.get()));

    Schema partial =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));

    Schema actual = RestoreColumns.restore(original, partial, ImmutableSet.of());
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "struct", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));
    assertThat(actual.asStruct()).isEqualTo(expected.asStruct());
  }
}
