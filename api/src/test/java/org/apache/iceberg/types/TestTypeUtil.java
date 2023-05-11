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

import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.IntegerType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestTypeUtil {
  @Test
  public void testReassignIdsDuplicateColumns() {
    Schema schema =
        new Schema(
            required(0, "a", Types.IntegerType.get()), required(1, "A", Types.IntegerType.get()));
    Schema sourceSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()), required(2, "A", Types.IntegerType.get()));
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
  }

  @Test
  public void testReassignIdsWithIdentifier() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(0, "a", Types.IntegerType.get()),
                required(1, "A", Types.IntegerType.get())),
            Sets.newHashSet(0));
    Schema sourceSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "a", Types.IntegerType.get()),
                required(2, "A", Types.IntegerType.get())),
            Sets.newHashSet(1));
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals(
        "identifier field ID should change based on source schema",
        sourceSchema.identifierFieldIds(),
        actualSchema.identifierFieldIds());
  }

  @Test
  public void testAssignIncreasingFreshIdWithIdentifier() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get())),
            Sets.newHashSet(10));
    Schema expectedSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "a", Types.IntegerType.get()),
                required(2, "A", Types.IntegerType.get())),
            Sets.newHashSet(1));
    final Schema actualSchema = TypeUtil.assignIncreasingFreshIds(schema);
    Assert.assertEquals(expectedSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals(
        "identifier field ID should change based on source schema",
        expectedSchema.identifierFieldIds(),
        actualSchema.identifierFieldIds());
  }

  @Test
  public void testAssignIncreasingFreshIdNewIdentifier() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get())),
            Sets.newHashSet(10));
    Schema sourceSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "a", Types.IntegerType.get()),
                required(2, "A", Types.IntegerType.get())));
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals(
        "source schema missing identifier should not impact refreshing new identifier",
        Sets.newHashSet(sourceSchema.findField("a").fieldId()),
        actualSchema.identifierFieldIds());
  }

  @Test
  public void testProject() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(13, "b", Types.IntegerType.get()),
                        required(14, "B", Types.IntegerType.get()),
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(
                                required(16, "c", Types.IntegerType.get()),
                                required(17, "C", Types.IntegerType.get())))))));

    Schema expectedTop = new Schema(Lists.newArrayList(required(11, "A", Types.IntegerType.get())));

    Schema actualTop = TypeUtil.project(schema, Sets.newHashSet(11));
    Assert.assertEquals(expectedTop.asStruct(), actualTop.asStruct());

    Schema expectedDepthOne =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(required(13, "b", Types.IntegerType.get())))));

    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(10, 12, 13));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo =
        new Schema(
            Lists.newArrayList(
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(required(17, "C", Types.IntegerType.get())))))));

    Schema actualDepthTwo = TypeUtil.project(schema, Sets.newHashSet(11, 12, 15, 17));
    Schema actualDepthTwoChildren = TypeUtil.project(schema, Sets.newHashSet(11, 17));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwoChildren.asStruct());
  }

  @Test
  public void testProjectNaturallyEmpty() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(required(20, "empty", Types.StructType.of())))))));

    Schema expectedDepthOne =
        new Schema(Lists.newArrayList(required(12, "someStruct", Types.StructType.of())));

    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(12));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(required(15, "anotherStruct", Types.StructType.of())))));

    Schema actualDepthTwo = TypeUtil.project(schema, Sets.newHashSet(12, 15));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());

    Schema expectedDepthThree =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(required(20, "empty", Types.StructType.of())))))));

    Schema actualDepthThree = TypeUtil.project(schema, Sets.newHashSet(12, 15, 20));
    Schema actualDepthThreeChildren = TypeUtil.project(schema, Sets.newHashSet(20));
    Assert.assertEquals(expectedDepthThree.asStruct(), actualDepthThree.asStruct());
    Assert.assertEquals(expectedDepthThree.asStruct(), actualDepthThreeChildren.asStruct());
  }

  @Test
  public void testProjectEmpty() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(13, "b", Types.IntegerType.get()),
                        required(14, "B", Types.IntegerType.get()),
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(
                                required(16, "c", Types.IntegerType.get()),
                                required(17, "C", Types.IntegerType.get())))))));

    Schema expectedDepthOne =
        new Schema(Lists.newArrayList(required(12, "someStruct", Types.StructType.of())));

    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(12));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(required(15, "anotherStruct", Types.StructType.of())))));

    Schema actualDepthTwo = TypeUtil.project(schema, Sets.newHashSet(12, 15));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
  }

  @Test
  public void testSelect() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(13, "b", Types.IntegerType.get()),
                        required(14, "B", Types.IntegerType.get()),
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(
                                required(16, "c", Types.IntegerType.get()),
                                required(17, "C", Types.IntegerType.get())))))));

    Schema expectedTop = new Schema(Lists.newArrayList(required(11, "A", Types.IntegerType.get())));

    Schema actualTop = TypeUtil.select(schema, Sets.newHashSet(11));
    Assert.assertEquals(expectedTop.asStruct(), actualTop.asStruct());

    Schema expectedDepthOne =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(13, "b", Types.IntegerType.get()),
                        required(14, "B", Types.IntegerType.get()),
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(
                                required(16, "c", Types.IntegerType.get()),
                                required(17, "C", Types.IntegerType.get())))))));

    Schema actualDepthOne = TypeUtil.select(schema, Sets.newHashSet(10, 12));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo =
        new Schema(
            Lists.newArrayList(
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(required(17, "C", Types.IntegerType.get())))))));

    Schema actualDepthTwo = TypeUtil.select(schema, Sets.newHashSet(11, 17));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
  }

  @Test
  public void testProjectMap() {
    // We can't partially project keys because it changes key equality
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get()),
                required(
                    12,
                    "map",
                    Types.MapType.ofRequired(
                        13,
                        14,
                        Types.StructType.of(
                            optional(100, "x", Types.IntegerType.get()),
                            optional(101, "y", Types.IntegerType.get())),
                        Types.StructType.of(
                            required(200, "z", Types.IntegerType.get()),
                            optional(
                                201,
                                "innerMap",
                                Types.MapType.ofOptional(
                                    202,
                                    203,
                                    Types.IntegerType.get(),
                                    Types.StructType.of(
                                        required(300, "foo", Types.IntegerType.get()),
                                        required(301, "bar", Types.IntegerType.get())))))))));

    Assert.assertThrows(
        "Cannot project maps explicitly",
        IllegalArgumentException.class,
        () -> TypeUtil.project(schema, Sets.newHashSet(12)));

    Assert.assertThrows(
        "Cannot project maps explicitly",
        IllegalArgumentException.class,
        () -> TypeUtil.project(schema, Sets.newHashSet(201)));

    Schema expectedTopLevel =
        new Schema(Lists.newArrayList(required(10, "a", Types.IntegerType.get())));
    Schema actualTopLevel = TypeUtil.project(schema, Sets.newHashSet(10));
    Assert.assertEquals(expectedTopLevel.asStruct(), actualTopLevel.asStruct());

    Schema expectedDepthOne =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(
                    12,
                    "map",
                    Types.MapType.ofRequired(
                        13,
                        14,
                        Types.StructType.of(
                            optional(100, "x", Types.IntegerType.get()),
                            optional(101, "y", Types.IntegerType.get())),
                        Types.StructType.of()))));
    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(10, 13, 14, 100, 101));
    Schema actualDepthOneNoKeys = TypeUtil.project(schema, Sets.newHashSet(10, 13, 14));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOneNoKeys.asStruct());

    Schema expectedDepthTwo =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(
                    12,
                    "map",
                    Types.MapType.ofRequired(
                        13,
                        14,
                        Types.StructType.of(
                            optional(100, "x", Types.IntegerType.get()),
                            optional(101, "y", Types.IntegerType.get())),
                        Types.StructType.of(
                            required(200, "z", Types.IntegerType.get()),
                            optional(
                                201,
                                "innerMap",
                                Types.MapType.ofOptional(
                                    202, 203, Types.IntegerType.get(), Types.StructType.of())))))));
    Schema actualDepthTwo =
        TypeUtil.project(schema, Sets.newHashSet(10, 13, 14, 100, 101, 200, 202, 203));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
  }

  @Test
  public void testGetProjectedIds() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "A", Types.IntegerType.get()),
                required(35, "emptyStruct", Types.StructType.of()),
                required(
                    12,
                    "someStruct",
                    Types.StructType.of(
                        required(13, "b", Types.IntegerType.get()),
                        required(14, "B", Types.IntegerType.get()),
                        required(
                            15,
                            "anotherStruct",
                            Types.StructType.of(
                                required(16, "c", Types.IntegerType.get()),
                                required(17, "C", Types.IntegerType.get())))))));

    Set<Integer> expectedIds = Sets.newHashSet(10, 11, 35, 12, 13, 14, 15, 16, 17);
    Set<Integer> actualIds = TypeUtil.getProjectedIds(schema);

    Assert.assertEquals(expectedIds, actualIds);
  }

  @Test
  public void testProjectListNested() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "list",
                    Types.ListType.ofRequired(
                        13,
                        Types.ListType.ofRequired(
                            14,
                            Types.MapType.ofRequired(
                                15,
                                16,
                                IntegerType.get(),
                                Types.StructType.of(
                                    required(17, "x", Types.IntegerType.get()),
                                    required(18, "y", Types.IntegerType.get()))))))));

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(12)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(13)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(14)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Schema expected =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "list",
                    Types.ListType.ofRequired(
                        13,
                        Types.ListType.ofRequired(
                            14,
                            Types.MapType.ofRequired(
                                15, 16, IntegerType.get(), Types.StructType.of()))))));

    Schema actual = TypeUtil.project(schema, Sets.newHashSet(16));
    Assert.assertEquals(expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testProjectMapNested() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "map",
                    Types.MapType.ofRequired(
                        13,
                        14,
                        Types.IntegerType.get(),
                        Types.MapType.ofRequired(
                            15,
                            16,
                            Types.IntegerType.get(),
                            Types.ListType.ofRequired(
                                17,
                                Types.StructType.of(
                                    required(18, "x", Types.IntegerType.get()),
                                    required(19, "y", Types.IntegerType.get()))))))));

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(12)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(14)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Assertions.assertThatThrownBy(() -> TypeUtil.project(schema, Sets.newHashSet(16)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot explicitly project List or Map types");

    Schema expected =
        new Schema(
            Lists.newArrayList(
                required(
                    12,
                    "map",
                    Types.MapType.ofRequired(
                        13,
                        14,
                        Types.IntegerType.get(),
                        Types.MapType.ofRequired(
                            15,
                            16,
                            Types.IntegerType.get(),
                            Types.ListType.ofRequired(17, Types.StructType.of()))))));

    Schema actual = TypeUtil.project(schema, Sets.newHashSet(17));
    Assert.assertEquals(expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testReassignIdsIllegalArgumentException() {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()), required(2, "b", Types.IntegerType.get()));
    Schema sourceSchema = new Schema(required(1, "a", Types.IntegerType.get()));
    Assertions.assertThatThrownBy(() -> TypeUtil.reassignIds(schema, sourceSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field b not found in source schema");
  }

  @Test
  public void testValidateSchemaViaIndexByName() {
    Types.NestedField nestedType =
        Types.NestedField.required(
            1,
            "a",
            Types.StructType.of(
                required(2, "b", Types.StructType.of(required(3, "c", Types.BooleanType.get()))),
                required(4, "b.c", Types.BooleanType.get())));

    Assertions.assertThatThrownBy(() -> TypeUtil.indexByName(Types.StructType.of(nestedType)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Invalid schema: multiple fields for name a.b.c");
  }

  @Test
  public void testSelectNot() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(1, "id", Types.LongType.get()),
                required(
                    2,
                    "location",
                    Types.StructType.of(
                        required(3, "lat", Types.DoubleType.get()),
                        required(4, "long", Types.DoubleType.get())))));

    Schema expectedNoPrimitive =
        new Schema(
            Lists.newArrayList(
                required(
                    2,
                    "location",
                    Types.StructType.of(
                        required(3, "lat", Types.DoubleType.get()),
                        required(4, "long", Types.DoubleType.get())))));

    Schema actualNoPrimitve = TypeUtil.selectNot(schema, Sets.newHashSet(1));
    Assert.assertEquals(expectedNoPrimitive.asStruct(), actualNoPrimitve.asStruct());

    // Expected legacy behavior is to completely remove structs if their elements are removed
    Schema expectedNoStructElements = new Schema(required(1, "id", Types.LongType.get()));
    Schema actualNoStructElements = TypeUtil.selectNot(schema, Sets.newHashSet(3, 4));
    Assert.assertEquals(expectedNoStructElements.asStruct(), actualNoStructElements.asStruct());

    // Expected legacy behavior is to ignore selectNot on struct elements.
    Schema actualNoStruct = TypeUtil.selectNot(schema, Sets.newHashSet(2));
    Assert.assertEquals(schema.asStruct(), actualNoStruct.asStruct());
  }

  @Test
  public void testReassignOrRefreshIds() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(10, "a", Types.IntegerType.get()),
                required(11, "c", Types.IntegerType.get()),
                required(12, "B", Types.IntegerType.get())),
            Sets.newHashSet(10));
    Schema sourceSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "a", Types.IntegerType.get()),
                required(15, "B", Types.IntegerType.get())));
    final Schema actualSchema = TypeUtil.reassignOrRefreshIds(schema, sourceSchema);
    final Schema expectedSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "a", Types.IntegerType.get()),
                required(16, "c", Types.IntegerType.get()),
                required(15, "B", Types.IntegerType.get())));
    Assert.assertEquals(expectedSchema.asStruct(), actualSchema.asStruct());
  }

  @Test
  public void testReassignOrRefreshIdsCaseInsensitive() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(1, "FIELD1", Types.IntegerType.get()),
                required(2, "FIELD2", Types.IntegerType.get())));
    Schema sourceSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "field1", Types.IntegerType.get()),
                required(2, "field2", Types.IntegerType.get())));
    final Schema actualSchema = TypeUtil.reassignOrRefreshIds(schema, sourceSchema, false);
    final Schema expectedSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "FIELD1", Types.IntegerType.get()),
                required(2, "FIELD2", Types.IntegerType.get())));
    Assert.assertEquals(expectedSchema.asStruct(), actualSchema.asStruct());
  }
}
