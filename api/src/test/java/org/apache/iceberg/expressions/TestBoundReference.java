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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestBoundReference {
  @Test
  public void testProducesNullNonNestedField() {
    // schema: x (required), y (optional)
    Schema schema =
        new Schema(
            required(1, "x", Types.IntegerType.get()), optional(2, "y", Types.IntegerType.get()));

    Types.NestedField requiredField = schema.findField(1);
    Accessor<StructLike> requiredAccessor = schema.accessorForField(1);

    BoundReference<Integer> requiredRef =
        new BoundReference<>(requiredField, requiredAccessor, "x");
    assertThat(requiredRef.producesNull()).isFalse();

    Types.NestedField optionalField = schema.findField(2);
    Accessor<StructLike> optionalAccessor = schema.accessorForField(2);

    BoundReference<Integer> optionalRef =
        new BoundReference<>(optionalField, optionalAccessor, "y");
    assertThat(optionalRef.producesNull()).isTrue();
  }

  @Test
  public void testProducesNullRequiredLeafNoOptional_ancestors() {
    // schema: s1 (required) -> s2 (required)
    Schema schema =
        new Schema(
            required(1, "s1", Types.StructType.of(required(2, "s2", Types.IntegerType.get()))));

    Types.NestedField requiredLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "s2");
    assertThat(ref.producesNull()).isFalse();
  }

  @Test
  public void testProducesNullOptionalLeaf() {
    // schema: s1 (required) -> s2 (optional)
    Schema schema =
        new Schema(
            required(1, "s1", Types.StructType.of(optional(2, "s2", Types.IntegerType.get()))));

    Types.NestedField optionalLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(optionalLeafField, accessor, "s2");
    assertThat(ref.producesNull()).isTrue();
  }

  @Test
  public void testProducesNullRequiredLeafWithOptionalTopAncestor() {
    // schema: s1 (optional) -> s2 (required)
    Schema schema =
        new Schema(
            optional(1, "s1", Types.StructType.of(required(2, "s2", Types.IntegerType.get()))));

    Types.NestedField requiredLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "s2");
    assertThat(ref.producesNull()).isTrue();
  }

  @Test
  public void testProducesNullRequiredLeafWithOptionalIntermediateAncestor() {
    // schema: s1 (required) -> s2 (optional) -> s3 (required) -> s4 (required)
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                Types.StructType.of(
                    optional(
                        2,
                        "s2",
                        Types.StructType.of(
                            required(
                                3,
                                "s3",
                                Types.StructType.of(
                                    required(4, "s4", Types.IntegerType.get()))))))));

    Types.NestedField requiredLeafField = schema.findField(4);
    Accessor<StructLike> accessor = schema.accessorForField(4);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "s4");
    assertThat(ref.producesNull()).isTrue();
  }

  @Test
  public void testProducesNullRequiredNestedField4() {
    // schema: s1 (required) -> s2 (required) -> s3 (required) -> s4 (required) -> s5 (required)
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                Types.StructType.of(
                    required(
                        2,
                        "s2",
                        Types.StructType.of(
                            required(
                                3,
                                "s3",
                                Types.StructType.of(
                                    required(
                                        4,
                                        "s4",
                                        Types.StructType.of(
                                            required(5, "s5", Types.IntegerType.get()))))))))));

    Types.NestedField requiredLeafField = schema.findField(5);
    Accessor<StructLike> accessor = schema.accessorForField(5);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "s5");
    assertThat(ref.producesNull()).isFalse();
  }
}
