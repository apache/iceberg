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

public class TestBoundReferenceProducesNull {
  @Test
  public void testNonNestedField() {
    // schema: required, optional
    Schema schema =
        new Schema(
            required(1, "required", Types.IntegerType.get()),
            optional(2, "optional", Types.IntegerType.get()));

    Types.NestedField requiredField = schema.findField(1);
    Accessor<StructLike> requiredAccessor = schema.accessorForField(1);

    BoundReference<Integer> requiredRef =
        new BoundReference<>(requiredField, requiredAccessor, "required");
    assertThat(requiredRef.producesNull()).isFalse();

    Types.NestedField optionalField = schema.findField(2);
    Accessor<StructLike> optionalAccessor = schema.accessorForField(2);

    BoundReference<Integer> optionalRef =
        new BoundReference<>(optionalField, optionalAccessor, "optional");
    assertThat(optionalRef.producesNull()).isTrue();
  }

  @Test
  public void testRequiredLeafNoOptionalAncestors() {
    // schema: requiredAncestor -> requiredLeaf
    Schema schema =
        new Schema(
            required(
                1,
                "requiredAncestor",
                Types.StructType.of(required(2, "requiredLeaf", Types.IntegerType.get()))));

    Types.NestedField requiredLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "requiredLeaf");
    assertThat(ref.producesNull()).isFalse();
  }

  @Test
  public void testOptionalLeaf() {
    // schema: requiredAncestor -> optionalLeaf
    Schema schema =
        new Schema(
            required(
                1,
                "requiredAncestor",
                Types.StructType.of(optional(2, "optionalLeaf", Types.IntegerType.get()))));

    Types.NestedField optionalLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(optionalLeafField, accessor, "optionalLeaf");
    assertThat(ref.producesNull()).isTrue();
  }

  @Test
  public void testRequiredLeafWithOptionalTopAncestor() {
    // schema: optionalAncestor -> requiredLeaf
    Schema schema =
        new Schema(
            optional(
                1,
                "optionalAncestor",
                Types.StructType.of(required(2, "requiredLeaf", Types.IntegerType.get()))));

    Types.NestedField requiredLeafField = schema.findField(2);
    Accessor<StructLike> accessor = schema.accessorForField(2);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "requiredLeaf");
    assertThat(ref.producesNull()).isTrue();
  }

  @Test
  public void testRequiredLeafWithOptionalIntermediateAncestor() {
    // schema: requiredRoot -> optionalIntermediate -> requiredIntermediate -> requiredLeaf
    Schema schema =
        new Schema(
            required(
                1,
                "requiredRoot",
                Types.StructType.of(
                    optional(
                        2,
                        "optionalIntermediate",
                        Types.StructType.of(
                            required(
                                3,
                                "requiredIntermediate",
                                Types.StructType.of(
                                    required(4, "requiredLeaf", Types.IntegerType.get()))))))));

    Types.NestedField requiredLeafField = schema.findField(4);
    Accessor<StructLike> accessor = schema.accessorForField(4);

    BoundReference<Integer> ref = new BoundReference<>(requiredLeafField, accessor, "requiredLeaf");
    assertThat(ref.producesNull()).isTrue();
  }
}
