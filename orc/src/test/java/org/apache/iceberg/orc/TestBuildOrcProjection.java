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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/** Test projections on ORC types. */
public class TestBuildOrcProjection {

  @Test
  public void testProjectionPrimitiveNoOp() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    assertEquals(2, orcSchema.getChildren().size());
    assertEquals(1, orcSchema.findSubtype("a").getId());
    assertEquals(TypeDescription.Category.INT, orcSchema.findSubtype("a").getCategory());
    assertEquals(2, orcSchema.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, orcSchema.findSubtype("b").getCategory());
  }

  @Test
  public void testProjectionPrimitive() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Schema evolveSchema =
        new Schema(
            optional(2, "a", Types.StringType.get()),
            optional(3, "c", Types.DateType.get()) // will produce ORC column c_r3 (new)
            );

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    assertEquals(2, newOrcSchema.getChildren().size());
    assertEquals(1, newOrcSchema.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, newOrcSchema.findSubtype("b").getCategory());
    assertEquals(2, newOrcSchema.findSubtype("c_r3").getId());
    assertEquals(TypeDescription.Category.DATE, newOrcSchema.findSubtype("c_r3").getCategory());
  }

  @Test
  public void testProjectionNestedNoOp() {
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(2, "b", Types.StringType.get()), optional(3, "c", Types.DateType.get()));
    Schema originalSchema = new Schema(optional(1, "a", nestedStructType));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(originalSchema, orcSchema);
    assertEquals(1, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("a").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertEquals(2, nestedCol.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, nestedCol.findSubtype("b").getCategory());
    assertEquals(3, nestedCol.findSubtype("c").getId());
    assertEquals(TypeDescription.Category.DATE, nestedCol.findSubtype("c").getCategory());
  }

  @Test
  public void testProjectionNested() {
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(2, "b", Types.StringType.get()), optional(3, "c", Types.DateType.get()));
    Schema originalSchema = new Schema(optional(1, "a", nestedStructType));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Types.StructType newNestedStructType =
        Types.StructType.of(
            optional(3, "cc", Types.DateType.get()), optional(2, "bb", Types.StringType.get()));
    Schema evolveSchema = new Schema(optional(1, "aa", newNestedStructType));

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    assertEquals(1, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("a").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertEquals(2, nestedCol.findSubtype("c").getId());
    assertEquals(TypeDescription.Category.DATE, nestedCol.findSubtype("c").getCategory());
    assertEquals(3, nestedCol.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.STRING, nestedCol.findSubtype("b").getCategory());
  }

  @Test
  public void testEvolutionAddContainerField() {
    Schema baseSchema = new Schema(required(1, "a", Types.IntegerType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            optional(2, "b", Types.StructType.of(required(3, "c", Types.LongType.get()))));

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema);
    assertEquals(2, newOrcSchema.getChildren().size());
    assertEquals(TypeDescription.Category.INT, newOrcSchema.findSubtype("a").getCategory());
    assertEquals(2, newOrcSchema.findSubtype("b_r2").getId());
    assertEquals(TypeDescription.Category.STRUCT, newOrcSchema.findSubtype("b_r2").getCategory());
    TypeDescription nestedCol = newOrcSchema.findSubtype("b_r2");
    assertEquals(3, nestedCol.findSubtype("c_r3").getId());
    assertEquals(TypeDescription.Category.LONG, nestedCol.findSubtype("c_r3").getCategory());
  }

  @Test
  public void testRequiredNestedFieldMissingInFile() {
    Schema baseSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.StructType.of(required(3, "c", Types.LongType.get()))));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(
                2,
                "b",
                Types.StructType.of(
                    required(3, "c", Types.LongType.get()),
                    required(4, "d", Types.LongType.get()))));

    Assertions.assertThatThrownBy(
            () -> ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field 4 of type long is required and was not found.");
  }
}
