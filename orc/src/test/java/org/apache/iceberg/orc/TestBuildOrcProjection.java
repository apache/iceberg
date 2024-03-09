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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

/** Test projections on ORC types. */
public class TestBuildOrcProjection {

  @Test
  public void testProjectionPrimitiveNoOp() {
    Schema originalSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()), optional(2, "b", Types.StringType.get()));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    assertThat(orcSchema.getChildren()).hasSize(2);
    assertThat(orcSchema.findSubtype("a").getId()).isEqualTo(1);
    assertThat(orcSchema.findSubtype("a").getCategory()).isEqualTo(TypeDescription.Category.INT);
    assertThat(orcSchema.findSubtype("b").getId()).isEqualTo(2);
    assertThat(orcSchema.findSubtype("b").getCategory()).isEqualTo(TypeDescription.Category.STRING);
  }

  @Test
  public void testProjectionPrimitive() {
    Configuration config = new Configuration();
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

    TypeDescription newOrcSchema =
        ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema, false);
    assertThat(newOrcSchema.getChildren()).hasSize(2);
    assertThat(newOrcSchema.findSubtype("b").getId()).isEqualTo(1);
    assertThat(newOrcSchema.findSubtype("b").getCategory())
        .isEqualTo(TypeDescription.Category.STRING);
    assertThat(newOrcSchema.findSubtype("c_r3").getId()).isEqualTo(2);
    assertThat(newOrcSchema.findSubtype("c_r3").getCategory())
        .isEqualTo(TypeDescription.Category.DATE);
  }

  @Test
  public void testProjectionNestedNoOp() {
    Configuration config = new Configuration();
    Types.StructType nestedStructType =
        Types.StructType.of(
            optional(2, "b", Types.StringType.get()), optional(3, "c", Types.DateType.get()));
    Schema originalSchema = new Schema(optional(1, "a", nestedStructType));

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    TypeDescription newOrcSchema =
        ORCSchemaUtil.buildOrcProjection(originalSchema, orcSchema, false);
    assertThat(newOrcSchema.getChildren()).hasSize(1);
    assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertThat(nestedCol.findSubtype("b").getId()).isEqualTo(2);
    assertThat(nestedCol.findSubtype("b").getCategory()).isEqualTo(TypeDescription.Category.STRING);
    assertThat(nestedCol.findSubtype("c").getId()).isEqualTo(3);
    assertThat(nestedCol.findSubtype("c").getCategory()).isEqualTo(TypeDescription.Category.DATE);
  }

  @Test
  public void testProjectionNested() {
    Configuration config = new Configuration();
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

    TypeDescription newOrcSchema =
        ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema, false);
    assertThat(newOrcSchema.getChildren()).hasSize(1);
    assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("a");
    assertThat(nestedCol.findSubtype("c").getId()).isEqualTo(2);
    assertThat(nestedCol.findSubtype("c").getCategory()).isEqualTo(TypeDescription.Category.DATE);
    assertThat(nestedCol.findSubtype("b").getId()).isEqualTo(3);
    assertThat(nestedCol.findSubtype("b").getCategory()).isEqualTo(TypeDescription.Category.STRING);
  }

  @Test
  public void testEvolutionAddContainerField() {
    Configuration config = new Configuration();
    Schema baseSchema = new Schema(required(1, "a", Types.IntegerType.get()));
    TypeDescription baseOrcSchema = ORCSchemaUtil.convert(baseSchema);

    Schema evolvedSchema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            optional(2, "b", Types.StructType.of(required(3, "c", Types.LongType.get()))));

    TypeDescription newOrcSchema =
        ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema, false);
    assertThat(newOrcSchema.getChildren()).hasSize(2);
    assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.INT);
    assertThat(newOrcSchema.findSubtype("b_r2").getId()).isEqualTo(2);
    assertThat(newOrcSchema.findSubtype("b_r2").getCategory())
        .isEqualTo(TypeDescription.Category.STRUCT);
    TypeDescription nestedCol = newOrcSchema.findSubtype("b_r2");
    assertThat(nestedCol.findSubtype("c_r3").getId()).isEqualTo(3);
    assertThat(nestedCol.findSubtype("c_r3").getCategory())
        .isEqualTo(TypeDescription.Category.LONG);
  }

  @Test
  public void testRequiredNestedFieldMissingInFile() {
    Configuration config = new Configuration();
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

    assertThatThrownBy(
            () -> ORCSchemaUtil.buildOrcProjection(evolvedSchema, baseOrcSchema, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field 4 of type long is required and was not found.");
  }

  @Test
  public void testTimestampType() {
    Configuration config = new Configuration();
    config.setBoolean(ConfigProperties.ORC_CONVERT_TIMESTAMPTZ, true);

    Schema originalSchema = new Schema(optional(1, "a", Types.TimestampType.withoutZone()));
    // Orc schema would be `timestamp` if table is converted from a hive table
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Schema evolveSchema = new Schema(optional(1, "a", Types.TimestampType.withZone()));

    // iceberg schema is timestamptz
    TypeDescription newOrcSchema =
        ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema, config);
    Assertions.assertThat(newOrcSchema.getChildren()).hasSize(1);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getId()).isEqualTo(1);
    Assertions.assertThat(newOrcSchema.findSubtype("a").getCategory())
        .isEqualTo(TypeDescription.Category.TIMESTAMP_INSTANT);
  }
}
