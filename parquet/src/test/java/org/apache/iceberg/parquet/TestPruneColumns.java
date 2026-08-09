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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.GeographyType;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestPruneColumns {
  @Test
  public void testMapKeyValueName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                MapType.ofOptional(
                    2,
                    3,
                    StringType.get(),
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                                    .as(LogicalTypeAnnotation.stringType())
                                    .id(2)
                                    .named("key"))
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("value"))
                            .named("custom_key_value_name"))
                    .as(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testListElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(6)
                                            .named("z"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                1,
                "m",
                ListType.ofOptional(
                    3,
                    StructType.of(
                        NestedField.required(4, "x", DoubleType.get()),
                        NestedField.required(5, "y", DoubleType.get())))));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.buildGroup(Type.Repetition.REPEATED)
                            .addField(
                                Types.buildGroup(Type.Repetition.OPTIONAL)
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(4)
                                            .named("x"))
                                    .addField(
                                        Types.primitive(
                                                PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                                            .id(5)
                                            .named("y"))
                                    .id(3)
                                    .named("custom_element_name"))
                            .named("custom_repeated_name"))
                    .as(LogicalTypeAnnotation.listType())
                    .id(1)
                    .named("m"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should not rename repeated struct").isEqualTo(expected);
  }

  @Test
  public void testStructElementName() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(3)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(7)
                            .named("x"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(8)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(9)
                            .named("z"))
                    .id(6)
                    .named("struct_name_2"))
            .named("table");

    // project map.value.x and map.value.y
    Schema projection =
        new Schema(
            NestedField.optional(
                2,
                "struct_name_1",
                StructType.of(
                    NestedField.required(4, "y", DoubleType.get()),
                    NestedField.required(5, "z", DoubleType.get()))),
            NestedField.optional(6, "struct_name_2", StructType.of()));

    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.buildGroup(Type.Repetition.OPTIONAL)
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(4)
                            .named("y"))
                    .addField(
                        Types.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                            .id(5)
                            .named("z"))
                    .id(2)
                    .named("struct_name_1"))
            .addField(Types.buildGroup(Type.Repetition.OPTIONAL).id(6).named("struct_name_2"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void testVariant() {
    MessageType fileSchema =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .addField(buildVariantType(3, "variant_2"))
            .named("table");

    Schema projection =
        new Schema(
            ImmutableList.of(
                NestedField.required(1, "id", IntegerType.get()),
                NestedField.required(2, "variant_1", VariantType.get())));
    MessageType expected =
        Types.buildMessage()
            .addField(
                Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                    .id(1)
                    .named("id"))
            .addField(buildVariantType(2, "variant_1"))
            .named("table");

    MessageType actual = ParquetSchemaUtil.pruneColumns(fileSchema, projection);
    assertThat(actual).as("Pruned schema should be matched").isEqualTo(expected);
  }

  @Test
  public void acceptsMatchingGeospatialParameters() {
    MessageType fileSchema =
        Types.buildMessage()
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.geometryType(null))
            .id(1)
            .named("geom_default")
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.geometryType("EPSG:3857"))
            .id(2)
            .named("geom_projected")
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.geographyType(null, null))
            .id(3)
            .named("geog_default")
            .optional(PrimitiveTypeName.BINARY)
            .as(
                LogicalTypeAnnotation.geographyType(
                    "EPSG:4326", EdgeInterpolationAlgorithm.ANDOYER))
            .id(4)
            .named("geog_custom")
            .named("table");

    Schema projection =
        new Schema(
            NestedField.optional(1, "geom_default", GeometryType.crs84()),
            NestedField.optional(2, "geom_projected", GeometryType.of("epsg:3857")),
            NestedField.optional(3, "geog_default", GeographyType.crs84()),
            NestedField.optional(
                4, "geog_custom", GeographyType.of("epsg:4326", EdgeAlgorithm.ANDOYER)));

    assertThat(ParquetSchemaUtil.pruneColumns(fileSchema, projection)).isEqualTo(fileSchema);
  }

  @Test
  public void rejectsGeometryCrsMismatch() {
    MessageType fileSchema =
        Types.buildMessage()
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.geometryType("EPSG:3857"))
            .id(1)
            .named("geom")
            .named("table");
    Schema projection = new Schema(NestedField.optional(1, "geom", GeometryType.crs84()));

    assertThatThrownBy(() -> ParquetSchemaUtil.pruneColumns(fileSchema, projection))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot read Parquet type")
        .hasMessageContaining("geometry(OGC:CRS84)");
  }

  @Test
  public void rejectsGeographyCrsMismatch() {
    MessageType fileSchema =
        Types.buildMessage()
            .optional(PrimitiveTypeName.BINARY)
            .as(
                LogicalTypeAnnotation.geographyType(
                    "EPSG:4326", EdgeInterpolationAlgorithm.SPHERICAL))
            .id(1)
            .named("geog")
            .named("table");
    Schema projection = new Schema(NestedField.optional(1, "geog", GeographyType.crs84()));

    assertThatThrownBy(() -> ParquetSchemaUtil.pruneColumns(fileSchema, projection))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot read Parquet type")
        .hasMessageContaining("geography(OGC:CRS84, spherical)");
  }

  @Test
  public void rejectsGeographyAlgorithmMismatch() {
    MessageType fileSchema =
        Types.buildMessage()
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.geographyType("OGC:CRS84", EdgeInterpolationAlgorithm.KARNEY))
            .id(1)
            .named("geog")
            .named("table");
    Schema projection = new Schema(NestedField.optional(1, "geog", GeographyType.crs84()));

    assertThatThrownBy(() -> ParquetSchemaUtil.pruneColumns(fileSchema, projection))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot read Parquet type")
        .hasMessageContaining("geography(OGC:CRS84, spherical)");
  }

  private static Type buildVariantType(int id, String name) {
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("value"))
        .id(id)
        .named(name);
  }
}
