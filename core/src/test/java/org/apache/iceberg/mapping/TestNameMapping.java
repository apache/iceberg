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
package org.apache.iceberg.mapping;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestNameMapping {
  @Test
  public void testFlatSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    MappedFields expected = MappedFields.of(MappedField.of(1, "id"), MappedField.of(2, "data"));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testNestedStructSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "location",
                Types.StructType.of(
                    required(4, "latitude", Types.FloatType.get()),
                    required(5, "longitude", Types.FloatType.get()))));

    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(
                3,
                "location",
                MappedFields.of(MappedField.of(4, "latitude"), MappedField.of(5, "longitude"))));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testMapSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "map",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.DoubleType.get())));

    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(
                3, "map", MappedFields.of(MappedField.of(4, "key"), MappedField.of(5, "value"))));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testComplexKeyMapSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "map",
                Types.MapType.ofRequired(
                    4,
                    5,
                    Types.StructType.of(
                        required(6, "x", Types.DoubleType.get()),
                        required(7, "y", Types.DoubleType.get())),
                    Types.DoubleType.get())));

    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(
                3,
                "map",
                MappedFields.of(
                    MappedField.of(
                        4, "key", MappedFields.of(MappedField.of(6, "x"), MappedField.of(7, "y"))),
                    MappedField.of(5, "value"))));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testComplexValueMapSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "map",
                Types.MapType.ofRequired(
                    4,
                    5,
                    Types.DoubleType.get(),
                    Types.StructType.of(
                        required(6, "x", Types.DoubleType.get()),
                        required(7, "y", Types.DoubleType.get())))));

    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(
                3,
                "map",
                MappedFields.of(
                    MappedField.of(4, "key"),
                    MappedField.of(
                        5,
                        "value",
                        MappedFields.of(MappedField.of(6, "x"), MappedField.of(7, "y"))))));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testListSchemaToMapping() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(3, "list", Types.ListType.ofRequired(4, Types.StringType.get())));

    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "list", MappedFields.of(MappedField.of(4, "element"))));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }

  @Test
  public void testFailsDuplicateId() {
    // the schema can be created because ID indexing is lazy
    assertThatThrownBy(
            () ->
                new Schema(
                    required(1, "id", Types.LongType.get()),
                    required(1, "data", Types.StringType.get())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Multiple entries with same key: 1=id and 1=data");
  }

  @Test
  public void testFailsDuplicateName() {
    assertThatThrownBy(
            () -> new NameMapping(MappedFields.of(MappedField.of(1, "x"), MappedField.of(2, "x"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Multiple entries with same key: x=2 and x=1");
  }

  @Test
  public void testAllowsDuplicateNamesInSeparateContexts() {
    new NameMapping(
        MappedFields.of(
            MappedField.of(1, "x", MappedFields.of(MappedField.of(3, "x"))),
            MappedField.of(2, "y", MappedFields.of(MappedField.of(4, "x")))));
  }

  @Test
  public void testMappingFindById() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "map",
                Types.MapType.ofRequired(
                    4,
                    5,
                    Types.DoubleType.get(),
                    Types.StructType.of(
                        required(6, "x", Types.DoubleType.get()),
                        required(7, "y", Types.DoubleType.get())))),
            required(8, "list", Types.ListType.ofRequired(9, Types.StringType.get())),
            required(
                10,
                "location",
                Types.StructType.of(
                    required(11, "latitude", Types.FloatType.get()),
                    required(12, "longitude", Types.FloatType.get()))));

    NameMapping mapping = MappingUtil.create(schema);

    assertThat(mapping.find(100)).as("Should not return a field mapping for a missing ID").isNull();
    assertThat(mapping.find(2)).isEqualTo(MappedField.of(2, "data"));
    assertThat(mapping.find(6)).isEqualTo(MappedField.of(6, "x"));
    assertThat(mapping.find(9)).isEqualTo(MappedField.of(9, "element"));
    assertThat(mapping.find(11)).isEqualTo(MappedField.of(11, "latitude"));
    assertThat(mapping.find(10))
        .isEqualTo(
            MappedField.of(
                10,
                "location",
                MappedFields.of(MappedField.of(11, "latitude"), MappedField.of(12, "longitude"))));
  }

  @Test
  public void testMappingFindByName() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(
                3,
                "map",
                Types.MapType.ofRequired(
                    4,
                    5,
                    Types.DoubleType.get(),
                    Types.StructType.of(
                        required(6, "x", Types.DoubleType.get()),
                        required(7, "y", Types.DoubleType.get())))),
            required(8, "list", Types.ListType.ofRequired(9, Types.StringType.get())),
            required(
                10,
                "location",
                Types.StructType.of(
                    required(11, "latitude", Types.FloatType.get()),
                    required(12, "longitude", Types.FloatType.get()))));

    NameMapping mapping = MappingUtil.create(schema);

    assertThat(mapping.find("element"))
        .as("Should not return a field mapping for a nested name")
        .isNull();
    assertThat(mapping.find("x"))
        .as("Should not return a field mapping for a nested name")
        .isNull();
    assertThat(mapping.find("key"))
        .as("Should not return a field mapping for a nested name")
        .isNull();
    assertThat(mapping.find("value"))
        .as("Should not return a field mapping for a nested name")
        .isNull();
    assertThat(mapping.find("data")).isEqualTo(MappedField.of(2, "data"));
    assertThat(mapping.find("map", "value", "x")).isEqualTo(MappedField.of(6, "x"));
    assertThat(mapping.find("list", "element")).isEqualTo(MappedField.of(9, "element"));
    assertThat(mapping.find("location", "latitude")).isEqualTo(MappedField.of(11, "latitude"));
    assertThat(mapping.find("location"))
        .isEqualTo(
            MappedField.of(
                10,
                "location",
                MappedFields.of(MappedField.of(11, "latitude"), MappedField.of(12, "longitude"))));
  }

  @Test
  public void testMappingVariantType() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.VariantType.get()));

    MappedFields expected = MappedFields.of(MappedField.of(1, "id"), MappedField.of(2, "data"));

    NameMapping mapping = MappingUtil.create(schema);
    assertThat(mapping.asMappedFields()).isEqualTo(expected);
  }
}
