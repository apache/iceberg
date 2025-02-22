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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPruneColumns {
  private static final org.apache.avro.Schema TEST_SCHEMA =
      AvroSchemaUtil.convert(
          new Schema(
                  Types.NestedField.required(0, "id", Types.LongType.get()),
                  Types.NestedField.optional(
                      2,
                      "properties",
                      Types.MapType.ofOptional(
                          3, 4, Types.StringType.get(), Types.IntegerType.get())),
                  Types.NestedField.required(
                      5,
                      "location",
                      Types.StructType.of(
                          Types.NestedField.required(6, "lat", Types.FloatType.get()),
                          Types.NestedField.optional(7, "long", Types.FloatType.get()))),
                  Types.NestedField.required(
                      8, "tags", Types.ListType.ofRequired(9, Types.StringType.get())),
                  Types.NestedField.optional(10, "payload", Types.VariantType.get()))
              .asStruct());

  @Test
  public void testSimple() {
    Schema expected = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(0));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  public void testSelectMap(int selectedId) {
    Schema expected =
        new Schema(
            Types.NestedField.optional(
                2,
                "properties",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.IntegerType.get())));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(selectedId));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @Test
  public void testSelectEmptyStruct() {
    Schema expected = new Schema(Types.NestedField.required(5, "location", Types.StructType.of()));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(5));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @Test
  public void testSelectStructField() {
    Schema expected =
        new Schema(
            Types.NestedField.required(
                5,
                "location",
                Types.StructType.of(Types.NestedField.optional(7, "long", Types.FloatType.get()))));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(7));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9})
  public void testSelectList(int selectedId) {
    Schema expected =
        new Schema(
            Types.NestedField.required(
                8, "tags", Types.ListType.ofRequired(9, Types.StringType.get())));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(selectedId));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @Test
  public void testSelectVariant() {
    Schema expected =
        new Schema(Types.NestedField.optional(10, "payload", Types.VariantType.get()));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(TEST_SCHEMA, Sets.newHashSet(10));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }
}
