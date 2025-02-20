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
import org.apache.iceberg.types.Types;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

public class TestPruneColumns {
  @Test
  public void testPruneColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "location",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.optional(2, "long", Types.FloatType.get())))),
            Types.NestedField.required(
                8, "types", Types.ListType.ofRequired(9, Types.StringType.get())),
            Types.NestedField.required(10, "data", Types.VariantType.get()));
    Schema expected =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(
                8, "types", Types.ListType.ofRequired(9, Types.StringType.get())));
    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(AvroSchemaUtil.convert(schema.asStruct()), Sets.set(0, 9));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @Test
  public void testSelectVariant() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(1, "data", Types.VariantType.get()));
    Schema expected = new Schema(Types.NestedField.required(1, "data", Types.VariantType.get()));

    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(AvroSchemaUtil.convert(schema.asStruct()), Sets.set(1));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }

  @Test
  public void testPruneVariant() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(1, "data", Types.VariantType.get()));
    Schema expected = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    org.apache.avro.Schema prunedSchema =
        AvroSchemaUtil.pruneColumns(AvroSchemaUtil.convert(schema.asStruct()), Sets.set(0));
    assertThat(prunedSchema).isEqualTo(AvroSchemaUtil.convert(expected.asStruct()));
  }
}
