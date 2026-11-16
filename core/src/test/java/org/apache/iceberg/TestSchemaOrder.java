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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestSchemaOrder {
  @Test
  public void exampleStructOrder() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(
                3,
                "point",
                Types.StructType.of(
                    Types.NestedField.required(4, "x", Types.FloatType.get()),
                    Types.NestedField.required(5, "y", Types.FloatType.get()))));

    assertThat(SchemaOrder.allFieldIds(schema)).containsExactly(1, 2, 3, 4, 5);
    assertThat(SchemaOrder.leafFieldIds(schema)).containsExactly(1, 2, 4, 5);
  }

  @Test
  public void listOrder() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2, "numbers", Types.ListType.ofRequired(3, Types.DoubleType.get())),
            Types.NestedField.optional(4, "data", Types.StringType.get()));

    assertThat(SchemaOrder.allFieldIds(schema)).containsExactly(1, 2, 3, 4);
    assertThat(SchemaOrder.leafFieldIds(schema)).containsExactly(1, 3, 4);
  }

  @Test
  public void mapOrder() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "properties",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get())),
            Types.NestedField.optional(5, "data", Types.StringType.get()));

    assertThat(SchemaOrder.allFieldIds(schema)).containsExactly(1, 2, 3, 4, 5);
    assertThat(SchemaOrder.leafFieldIds(schema)).containsExactly(1, 3, 4, 5);
  }

  @Test
  public void mixedTypeOrder() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(4, "feature1", Types.BooleanType.get()),
                    optional(5, "feature2", Types.BooleanType.get()))),
            required(
                6,
                "locations",
                Types.MapType.ofRequired(
                    7,
                    8, // Map value ID comes after key fields (between 12 and 13)
                    Types.StructType.of(
                        required(9, "address", Types.StringType.get()),
                        required(10, "city", Types.StringType.get()),
                        required(11, "state", Types.StringType.get()),
                        required(12, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(13, "lat", Types.FloatType.get()),
                        required(14, "long", Types.FloatType.get())))),
            optional(
                15,
                "points",
                Types.ListType.ofOptional(
                    16,
                    Types.StructType.of(
                        required(17, "x", Types.LongType.get()),
                        required(18, "y", Types.LongType.get())))),
            required(19, "numbers", Types.ListType.ofRequired(20, Types.DoubleType.get())),
            optional(
                21,
                "properties",
                Types.MapType.ofOptional(22, 23, Types.StringType.get(), Types.StringType.get())),
            optional(24, "variant", Types.VariantType.get()));

    assertThat(SchemaOrder.allFieldIds(schema))
        .containsExactly(
            1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 8, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24);
    assertThat(SchemaOrder.leafFieldIds(schema))
        .containsExactly(1, 2, 4, 5, 9, 10, 11, 12, 13, 14, 17, 18, 20, 22, 23, 24);
  }
}
