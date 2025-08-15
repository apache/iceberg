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

import java.util.Set;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsConfig {

  @Test
  public void testNestedStruct() {
    Schema schema =
        new Schema(
            required(
                1,
                "top_struct_a1",
                Types.StructType.of(
                    required(2, "middle_b1", Types.IntegerType.get()),
                    required(
                        3,
                        "middle_struct_b1",
                        Types.StructType.of(optional(4, "bottom_c1", Types.StringType.get()))))),
            required(
                5,
                "top_struct_a2",
                Types.StructType.of(
                    required(6, "middle_b2", Types.IntegerType.get()),
                    required(
                        7,
                        "middle_struct_b2",
                        Types.StructType.of(optional(8, "bottom_c2", Types.StringType.get()))))),
            required(9, "top_a", Types.IntegerType.get()));

    assertThat(MetricsConfig.limitFieldIds(schema, 1))
        .as("Should only include top level primitive field")
        .isEqualTo(Set.of(9));
    assertThat(MetricsConfig.limitFieldIds(schema, 2))
        .as("Should include next level primitive field before nested struct")
        .isEqualTo(Set.of(9, 2));
    assertThat(MetricsConfig.limitFieldIds(schema, 3))
        .as("Should include all eligible fields of first struct before evaluate second struct")
        .isEqualTo(Set.of(9, 2, 4));
    assertThat(MetricsConfig.limitFieldIds(schema, 4))
        .as("Should only include middle_b2 when evaluate second struct")
        .isEqualTo(Set.of(9, 2, 4, 6));
    assertThat(MetricsConfig.limitFieldIds(schema, 5))
        .as("Should include all primitive fields")
        .isEqualTo(Set.of(9, 2, 6, 4, 8));
    assertThat(MetricsConfig.limitFieldIds(schema, 6))
        .as("Should return all primitive fields when limit is higher")
        .isEqualTo(Set.of(9, 2, 6, 4, 8));
  }

  @Test
  public void testNestedMap() {
    Schema schema =
        new Schema(
            required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.IntegerType.get(), Types.IntegerType.get())),
            required(4, "top", Types.IntegerType.get()));

    assertThat(MetricsConfig.limitFieldIds(schema, 1)).isEqualTo(Set.of(4));
    assertThat(MetricsConfig.limitFieldIds(schema, 2)).isEqualTo(Set.of(4, 2));
    assertThat(MetricsConfig.limitFieldIds(schema, 3)).isEqualTo(Set.of(4, 2, 3));
    assertThat(MetricsConfig.limitFieldIds(schema, 4)).isEqualTo(Set.of(4, 2, 3));
  }

  @Test
  public void testNestedListOfMaps() {
    Schema schema =
        new Schema(
            required(
                1,
                "array_of_maps",
                Types.ListType.ofRequired(
                    2,
                    Types.MapType.ofRequired(
                        3, 4, Types.IntegerType.get(), Types.IntegerType.get()))),
            required(5, "top", Types.IntegerType.get()));

    assertThat(MetricsConfig.limitFieldIds(schema, 1)).isEqualTo(Set.of(5));
    assertThat(MetricsConfig.limitFieldIds(schema, 2)).isEqualTo(Set.of(5, 3));
    assertThat(MetricsConfig.limitFieldIds(schema, 3)).isEqualTo(Set.of(5, 3, 4));
    assertThat(MetricsConfig.limitFieldIds(schema, 4)).isEqualTo(Set.of(5, 3, 4));
  }
}
