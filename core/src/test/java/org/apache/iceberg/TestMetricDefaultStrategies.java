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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Set;
import org.apache.iceberg.MetricsConfig.OriginalMetricsMaxInferredColumnDefaultsStrategy;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetricDefaultStrategies {

  @Test
  public void testOrignalStategyNestedStructsNotRespectedInLimit() {
    OriginalMetricsMaxInferredColumnDefaultsStrategy originalStrategy =
        new OriginalMetricsMaxInferredColumnDefaultsStrategy();

    Schema schema =
        new Schema(
            required(
                1,
                "col_struct",
                Types.StructType.of(
                    required(2, "a", Types.IntegerType.get()),
                    required(3, "b", Types.IntegerType.get()))),
            required(4, "top", Types.IntegerType.get()));

    Schema subSchema = originalStrategy.subSchemaMetricPriority(schema, 1);

    Assertions.assertThat(subSchema.sameSchema(TypeUtil.project(schema, Set.of(1, 2, 3)))).isTrue();
  }

  @Test
  public void testBreadthStategyNestedStructsRespectedInLimit() {
    MetricsConfig.BreadthFirstFieldPriority originalStrategy =
        new MetricsConfig.BreadthFirstFieldPriority();

    Schema schema =
        new Schema(
            required(
                1,
                "col_struct",
                Types.StructType.of(
                    required(2, "a", Types.IntegerType.get()),
                    required(3, "b", Types.IntegerType.get()))),
            required(4, "top", Types.IntegerType.get()));

    Schema subSchema = originalStrategy.subSchemaMetricPriority(schema, 1);

    Assertions.assertThat(subSchema.sameSchema(TypeUtil.project(schema, Set.of(4)))).isTrue();
  }

  @Test
  public void testBreadthFirstStrategyWithNestedMap() {
    MetricsConfig.BreadthFirstFieldPriority originalStrategy =
        new MetricsConfig.BreadthFirstFieldPriority();

    Schema schema =
        new Schema(
            required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.IntegerType.get(), Types.IntegerType.get())),
            required(4, "top", Types.IntegerType.get()));

    Schema subSchema = originalStrategy.subSchemaMetricPriority(schema, 2);

    Assertions.assertThat(subSchema.sameSchema(TypeUtil.project(schema, Set.of(4, 2)))).isTrue();
  }

  @Test
  public void testBreadFirstStrategyWithNestedListOfMaps() {
    MetricsConfig.BreadthFirstFieldPriority originalStrategy =
        new MetricsConfig.BreadthFirstFieldPriority();

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

    Schema subSchema = originalStrategy.subSchemaMetricPriority(schema, 2);

    Assertions.assertThat(subSchema.sameSchema(TypeUtil.project(schema, Set.of(5, 3)))).isTrue();
  }

  //TODO test depth strategies if we even decide to include that in the final change.
}
