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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsConfig {

  @Test
  public void testNestedStruct() {
    Schema schema =
        new Schema(
            required(
                11,
                "level1_struct_a",
                Types.StructType.of(
                    required(21, "level2_primitive_i", Types.IntegerType.get()),
                    required(
                        22,
                        "level2_struct_a",
                        Types.StructType.of(
                            optional(31, "level3_primitive_s", Types.StringType.get()))),
                    optional(23, "level2_primitive_b", Types.BooleanType.get()))),
            required(
                12,
                "level1_struct_b",
                Types.StructType.of(
                    required(24, "level2_primitive_i", Types.IntegerType.get()),
                    required(
                        25,
                        "level2_struct_b",
                        Types.StructType.of(
                            optional(32, "level3_primitive_s", Types.StringType.get()))))),
            required(13, "level1_primitive_i", Types.IntegerType.get()));

    assertThat(MetricsConfig.limitFieldIds(schema, 1))
        .as("Should only include top level primitive field")
        .isEqualTo(Set.of(13));
    assertThat(MetricsConfig.limitFieldIds(schema, 2))
        .as("Should include level 2 primitive field before nested struct")
        .isEqualTo(Set.of(13, 21));
    assertThat(MetricsConfig.limitFieldIds(schema, 3))
        .as("Should include all of level 2 primitive fields of struct a before nested struct")
        .isEqualTo(Set.of(13, 21, 23));
    assertThat(MetricsConfig.limitFieldIds(schema, 4))
        .as("Should include all eligible fields in struct a")
        .isEqualTo(Set.of(13, 21, 23, 31));
    assertThat(MetricsConfig.limitFieldIds(schema, 5))
        .as("Should include first primitive field in struct b")
        .isEqualTo(Set.of(13, 21, 23, 31, 24));
    assertThat(MetricsConfig.limitFieldIds(schema, 6))
        .as("Should include all primitive fields")
        .isEqualTo(Set.of(13, 21, 23, 31, 24, 32));
    assertThat(MetricsConfig.limitFieldIds(schema, 7))
        .as("Should return all primitive fields when limit is higher")
        .isEqualTo(Set.of(13, 21, 23, 31, 24, 32));
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

  @Test
  public void testColumnModeAndFieldIdsFromColumnConfig() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "category", Types.StringType.get()));

    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "none");

    MetricsConfig config = MetricsConfig.from(props, schema, null);

    assertThat(config.metricsFieldIds())
        .as("Should track field ids for configured and defaulted columns")
        .containsExactlyInAnyOrder(1, 2, 3);

    assertThat(config.columnMode(1))
        .as("Should return the configured mode for a tracked field id")
        .isEqualTo(MetricsModes.Full.get());
    assertThat(config.columnMode(2)).isEqualTo(MetricsModes.None.get());
    assertThat(config.columnMode(3))
        .as("Defaulted column should use the default mode")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
  }

  @Test
  public void testAllDefaultedColumnsTracked() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "category", Types.StringType.get()));

    MetricsConfig config = MetricsConfig.from(ImmutableMap.of(), schema, null);

    assertThat(config.metricsFieldIds())
        .as("Should track field ids for all columns that use the default mode")
        .containsExactlyInAnyOrder(1, 2, 3);

    assertThat(config.columnMode(1)).isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(2)).isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(3)).isEqualTo(MetricsModes.Truncate.withLength(16));
  }

  @Test
  public void testLimitingMetricsFieldIds() {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.IntegerType.get()),
            required(3, "c", Types.IntegerType.get()),
            required(4, "d", Types.IntegerType.get()),
            required(5, "e", Types.IntegerType.get()));

    Map<String, String> limitedToTwo =
        ImmutableMap.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2");
    MetricsConfig config = MetricsConfig.from(limitedToTwo, schema, null);

    // only the fields within the inferred limit are tracked, matching limitFieldIds
    assertThat(config.metricsFieldIds())
        .as("Should track only the field ids within the inferred column limit")
        .containsExactlyInAnyOrderElementsOf(MetricsConfig.limitFieldIds(schema, 2))
        .containsExactlyInAnyOrder(1, 2);

    // tracked fields keep metrics at the default mode
    assertThat(config.columnMode(1)).isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(2)).isEqualTo(MetricsModes.Truncate.withLength(16));
    // fields past the limit are dropped from both id tracking and metrics
    assertThat(config.columnMode(3))
        .as("Field past the limit should not have metrics")
        .isEqualTo(MetricsModes.None.get());
    assertThat(config.columnMode(4)).isEqualTo(MetricsModes.None.get());
    assertThat(config.columnMode(5)).isEqualTo(MetricsModes.None.get());

    // raising the limit expands both the tracked ids and the columns with metrics
    Map<String, String> limitedToThree =
        ImmutableMap.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "3");
    MetricsConfig wider = MetricsConfig.from(limitedToThree, schema, null);

    assertThat(wider.metricsFieldIds())
        .as("Raising the limit should track more field ids")
        .containsExactlyInAnyOrder(1, 2, 3);
    assertThat(wider.columnMode(3))
        .as("Raising the limit should give the field metrics at the default mode")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(wider.columnMode(4)).isEqualTo(MetricsModes.None.get());
  }
}
