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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsConfig {

  private static final int ID = 1;
  private static final int EVENT_TIME = 2;
  private static final int CATEGORY = 3;
  private static final int DATA = 4;

  private static final Schema SCHEMA =
      new Schema(
          required(ID, "id", Types.IntegerType.get()),
          optional(EVENT_TIME, "event_time", Types.TimestampType.withoutZone()),
          optional(CATEGORY, "category", Types.StringType.get()),
          optional(DATA, "data", Types.StringType.get()));

  @Test
  public void testInvalidColumnModeValue() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col",
            "troncate(5)");

    Schema schema = new Schema(required(1, "col", Types.StringType.get()));

    MetricsConfig config = MetricsTestUtil.from(properties, schema);
    assertThat(config.columnMode(1))
        .as("Invalid mode should be defaulted to table default (full)")
        .isEqualTo(MetricsModes.Full.get());

    assertThat(config.metricsFieldIds()).containsExactly(1);
  }

  @Test
  public void testInvalidDefaultColumnModeValue() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "fuull",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col",
            "troncate(5)");

    Schema schema = new Schema(required(1, "col", Types.StringType.get()));

    MetricsConfig config = MetricsTestUtil.from(properties, schema);
    assertThat(config.columnMode(1))
        .as("Invalid mode should be defaulted to library default (truncate(16))")
        .isEqualTo(MetricsModes.Truncate.withLength(16));

    assertThat(config.metricsFieldIds()).containsExactly(1);
  }

  @Test
  public void testMetricsConfigSortedColsDefault() {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()),
            required(4, "col4", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();
    Map<String, String> properties =
        Map.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "counts",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1",
            "counts",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2",
            "none");

    MetricsConfig config = MetricsTestUtil.from(properties, schema, sortOrder);
    assertThat(config.columnMode(1))
        .as("Non-sorted existing column should not be overridden")
        .isEqualTo(MetricsModes.Counts.get());
    assertThat(config.columnMode(2))
        .as("Sorted column defaults should not override user specified config")
        .isEqualTo(MetricsModes.None.get());
    assertThat(config.columnMode(3))
        .as("Unspecified sorted column should use default")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(4))
        .as("Unspecified normal column should use default")
        .isEqualTo(MetricsModes.Counts.get());

    assertThat(config.metricsFieldIds()).containsExactly(1, 2, 3, 4);
  }

  @Test
  public void testMetricsConfigSortedColsDefaultByInvalid() {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();
    Map<String, String> properties =
        Map.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "counts",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1",
            "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2",
            "invalid");

    MetricsConfig config = MetricsTestUtil.from(properties, schema, sortOrder);
    assertThat(config.columnMode(1))
        .as("Non-sorted existing column should not be overridden by sorted column")
        .isEqualTo(MetricsModes.Full.get());
    assertThat(config.columnMode(2))
        .as("Original default applies as user entered invalid mode for sorted column")
        .isEqualTo(MetricsModes.Truncate.withLength(16));

    assertThat(config.metricsFieldIds()).containsExactly(1, 2, 3);
  }

  @Test
  public void testMetricsConfigInferredDefaultModeLimit() {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()));

    // only infer a default for the first two columns
    Map<String, String> properties =
        Map.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2");

    MetricsConfig config = MetricsTestUtil.from(properties, schema);

    assertThat(config.columnMode(1)).isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(2)).isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(3)).isEqualTo(MetricsModes.None.get());

    assertThat(config.metricsFieldIds()).containsExactly(1, 2);
  }

  @Test
  public void testMetricsVariantSupported() {
    Schema schema =
        new Schema(
            required(1, "variant", Types.VariantType.get()),
            required(2, "int", Types.IntegerType.get()));

    // only infer a default for the first column
    Map<String, String> properties =
        Map.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1");

    MetricsConfig config = MetricsTestUtil.from(properties, schema);

    Map<Integer, MetricsModes.MetricsMode> metricModes =
        schema.idToName().keySet().stream().collect(Collectors.toMap(id -> id, config::columnMode));

    assertThat(metricModes)
        .containsOnly(
            Map.entry(1, MetricsModes.Truncate.withLength(16)),
            Map.entry(2, MetricsModes.None.get()));

    assertThat(config.metricsFieldIds()).containsExactly(1);
  }

  @Test
  public void testMetricsConfigNestedTypesStructs() {
    Schema schema =
        new Schema(
            required(
                5,
                "col_struct",
                Types.StructType.of(
                    required(33, "a", Types.IntegerType.get()),
                    required(1, "b", Types.IntegerType.get()))),
            required(4, "top", Types.IntegerType.get()));

    Map<String, String> properties =
        Map.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2");

    MetricsConfig config = MetricsTestUtil.from(properties, schema);

    Map<Integer, MetricsModes.MetricsMode> metricModes =
        schema.idToName().keySet().stream().collect(Collectors.toMap(id -> id, config::columnMode));

    assertThat(metricModes).containsOnlyKeys(33, 5, 1, 4);

    assertThat(metricModes).containsEntry(33, MetricsModes.Truncate.withLength(16));
    assertThat(metricModes).containsEntry(1, MetricsModes.None.get());
    assertThat(metricModes).containsEntry(4, MetricsModes.Truncate.withLength(16));

    assertThat(config.metricsFieldIds()).containsExactly(33, 4);
  }

  @Test
  void configCannotDisablePartitionSourceMetrics() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    Map<String, String> props =
        ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "category", "none");
    MetricsConfig config = MetricsTestUtil.from(props, SCHEMA, SortOrder.unsorted(), spec);

    assertThat(config.columnMode(CATEGORY))
        .as("column config should not be able to disable metrics for a partition source column")
        .isEqualTo(MetricsModes.Full.get());
  }

  @Test
  void bucketPartitionColumnIgnored() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("category", 4).build();
    MetricsConfig config =
        MetricsTestUtil.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted(), spec);

    assertThat(config.columnMode(CATEGORY))
        .as("non-order-preserving partition transform should not promote its source column")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
  }

  @Test
  void truncatePartitionColumnFullMetrics() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate("category", 4).build();
    MetricsConfig config =
        MetricsTestUtil.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted(), spec);

    assertThat(config.columnMode(CATEGORY))
        .as("truncate partition source column should get full metrics")
        .isEqualTo(MetricsModes.Full.get());
  }

  @Test
  void multiplePartitionFieldsForSameSourceColumn() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("category").truncate("category", 4).build();
    MetricsConfig config =
        MetricsTestUtil.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted(), spec);

    assertThat(config.columnMode(CATEGORY))
        .as("source column of multiple partition fields should get full metrics")
        .isEqualTo(MetricsModes.Full.get());
  }

  @Test
  void columnModeAndFieldIdsFromPartitionSpec() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).day("event_time").identity("category").build();
    MetricsConfig config =
        MetricsTestUtil.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted(), spec);

    assertThat(config.metricsFieldIds())
        .as("Should track field ids for partition and non-partition columns")
        .containsExactlyInAnyOrder(ID, EVENT_TIME, CATEGORY, DATA);

    assertThat(config.columnMode(EVENT_TIME))
        .as("day partition source column should get full metrics")
        .isEqualTo(MetricsModes.Full.get());
    assertThat(config.columnMode(CATEGORY))
        .as("identity partition source column should get full metrics")
        .isEqualTo(MetricsModes.Full.get());
    assertThat(config.columnMode(ID))
        .as("non-partition column should keep the default mode")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(config.columnMode(DATA))
        .as("non-partition column should keep the default mode")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  void metricsFieldIdsCannotBeModified() {
    MetricsConfig config = MetricsTestUtil.from(ImmutableMap.of(), SCHEMA);
    Iterator<Integer> fieldIds = config.metricsFieldIds().iterator();

    assertThat(fieldIds.next()).isEqualTo(ID);
    assertThatThrownBy(fieldIds::remove).isInstanceOf(UnsupportedOperationException.class);
    assertThat(config.metricsFieldIds()).containsExactly(ID, EVENT_TIME, CATEGORY, DATA);
  }

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

    MetricsConfig config = MetricsTestUtil.from(props, schema);

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

    MetricsConfig config = MetricsTestUtil.from(ImmutableMap.of(), schema);

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
    MetricsConfig config = MetricsTestUtil.from(limitedToTwo, schema);

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
    MetricsConfig wider = MetricsTestUtil.from(limitedToThree, schema);

    assertThat(wider.metricsFieldIds())
        .as("Raising the limit should track more field ids")
        .containsExactlyInAnyOrder(1, 2, 3);
    assertThat(wider.columnMode(3))
        .as("Raising the limit should give the field metrics at the default mode")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
    assertThat(wider.columnMode(4)).isEqualTo(MetricsModes.None.get());
  }

  @Test
  public void testMetricsConfigKryoSerialization() throws Exception {
    Map<String, String> metricsConfig =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "counts",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1",
            "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2",
            "truncate(16)");

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "col1", Types.IntegerType.get()),
            Types.NestedField.optional(2, "col2", Types.StringType.get()),
            Types.NestedField.optional(3, "col3", Types.StringType.get()));

    MetricsConfig config = MetricsTestUtil.from(metricsConfig, schema);
    MetricsConfig deserialized = TestHelpers.KryoHelpers.roundTripSerialize(config);

    assertThat(deserialized.columnMode(1)).asString().isEqualTo(MetricsModes.Full.get().toString());
    assertThat(deserialized.columnMode(2))
        .asString()
        .isEqualTo(MetricsModes.Truncate.withLength(16).toString());
    assertThat(deserialized.columnMode(3))
        .asString()
        .isEqualTo(MetricsModes.Counts.get().toString());

    assertThat(deserialized.metricsFieldIds()).containsExactlyElementsOf(config.metricsFieldIds());
  }

  @Test
  public void testMetricsConfigJavaSerialization() throws Exception {
    Map<String, String> metricsConfig =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "counts",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1",
            "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2",
            "truncate(16)");

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "col1", Types.IntegerType.get()),
            Types.NestedField.optional(2, "col2", Types.StringType.get()),
            Types.NestedField.optional(3, "col3", Types.StringType.get()));

    MetricsConfig config = MetricsTestUtil.from(metricsConfig, schema);
    MetricsConfig deserialized = TestHelpers.roundTripSerialize(config);

    assertThat(deserialized.columnMode(1)).asString().isEqualTo(MetricsModes.Full.get().toString());
    assertThat(deserialized.columnMode(2))
        .asString()
        .isEqualTo(MetricsModes.Truncate.withLength(16).toString());
    assertThat(deserialized.columnMode(3))
        .asString()
        .isEqualTo(MetricsModes.Counts.get().toString());

    assertThat(deserialized.metricsFieldIds()).containsExactlyElementsOf(config.metricsFieldIds());
  }
}
