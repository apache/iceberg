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

import static org.apache.iceberg.TestHelpers.ALL_VERSIONS;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetricsModes.Counts;
import org.apache.iceberg.MetricsModes.Full;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.MetricsModes.Truncate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetricsModes {
  @TempDir private File tableDir;

  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return ALL_VERSIONS;
  }

  @TempDir private Path temp;

  @AfterEach
  public void after() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testMetricsModeParsing() {
    assertThat(MetricsModes.fromString("none")).isEqualTo(None.get());
    assertThat(MetricsModes.fromString("nOnE")).isEqualTo(None.get());
    assertThat(MetricsModes.fromString("counts")).isEqualTo(Counts.get());
    assertThat(MetricsModes.fromString("coUntS")).isEqualTo(Counts.get());
    assertThat(MetricsModes.fromString("truncate(1)")).isEqualTo(Truncate.withLength(1));
    assertThat(MetricsModes.fromString("truNcAte(10)")).isEqualTo(Truncate.withLength(10));
    assertThat(MetricsModes.fromString("full")).isEqualTo(Full.get());
    assertThat(MetricsModes.fromString("FULL")).isEqualTo(Full.get());
  }

  @TestTemplate
  public void testInvalidTruncationLength() {
    assertThatThrownBy(() -> MetricsModes.fromString("truncate(0)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncate length should be positive");
  }

  @TestTemplate
  public void testInvalidColumnModeValue() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "full",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col",
            "troncate(5)");

    MetricsConfig config = MetricsConfig.fromProperties(properties);
    assertThat(config.columnMode("col"))
        .as("Invalid mode should be defaulted to table default (full)")
        .isEqualTo(MetricsModes.Full.get());
  }

  @TestTemplate
  public void testInvalidDefaultColumnModeValue() {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            "fuull",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col",
            "troncate(5)");

    MetricsConfig config = MetricsConfig.fromProperties(properties);
    assertThat(config.columnMode("col"))
        .as("Invalid mode should be defaulted to library default (truncate(16))")
        .isEqualTo(MetricsModes.Truncate.withLength(16));
  }

  @TestTemplate
  public void testMetricsConfigSortedColsDefault() throws Exception {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()),
            required(4, "col4", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();
    Table testTable =
        TestTables.create(
            tableDir, "test", schema, PartitionSpec.unpartitioned(), sortOrder, formatVersion);
    testTable
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "counts")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "none")
        .commit();

    MetricsConfig config = MetricsConfig.forTable(testTable);
    assertThat(config.columnMode("col1"))
        .as("Non-sorted existing column should not be overridden")
        .isEqualTo(Counts.get());
    assertThat(config.columnMode("col2"))
        .as("Sorted column defaults should not override user specified config")
        .isEqualTo(None.get());
    assertThat(config.columnMode("col3"))
        .as("Unspecified sorted column should use default")
        .isEqualTo(Truncate.withLength(16));
    assertThat(config.columnMode("col4"))
        .as("Unspecified normal column should use default")
        .isEqualTo(Counts.get());
  }

  @TestTemplate
  public void testMetricsConfigSortedColsDefaultByInvalid() throws Exception {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("col2").asc("col3").build();
    Table testTable =
        TestTables.create(
            tableDir, "test", schema, PartitionSpec.unpartitioned(), sortOrder, formatVersion);
    testTable
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col1", "full")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "col2", "invalid")
        .commit();

    MetricsConfig config = MetricsConfig.forTable(testTable);
    assertThat(config.columnMode("col1"))
        .as("Non-sorted existing column should not be overridden by sorted column")
        .isEqualTo(Full.get());
    assertThat(config.columnMode("col2"))
        .as("Original default applies as user entered invalid mode for sorted column")
        .isEqualTo(Counts.get());
  }

  @TestTemplate
  public void testMetricsConfigInferredDefaultModeLimit() throws IOException {
    Schema schema =
        new Schema(
            required(1, "col1", Types.IntegerType.get()),
            required(2, "col2", Types.IntegerType.get()),
            required(3, "col3", Types.IntegerType.get()));

    Table table =
        TestTables.create(
            tableDir,
            "test",
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            formatVersion);

    // only infer a default for the first two columns
    table
        .updateProperties()
        .set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2")
        .commit();

    MetricsConfig config = MetricsConfig.forTable(table);

    assertThat(config.columnMode("col1")).isEqualTo(Truncate.withLength(16));
    assertThat(config.columnMode("col2")).isEqualTo(Truncate.withLength(16));
    assertThat(config.columnMode("col3")).isEqualTo(None.get());
  }

  @TestTemplate
  public void testMetricsVariantSupported() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    Schema schema =
        new Schema(
            required(1, "variant", Types.VariantType.get()),
            required(2, "int", Types.IntegerType.get()));

    Table table =
        TestTables.create(
            tableDir,
            "test",
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            formatVersion);

    // only infer a default for the first column
    table
        .updateProperties()
        .set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1")
        .commit();

    MetricsConfig config = MetricsConfig.forTable(table);

    Map<String, MetricsModes.MetricsMode> metricModes =
        schema.idToName().values().stream().collect(Collectors.toMap(k -> k, config::columnMode));

    assertThat(metricModes)
        .containsOnly(Map.entry("variant", Truncate.withLength(16)), Map.entry("int", None.get()));
  }

  @TestTemplate
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

    Table table =
        TestTables.create(
            tableDir,
            "test",
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            formatVersion);

    // only infer a default for the first two columns
    table
        .updateProperties()
        .set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "2")
        .commit();

    MetricsConfig config = MetricsConfig.forTable(table);

    Map<String, MetricsModes.MetricsMode> metricModes =
        schema.idToName().values().stream().collect(Collectors.toMap(k -> k, config::columnMode));

    assertThat(metricModes).containsOnlyKeys("col_struct.a", "col_struct", "col_struct.b", "top");

    assertThat(metricModes).containsEntry("col_struct.a", Truncate.withLength(16));
    assertThat(metricModes).containsEntry("col_struct.b", None.get());
    assertThat(metricModes).containsEntry("top", Truncate.withLength(16));
  }
}
