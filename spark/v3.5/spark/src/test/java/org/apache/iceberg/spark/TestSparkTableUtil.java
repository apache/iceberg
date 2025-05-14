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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.KryoHelpers;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkTableUtil {

  @TempDir private File tableDir;

  @Parameters(name = "formatVersion = {0}")
  public static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @Parameter private int formatVersion;

  @AfterEach
  public void after() {
    TestTables.clearTables();
  }

  @Test
  public void testSparkPartitionOKryoSerialization() throws IOException {
    Map<String, String> values = ImmutableMap.of("id", "2");
    String uri = "s3://bucket/table/data/id=2";
    String format = "parquet";
    SparkPartition sparkPartition = new SparkPartition(values, uri, format);

    SparkPartition deserialized = KryoHelpers.roundTripSerialize(sparkPartition);
    assertThat(sparkPartition).isEqualTo(deserialized);
  }

  @Test
  public void testSparkPartitionJavaSerialization() throws IOException, ClassNotFoundException {
    Map<String, String> values = ImmutableMap.of("id", "2");
    String uri = "s3://bucket/table/data/id=2";
    String format = "parquet";
    SparkPartition sparkPartition = new SparkPartition(values, uri, format);

    SparkPartition deserialized = TestHelpers.roundTripSerialize(sparkPartition);
    assertThat(sparkPartition).isEqualTo(deserialized);
  }

  @TestTemplate
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
            Types.NestedField.required(1, "col1", Types.StringType.get()),
            Types.NestedField.required(2, "col2", Types.StringType.get()),
            Types.NestedField.required(3, "col3", Types.StringType.get()));
    Table testTable =
        TestTables.create(
            tableDir,
            "test_table",
            schema,
            PartitionSpec.unpartitioned(),
            formatVersion,
            metricsConfig);

    MetricsConfig config = MetricsConfig.forTable(testTable);
    MetricsConfig deserialized = KryoHelpers.roundTripSerialize(config);

    assertThat(deserialized.columnMode("col1"))
        .asString()
        .isEqualTo(MetricsModes.Full.get().toString());
    assertThat(deserialized.columnMode("col2"))
        .asString()
        .isEqualTo(MetricsModes.Truncate.withLength(16).toString());
    assertThat(deserialized.columnMode("col3"))
        .asString()
        .isEqualTo(MetricsModes.Counts.get().toString());
  }

  @TestTemplate
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
            Types.NestedField.required(1, "col1", Types.StringType.get()),
            Types.NestedField.required(2, "col2", Types.StringType.get()),
            Types.NestedField.required(3, "col3", Types.StringType.get()));
    Table testTable =
        TestTables.create(
            tableDir,
            "test_table",
            schema,
            PartitionSpec.unpartitioned(),
            formatVersion,
            metricsConfig);

    MetricsConfig config = MetricsConfig.forTable(testTable);
    MetricsConfig deserialized = TestHelpers.roundTripSerialize(config);

    assertThat(deserialized.columnMode("col1"))
        .asString()
        .isEqualTo(MetricsModes.Full.get().toString());
    assertThat(deserialized.columnMode("col2"))
        .asString()
        .isEqualTo(MetricsModes.Truncate.withLength(16).toString());
    assertThat(deserialized.columnMode("col3"))
        .asString()
        .isEqualTo(MetricsModes.Counts.get().toString());
  }
}
