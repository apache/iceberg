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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * This tests the distribution mode of Flink sink. Extract them separately since it is unnecessary
 * to test different file formats (Avro, Orc, Parquet) like in {@link TestFlinkIcebergSink}.
 * Removing the file format dimension reduces the number of combinations from 12 to 4, which helps
 * reduce test run time.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSinkDistributionMode extends TestFlinkIcebergSinkBase {

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private final FileFormat format = FileFormat.PARQUET;

  @Parameter(index = 0)
  private int parallelism;

  @Parameter(index = 1)
  private boolean partitioned;

  @Parameters(name = "parallelism = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {1, true},
      {1, false},
      {2, true},
      {2, false}
    };
  }

  @BeforeEach
  public void before() throws IOException {
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    this.env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    this.tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @TestTemplate
  public void testShuffleByPartitionWithSchema() throws Exception {
    testWriteRow(parallelism, SimpleDataUtil.FLINK_SCHEMA, DistributionMode.HASH);
    if (partitioned) {
      assertThat(partitionFiles("aaa")).isEqualTo(1);
      assertThat(partitionFiles("bbb")).isEqualTo(1);
      assertThat(partitionFiles("ccc")).isEqualTo(1);
    }
  }

  @TestTemplate
  public void testJobNoneDistributeMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(parallelism, null, DistributionMode.NONE);

    if (parallelism > 1) {
      if (partitioned) {
        int files = partitionFiles("aaa") + partitionFiles("bbb") + partitionFiles("ccc");
        assertThat(files).isGreaterThan(3);
      }
    }
  }

  @TestTemplate
  public void testJobNullDistributionMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(parallelism, null, null);

    if (partitioned) {
      assertThat(partitionFiles("aaa")).isEqualTo(1);
      assertThat(partitionFiles("bbb")).isEqualTo(1);
      assertThat(partitionFiles("ccc")).isEqualTo(1);
    }
  }

  @TestTemplate
  public void testPartitionWriteMode() throws Exception {
    testWriteRow(parallelism, null, DistributionMode.HASH);
    if (partitioned) {
      assertThat(partitionFiles("aaa")).isEqualTo(1);
      assertThat(partitionFiles("bbb")).isEqualTo(1);
      assertThat(partitionFiles("ccc")).isEqualTo(1);
    }
  }

  @TestTemplate
  public void testOverrideWriteConfigWithUnknownDistributionMode() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid distribution mode: UNRECOGNIZED");
  }
}
