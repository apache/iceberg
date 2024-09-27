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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSinkBranch extends TestFlinkIcebergSinkBase {
  @RegisterExtension
  public static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @Parameter(index = 0)
  private String formatVersion;

  @Parameter(index = 1)
  private String branch;

  @Parameter(index = 2)
  private boolean useV2Sink;

  private TableLoader tableLoader;

  @Parameters(name = "formatVersion = {0}, branch = {1}, useV2Sink = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"1", "main", false},
      {"1", "testBranch", false},
      {"2", "main", false},
      {"2", "testBranch", false},
      {"1", "main", true},
      {"1", "testBranch", true},
      {"2", "main", true},
      {"2", "testBranch", true}
    };
  }

  @BeforeEach
  public void before() throws IOException {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.AVRO.name(),
                    TableProperties.FORMAT_VERSION,
                    formatVersion));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100);

    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @TestTemplate
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE);
    verifyOtherBranchUnmodified();
  }

  private void testWriteRow(TableSchema tableSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    BaseIcebergSinkBuilder.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA, useV2Sink)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .toBranch(branch)
        .distributionMode(distributionMode)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows), branch);
    SimpleDataUtil.assertTableRows(
        table,
        ImmutableList.of(),
        branch.equals(SnapshotRef.MAIN_BRANCH) ? "test-branch" : SnapshotRef.MAIN_BRANCH);

    verifyOtherBranchUnmodified();
  }

  private void verifyOtherBranchUnmodified() {
    String otherBranch =
        branch.equals(SnapshotRef.MAIN_BRANCH) ? "test-branch" : SnapshotRef.MAIN_BRANCH;
    if (otherBranch.equals(SnapshotRef.MAIN_BRANCH)) {
      assertThat(table.currentSnapshot()).isNull();
    }

    assertThat(table.snapshot(otherBranch)).isNull();
  }
}
