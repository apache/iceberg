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

import java.io.IOException;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSinkBranch extends TestFlinkIcebergSinkBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE);

  private final String branch;
  private TableLoader tableLoader;

  @Parameterized.Parameters(name = "formatVersion = {0}, branch = {1}")
  public static Object[] parameters() {
    return new Object[] {"main", "testBranch"};
  }

  public TestFlinkIcebergSinkBranch(String branch) {
    this.branch = branch;
  }

  @Before
  public void before() throws IOException {
    table =
        catalogResource
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.AVRO.name(),
                    TableProperties.FORMAT_VERSION,
                    "1"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100);

    tableLoader = catalogResource.tableLoader();
  }

  @Test
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE);
    verifyOtherBranchUnmodified();
  }

  private void testWriteRow(TableSchema tableSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
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
      Assert.assertNull(table.currentSnapshot());
    }

    Assert.assertTrue(table.snapshot(otherBranch) == null);
  }
}
