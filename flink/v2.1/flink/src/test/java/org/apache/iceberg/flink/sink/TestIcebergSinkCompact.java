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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.api.LockConfig;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFilesConfig;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIcebergSinkCompact extends TestFlinkIcebergSinkBase {

  private Map<String, String> flinkConf;

  @BeforeEach
  void before() throws IOException {
    this.flinkConf = Maps.newHashMap();
    flinkConf.put(FlinkWriteOptions.COMPACTION_ENABLE.key(), "true");
    flinkConf.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.JdbcLockConfig.JDBC);
    flinkConf.put(
        LockConfig.JdbcLockConfig.JDBC_URI_OPTION.key(),
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));
    flinkConf.put(LockConfig.LOCK_ID_OPTION.key(), "test-lock-id");
    flinkConf.put(RewriteDataFilesConfig.SCHEDULE_ON_DATA_FILE_SIZE, "1");

    flinkConf.put(LockConfig.JdbcLockConfig.JDBC_INIT_LOCK_TABLE_OPTION.key(), "true");
    flinkConf.put(RewriteDataFilesConfig.PREFIX + SizeBasedFileRewritePlanner.REWRITE_ALL, "true");

    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                Maps.newHashMap());

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100);

    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @Test
  public void testCompactFileE2e() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    IcebergSink.forRowData(dataStream)
        .setAll(flinkConf)
        .table(table)
        .tableLoader(tableLoader)
        .append();

    env.execute("Test Iceberg Compaction DataStream");

    table.refresh();
    // check the data file count after compact
    List<DataFile> afterCompactDataFiles = getDataFiles(table.currentSnapshot(), table);
    assertThat(afterCompactDataFiles).hasSize(1);

    // check the data file count before compact
    List<DataFile> preCompactDataFiles =
        getDataFiles(table.snapshot(table.currentSnapshot().parentId()), table);
    assertThat(preCompactDataFiles).hasSize(3);
  }

  private List<DataFile> getDataFiles(Snapshot snapshot, Table table) throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    for (ManifestFile dataManifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(dataManifest, table.io())) {
        reader.iterator().forEachRemaining(dataFiles::add);
      }
    }

    return dataFiles;
  }

  @Test
  public void testTableMaintenanceOperatorAdded() {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    IcebergSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .setAll(flinkConf)
        .append();

    boolean containRewrite = false;
    StreamGraph streamGraph = env.getStreamGraph();
    for (JobVertex vertex : streamGraph.getJobGraph().getVertices()) {
      if (vertex.getName().contains("Rewrite")) {
        containRewrite = true;
        break;
      }
    }

    assertThat(containRewrite).isTrue();
  }
}
