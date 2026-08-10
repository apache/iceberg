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
package org.apache.iceberg.flink;

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageEdge;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * End-to-end test that a FlinkSQL job reports the source→sink edge for Iceberg tables to a {@link
 * JobStatusChangedListener}, and that the coordinates survive the Table planner.
 *
 * <p>The source half needs no configuration. The sink half needs {@code
 * table.exec.iceberg.use-v2-sink=true} — the only sink that reports lineage — and {@code
 * table.exec.uid.generation=ALWAYS}, for the reason given in {@code
 * IcebergTableSink#canProvideSinkV2}. A deployment that wants sink lineage has to set both.
 */
public class TestIcebergSqlLineage {

  private static final String CATALOG = "lineage_catalog";
  private static final String DATABASE = "lineage_db";
  private static final String SOURCE_TABLE = "src";
  private static final String SINK_TABLE = "dst";

  /** Static because Flink instantiates the listener reflectively, from a class name. */
  private static final List<LineageGraph> CAPTURED_GRAPHS = new CopyOnWriteArrayList<>();

  public static class CapturingListenerFactory implements JobStatusChangedListenerFactory {
    @Override
    public JobStatusChangedListener createListener(Context context) {
      return TestIcebergSqlLineage::capture;
    }
  }

  private static void capture(JobStatusChangedEvent event) {
    if (event instanceof JobCreatedEvent) {
      CAPTURED_GRAPHS.add(((JobCreatedEvent) event).lineageGraph());
    }
  }

  /**
   * The listener must be registered on the cluster config, not the job: {@code MiniClusterExecutor}
   * builds its listeners once, in its constructor, from {@code MiniCluster#getConfiguration()}.
   * Setting it on the {@link StreamExecutionEnvironment} instead captures nothing, silently.
   */
  @RegisterExtension
  private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(1)
              .setConfiguration(
                  new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG)
                      .set(
                          DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS,
                          ImmutableList.of(CapturingListenerFactory.class.getName())))
              .build());

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, SOURCE_TABLE);

  private StreamTableEnvironment tableEnv;

  @BeforeEach
  public void before() {
    CAPTURED_GRAPHS.clear();

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
            new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG));
    env.setParallelism(1);
    this.tableEnv = StreamTableEnvironment.create(env);
    tableEnv.getConfig().set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true);
    tableEnv
        .getConfig()
        .set(
            ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION,
            ExecutionConfigOptions.UidGeneration.ALWAYS);

    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
            CATALOG, CATALOG_EXTENSION.warehouse()));
    tableEnv.executeSql(String.format("USE CATALOG %s", CATALOG));
    tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE));
    tableEnv.executeSql(String.format("USE %s", DATABASE));
    tableEnv.executeSql(String.format("CREATE TABLE %s (id INT, data STRING)", SOURCE_TABLE));
    tableEnv.executeSql(String.format("CREATE TABLE %s (id INT, data STRING)", SINK_TABLE));
  }

  @Test
  public void reportsTheSourceToSinkEdgeForAnInsertSelect() throws Exception {
    tableEnv
        .executeSql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", SOURCE_TABLE))
        .await();
    CAPTURED_GRAPHS.clear();

    tableEnv
        .executeSql(String.format("INSERT INTO %s SELECT * FROM %s", SINK_TABLE, SOURCE_TABLE))
        .await();

    assertThat(CAPTURED_GRAPHS).hasSize(1);
    LineageGraph graph = CAPTURED_GRAPHS.get(0);

    assertThat(graph.sources()).hasSize(1);
    assertThat(graph.sinks()).hasSize(1);
    assertThat(graph.relations()).hasSize(1);

    LineageEdge edge = graph.relations().get(0);
    assertThat(icebergCoordinates(edge.source()))
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG)
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DATABASE)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, SOURCE_TABLE);
    assertThat(icebergCoordinates(edge.sink()))
        .containsEntry(IcebergLineageUtil.CONFIG_CATALOG, CATALOG)
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DATABASE)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, SINK_TABLE);
  }

  @Test
  public void icebergCoordinatesSurviveThePlannerOverwritingTheDatasetName() throws Exception {
    tableEnv
        .executeSql(String.format("INSERT INTO %s SELECT * FROM %s", SINK_TABLE, SOURCE_TABLE))
        .await();

    // The planner wraps the connector's dataset in TableLineageDatasetImpl, which overwrites
    // name() but leaves namespace() and facets() alone — hence coordinates go in the facet.
    LineageVertex sinkVertex = CAPTURED_GRAPHS.get(0).sinks().get(0);
    LineageDataset dataset = sinkVertex.datasets().get(0);

    assertThat(dataset.namespace()).isEqualTo(CATALOG_EXTENSION.warehouse());
    assertThat(icebergCoordinates(sinkVertex))
        .containsEntry(IcebergLineageUtil.CONFIG_NAMESPACE, DATABASE)
        .containsEntry(IcebergLineageUtil.CONFIG_TABLE, SINK_TABLE);
  }

  /** The config of the {@code iceberg} facet on the vertex's single dataset. */
  private static Map<String, String> icebergCoordinates(LineageVertex vertex) {
    assertThat(vertex.datasets()).hasSize(1);
    LineageDatasetFacet facet =
        vertex.datasets().get(0).facets().get(IcebergLineageUtil.FACET_NAME);
    assertThat(facet).isInstanceOf(DatasetConfigFacet.class);
    return ((DatasetConfigFacet) facet).config();
  }
}
