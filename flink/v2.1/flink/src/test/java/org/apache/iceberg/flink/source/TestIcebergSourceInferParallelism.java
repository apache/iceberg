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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestIcebergSourceInferParallelism {
  private static final int NUM_TMS = 2;
  private static final int SLOTS_PER_TM = 2;
  private static final int PARALLELISM = NUM_TMS * SLOTS_PER_TM;
  private static final int MAX_INFERRED_PARALLELISM = 3;
  private static final int JOB_MAX_PARALLELISM = 2;
  private static final String CATALOG = "infer_parallelism_catalog";

  @RegisterExtension
  private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUM_TMS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TM)
              .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
              .build());

  @RegisterExtension
  protected static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  @TempDir private Path tmpDir;

  private Table table;
  private GenericAppenderHelper dataAppender;

  @BeforeEach
  public void before() throws IOException {
    this.table =
        CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    this.dataAppender = new GenericAppenderHelper(table, FileFormat.PARQUET, tmpDir);
  }

  @AfterEach
  public void after() {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
  }

  /**
   * The two ways a job reaches {@link IcebergSource}. Both must infer the same parallelism: the SQL
   * path bypasses {@code buildStream} — it hands the source to Flink declaratively, for lineage —
   * and so applies the inference itself.
   */
  private enum Api {
    DATASTREAM,
    SQL
  }

  @ParameterizedTest
  @EnumSource(Api.class)
  public void testEmptyTable(Api api) throws Exception {
    // Inferred parallelism should be at least 1 even if table is empty
    test(api, 1, 0);
  }

  @ParameterizedTest
  @EnumSource(Api.class)
  public void testTableWithFilesLessThanMaxInferredParallelism(Api api) throws Exception {
    // Append files to the table
    for (int i = 0; i < 2; ++i) {
      List<Record> batch = RandomGenericData.generate(table.schema(), 1, 0);
      dataAppender.appendToTable(batch);
    }

    // Inferred parallelism should equal to 2 splits
    test(api, 2, 2);
  }

  @ParameterizedTest
  @EnumSource(Api.class)
  public void testTableWithFilesMoreThanMaxInferredParallelism(Api api) throws Exception {
    // Append files to the table
    for (int i = 0; i < MAX_INFERRED_PARALLELISM + 1; ++i) {
      List<Record> batch = RandomGenericData.generate(table.schema(), 1, 0);
      dataAppender.appendToTable(batch);
    }

    // Inferred parallelism should be capped by the MAX_INFERRED_PARALLELISM
    test(api, MAX_INFERRED_PARALLELISM, MAX_INFERRED_PARALLELISM + 1);
  }

  @ParameterizedTest
  @EnumSource(Api.class)
  public void testInferredParallelismIsClampedByJobMaxParallelism(Api api) throws Exception {
    for (int i = 0; i < MAX_INFERRED_PARALLELISM + 1; ++i) {
      List<Record> batch = RandomGenericData.generate(table.schema(), 1, 0);
      dataAppender.appendToTable(batch);
    }

    // The split count infers MAX_INFERRED_PARALLELISM, above the job's max parallelism. The SQL
    // path clamps by reading PipelineOptions#MAX_PARALLELISM from the table config rather than
    // from the StreamExecutionEnvironment, which it does not have; the two agree because
    // ExecutionConfig wraps the environment's own Configuration, which is what the table config
    // is rooted in. Guards that equivalence, since it is not obvious from either call site.
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setMaxParallelism(JOB_MAX_PARALLELISM);

    test(api, env, JOB_MAX_PARALLELISM, MAX_INFERRED_PARALLELISM + 1);
  }

  private void test(Api api, int expectedParallelism, int expectedRecords) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);

    test(api, env, expectedParallelism, expectedRecords);
  }

  private void test(
      Api api, StreamExecutionEnvironment env, int expectedParallelism, int expectedRecords)
      throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, true);
    config.set(
        FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX,
        MAX_INFERRED_PARALLELISM);

    switch (api) {
      case DATASTREAM:
        testDataStream(env, config, expectedParallelism, expectedRecords);
        break;
      case SQL:
        testSql(env, config, expectedParallelism, expectedRecords);
        break;
      default:
        throw new IllegalArgumentException("Unknown api: " + api);
    }
  }

  private void testDataStream(
      StreamExecutionEnvironment env,
      Configuration config,
      int expectedParallelism,
      int expectedRecords)
      throws Exception {
    DataStream<Row> dataStream =
        IcebergSource.forRowData()
            .tableLoader(CATALOG_EXTENSION.tableLoader())
            .table(table)
            .flinkConfig(config)
            // force one file per split
            .splitSize(1L)
            .buildStream(env)
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(table.schema())));

    DataStream.Collector<Row> collector = new DataStream.Collector<>();
    dataStream.collectAsync(collector);
    JobClient jobClient = env.executeAsync();
    try (CloseableIterator<Row> iterator = collector.getOutput()) {
      assertThat(drain(iterator)).hasSize(expectedRecords);
      verifySourceParallelism(
          expectedParallelism, miniCluster().getExecutionGraph(jobClient.getJobID()).get());
    }
  }

  private void testSql(
      StreamExecutionEnvironment env,
      Configuration config,
      int expectedParallelism,
      int expectedRecords)
      throws Exception {
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    config.toMap().forEach(tableEnv.getConfig()::set);

    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
            CATALOG, CATALOG_EXTENSION.warehouse()));

    TableResult result =
        tableEnv.executeSql(
            String.format(
                // backticks because TestFixtures.DATABASE is the reserved word "default";
                // split-size forces one file per split, as the DataStream variant does
                "SELECT * FROM `%s`.`%s`.`%s` /*+ OPTIONS('split-size'='1') */",
                CATALOG, TestFixtures.DATABASE, TestFixtures.TABLE));

    JobClient jobClient = result.getJobClient().orElseThrow(AssertionError::new);
    try (CloseableIterator<Row> iterator = result.collect()) {
      assertThat(drain(iterator)).hasSize(expectedRecords);
      verifySourceParallelism(
          expectedParallelism, miniCluster().getExecutionGraph(jobClient.getJobID()).get());
    } finally {
      tableEnv.executeSql("DROP CATALOG " + CATALOG);
    }
  }

  private static List<Row> drain(CloseableIterator<Row> iterator) {
    List<Row> rows = Lists.newArrayList();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }

    return rows;
  }

  /**
   * Borrowed this approach from Flink {@code FileSourceTextLinesITCase} to get source parallelism
   * from execution graph.
   */
  private static void verifySourceParallelism(
      int expectedParallelism, AccessExecutionGraph executionGraph) {
    AccessExecutionJobVertex sourceVertex =
        executionGraph.getVerticesTopologically().iterator().next();
    assertThat(sourceVertex.getParallelism()).isEqualTo(expectedParallelism);
  }

  /**
   * Use reflection to get {@code InternalMiniClusterExtension} and {@code MiniCluster} to get
   * execution graph and source parallelism. Haven't find other way via public APIS.
   */
  private static MiniCluster miniCluster() throws Exception {
    Field privateField =
        MiniClusterExtension.class.getDeclaredField("internalMiniClusterExtension");
    privateField.setAccessible(true);
    InternalMiniClusterExtension internalExtension =
        (InternalMiniClusterExtension) privateField.get(MINI_CLUSTER_EXTENSION);
    return internalExtension.getMiniCluster();
  }
}
