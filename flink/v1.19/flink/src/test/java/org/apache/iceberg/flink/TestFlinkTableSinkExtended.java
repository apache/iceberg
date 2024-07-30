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

import static org.apache.iceberg.flink.FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HADOOP;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests the more extended features of Flink sink. Extract them separately since it is
 * unnecessary to test all the parameters combinations in {@link TestFlinkTableSink}, like catalog
 * types, namespaces, file format, streaming/batch. Those combinations explode exponentially. Each
 * test method in {@link TestFlinkTableSink} runs 21 combinations, which are expensive and slow.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkTableSinkExtended extends SqlBase {
  protected static final String CATALOG = "testhadoop";
  protected static final String DATABASE = "db";
  protected static final String TABLE = "tbl";

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  private static final String SOURCE_TABLE = "default_catalog.default_database.bounded_source";
  private static final String FLINK_DATABASE = CATALOG + "." + DATABASE;
  private static final Namespace ICEBERG_NAMESPACE = Namespace.of(new String[] {DATABASE});

  @TempDir protected File warehouseRoot;

  protected HadoopCatalog catalog = null;

  private TableEnvironment tEnv;

  @Parameter protected boolean isStreamingJob;

  @Parameters(name = "isStreamingJob={0}")
  protected static List<Object[]> parameters() {
    return Arrays.asList(new Boolean[] {true}, new Boolean[] {false});
  }

  protected synchronized TableEnvironment getTableEnv() {
    if (tEnv == null) {
      EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
      if (isStreamingJob) {
        settingsBuilder.inStreamingMode();
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
        env.enableCheckpointing(400);
        env.setMaxParallelism(2);
        env.setParallelism(2);
        tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
      } else {
        settingsBuilder.inBatchMode();
        tEnv = TableEnvironment.create(settingsBuilder.build());
      }
    }
    return tEnv;
  }

  @BeforeEach
  public void before() {
    String warehouseLocation = "file:" + warehouseRoot.getPath();
    this.catalog = new HadoopCatalog(new Configuration(), warehouseLocation);
    Map<String, String> config = Maps.newHashMap();
    config.put("type", "iceberg");
    config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HADOOP);
    config.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    sql("CREATE CATALOG %s WITH %s", CATALOG, toWithClause(config));

    sql("CREATE DATABASE %s", FLINK_DATABASE);
    sql("USE CATALOG %s", CATALOG);
    sql("USE %s", DATABASE);
    sql(
        "CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s')",
        TABLE, FileFormat.PARQUET.name());
  }

  @AfterEach
  public void clean() throws Exception {
    sql("DROP TABLE IF EXISTS %s.%s", FLINK_DATABASE, TABLE);
    dropDatabase(FLINK_DATABASE, true);
    BoundedTableFactory.clearDataSets();

    dropCatalog(CATALOG, true);
    catalog.close();
  }

  @TestTemplate
  public void testWriteParallelism() {
    List<Row> dataSet =
        IntStream.range(1, 1000)
            .mapToObj(i -> ImmutableList.of(Row.of(i, "aaa"), Row.of(i, "bbb"), Row.of(i, "ccc")))
            .flatMap(List::stream)
            .collect(Collectors.toList());
    String dataId = BoundedTableFactory.registerDataSet(ImmutableList.of(dataSet));
    sql(
        "CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL)"
            + " WITH ('connector'='BoundedSource', 'data-id'='%s')",
        SOURCE_TABLE, dataId);

    PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) getTableEnv()).getPlanner();
    String insertSQL =
        String.format(
            "INSERT INTO %s /*+ OPTIONS('write-parallelism'='1') */ SELECT * FROM %s",
            TABLE, SOURCE_TABLE);
    ModifyOperation operation = (ModifyOperation) planner.getParser().parse(insertSQL).get(0);
    Transformation<?> dummySink = planner.translate(Collections.singletonList(operation)).get(0);
    Transformation<?> committer = dummySink.getInputs().get(0);
    Transformation<?> writer = committer.getInputs().get(0);

    assertThat(writer.getParallelism()).as("Should have the expected 1 parallelism.").isEqualTo(1);
    writer
        .getInputs()
        .forEach(
            input ->
                assertThat(input.getParallelism())
                    .as("Should have the expected parallelism.")
                    .isEqualTo(isStreamingJob ? 2 : 4));
  }

  @TestTemplate
  public void testHashDistributeMode() throws Exception {
    // Initialize a BoundedSource table to precisely emit those rows in only one checkpoint.
    List<Row> dataSet =
        IntStream.range(1, 1000)
            .mapToObj(i -> ImmutableList.of(Row.of(i, "aaa"), Row.of(i, "bbb"), Row.of(i, "ccc")))
            .flatMap(List::stream)
            .collect(Collectors.toList());
    String dataId = BoundedTableFactory.registerDataSet(ImmutableList.of(dataSet));
    sql(
        "CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL)"
            + " WITH ('connector'='BoundedSource', 'data-id'='%s')",
        SOURCE_TABLE, dataId);

    assertThat(sql("SELECT * FROM %s", SOURCE_TABLE))
        .as("Should have the expected rows in source table.")
        .containsExactlyInAnyOrderElementsOf(dataSet);

    Map<String, String> tableProps =
        ImmutableMap.of(
            "write.format.default",
            FileFormat.PARQUET.name(),
            TableProperties.WRITE_DISTRIBUTION_MODE,
            DistributionMode.HASH.modeName());

    String tableName = "test_hash_distribution_mode";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) WITH %s",
        tableName, toWithClause(tableProps));

    try {
      // Insert data set.
      sql("INSERT INTO %s SELECT * FROM %s", tableName, SOURCE_TABLE);

      assertThat(sql("SELECT * FROM %s", tableName))
          .as("Should have the expected rows in sink table.")
          .containsExactlyInAnyOrderElementsOf(dataSet);

      // Sometimes we will have more than one checkpoint if we pass the auto checkpoint interval,
      // thus producing multiple snapshots.  Here we assert that each snapshot has only 1 file per
      // partition.
      Table table = catalog.loadTable(TableIdentifier.of(ICEBERG_NAMESPACE, tableName));
      Map<Long, List<DataFile>> snapshotToDataFiles = SimpleDataUtil.snapshotToDataFiles(table);
      for (List<DataFile> dataFiles : snapshotToDataFiles.values()) {
        if (dataFiles.isEmpty()) {
          continue;
        }

        assertThat(
                SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "aaa")))
            .hasSize(1);
        assertThat(
                SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "bbb")))
            .hasSize(1);
        assertThat(
                SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "ccc")))
            .hasSize(1);
      }
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", FLINK_DATABASE, tableName);
    }
  }
}
