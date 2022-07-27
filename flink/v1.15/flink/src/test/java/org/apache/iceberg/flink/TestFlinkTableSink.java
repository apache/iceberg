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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String SOURCE_TABLE = "default_catalog.default_database.bounded_source";
  private static final String TABLE_NAME = "test_table";
  private TableEnvironment tEnv;
  private Table icebergTable;

  private final FileFormat format;
  private final boolean isStreamingJob;

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
        }
      }
    }
    return parameters;
  }

  public TestFlinkTableSink(
      String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql(
        "CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s')",
        TABLE_NAME, format.name());
    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
  }

  @Override
  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @Test
  public void testInsertFromSourceTable() throws Exception {
    // Register the rows into a temporary table.
    getTableEnv()
        .createTemporaryView(
            "sourceTable",
            getTableEnv()
                .fromValues(
                    SimpleDataUtil.FLINK_SCHEMA.toRowDataType(),
                    Expressions.row(1, "hello"),
                    Expressions.row(2, "world"),
                    Expressions.row(3, (String) null),
                    Expressions.row(null, "bar")));

    // Redirect the records from source table to destination table.
    sql("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTable,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hello"),
            SimpleDataUtil.createRecord(2, "world"),
            SimpleDataUtil.createRecord(3, null),
            SimpleDataUtil.createRecord(null, "bar")));
  }

  @Test
  public void testOverwriteTable() throws Exception {
    Assume.assumeFalse(
        "Flink unbounded streaming does not support overwrite operation", isStreamingJob);

    sql("INSERT INTO %s SELECT 1, 'a'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(
        icebergTable, Lists.newArrayList(SimpleDataUtil.createRecord(1, "a")));

    sql("INSERT OVERWRITE %s SELECT 2, 'b'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(
        icebergTable, Lists.newArrayList(SimpleDataUtil.createRecord(2, "b")));
  }

  @Test
  public void testReplacePartitions() throws Exception {
    Assume.assumeFalse(
        "Flink unbounded streaming does not support overwrite operation", isStreamingJob);
    String tableName = "test_partition";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) WITH ('write.format.default'='%s')",
        tableName, format.name());

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));

      sql("INSERT INTO %s SELECT 1, 'a'", tableName);
      sql("INSERT INTO %s SELECT 2, 'b'", tableName);
      sql("INSERT INTO %s SELECT 3, 'c'", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "a"),
              SimpleDataUtil.createRecord(2, "b"),
              SimpleDataUtil.createRecord(3, "c")));

      sql("INSERT OVERWRITE %s SELECT 4, 'b'", tableName);
      sql("INSERT OVERWRITE %s SELECT 5, 'a'", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(5, "a"),
              SimpleDataUtil.createRecord(4, "b"),
              SimpleDataUtil.createRecord(3, "c")));

      sql("INSERT OVERWRITE %s PARTITION (data='a') SELECT 6", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(6, "a"),
              SimpleDataUtil.createRecord(4, "b"),
              SimpleDataUtil.createRecord(3, "c")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testInsertIntoPartition() throws Exception {
    String tableName = "test_insert_into_partition";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) WITH ('write.format.default'='%s')",
        tableName, format.name());

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));

      // Full partition.
      sql("INSERT INTO %s PARTITION (data='a') SELECT 1", tableName);
      sql("INSERT INTO %s PARTITION (data='a') SELECT 2", tableName);
      sql("INSERT INTO %s PARTITION (data='b') SELECT 3", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "a"),
              SimpleDataUtil.createRecord(2, "a"),
              SimpleDataUtil.createRecord(3, "b")));

      // Partial partition.
      sql("INSERT INTO %s SELECT 4, 'c'", tableName);
      sql("INSERT INTO %s SELECT 5, 'd'", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "a"),
              SimpleDataUtil.createRecord(2, "a"),
              SimpleDataUtil.createRecord(3, "b"),
              SimpleDataUtil.createRecord(4, "c"),
              SimpleDataUtil.createRecord(5, "d")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testHashDistributeMode() throws Exception {
    String tableName = "test_hash_distribution_mode";
    Map<String, String> tableProps =
        ImmutableMap.of(
            "write.format.default",
            format.name(),
            TableProperties.WRITE_DISTRIBUTION_MODE,
            DistributionMode.HASH.modeName());

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
    Assert.assertEquals(
        "Should have the expected rows in source table.",
        Sets.newHashSet(dataSet),
        Sets.newHashSet(sql("SELECT * FROM %s", SOURCE_TABLE)));

    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) WITH %s",
        tableName, toWithClause(tableProps));

    try {
      // Insert data set.
      sql("INSERT INTO %s SELECT * FROM %s", tableName, SOURCE_TABLE);

      Assert.assertEquals(
          "Should have the expected rows in sink table.",
          Sets.newHashSet(dataSet),
          Sets.newHashSet(sql("SELECT * FROM %s", tableName)));

      // Sometimes we will have more than one checkpoint if we pass the auto checkpoint interval,
      // thus producing multiple snapshots.  Here we assert that each snapshot has only 1 file per
      // partition.
      Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      Map<Long, List<DataFile>> snapshotToDataFiles = SimpleDataUtil.snapshotToDataFiles(table);
      for (List<DataFile> dataFiles : snapshotToDataFiles.values()) {
        if (dataFiles.isEmpty()) {
          continue;
        }

        Assert.assertEquals(
            "There should be 1 data file in partition 'aaa'",
            1,
            SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "aaa"))
                .size());
        Assert.assertEquals(
            "There should be 1 data file in partition 'bbb'",
            1,
            SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "bbb"))
                .size());
        Assert.assertEquals(
            "There should be 1 data file in partition 'ccc'",
            1,
            SimpleDataUtil.matchingPartitions(
                    dataFiles, table.spec(), ImmutableMap.of("data", "ccc"))
                .size());
      }
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
