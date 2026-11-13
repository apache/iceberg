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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkTableSink extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private TableEnvironment tEnv;
  private Table icebergTable;

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private boolean isStreamingJob;

  @Parameter(index = 4)
  private boolean useV2Sink;

  @Parameters(
      name = "catalogType={0}, baseNamespace={1}, format={2}, isStreaming={3}, useV2Sink={4}")
  public static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : CatalogTestBase.parameters()) {
          CatalogType catalogType = (CatalogType) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(
              new Object[] {
                catalogType, baseNamespace, format, isStreaming, false /* don't use v2 sink */
              });
        }
      }
    }

    for (FileFormat format :
        new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        parameters.add(
            new Object[] {
              CatalogType.HADOOP,
              Namespace.of("l0", "l1"),
              format,
              isStreaming,
              true /* use v2 sink */
            });
      }
    }

    return parameters;
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
    }

    tEnv.getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, useV2Sink);

    return tEnv;
  }

  @Override
  @BeforeEach
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
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    dropDatabase(flinkDatabase, true);
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @TestTemplate
  public void testInsertFromSourceTable() throws Exception {
    // Register the rows into a temporary table.
    getTableEnv()
        .createTemporaryView(
            "sourceTable",
            getTableEnv()
                .fromValues(
                    SimpleDataUtil.FLINK_SCHEMA.toSourceRowDataType(),
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

  @TestTemplate
  public void testOverwriteTable() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();

    sql("INSERT INTO %s SELECT 1, 'a'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(
        icebergTable, Lists.newArrayList(SimpleDataUtil.createRecord(1, "a")));

    sql("INSERT OVERWRITE %s SELECT 2, 'b'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(
        icebergTable, Lists.newArrayList(SimpleDataUtil.createRecord(2, "b")));
  }

  @TestTemplate
  public void testReplacePartitions() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
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

  @TestTemplate
  public void testFullOverwriteFromWriteOption() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_full_overwrite_option";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) WITH ('write.format.default'='%s')",
        tableName, format.name());

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      sql("INSERT OVERWRITE %s /*+ OPTIONS('overwrite-mode'='static') */ SELECT 4, 'b'", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable, Lists.newArrayList(SimpleDataUtil.createRecord(4, "b")));
      assertFullOverwriteSnapshot(partitionedTable);
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testFullOverwriteFromTableProperty() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_full_overwrite_property";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      sql("INSERT OVERWRITE %s SELECT 4, 'b'", tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable, Lists.newArrayList(SimpleDataUtil.createRecord(4, "b")));
      assertFullOverwriteSnapshot(partitionedTable);
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testFullOverwriteFromEmptySourceRemovesAllRows() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_full_overwrite_empty_source";
    String sourceName = "test_full_overwrite_empty_source_input";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());
    sql("CREATE TABLE %s(id INT, data VARCHAR)", sourceName);

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      sql("INSERT OVERWRITE %s SELECT id, data FROM %s", tableName, sourceName);

      SimpleDataUtil.assertTableRecords(partitionedTable, Lists.newArrayList());
      partitionedTable.refresh();
      Snapshot snapshot = partitionedTable.currentSnapshot();
      assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);
      assertThat(snapshot.summary())
          .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "3")
          .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0");
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, sourceName);
    }
  }

  @TestTemplate
  public void testDynamicOverwriteOptionOverridesStaticTableProperty() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_dynamic_option_static_property";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());

    try {
      Table partitionedTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      sql(
          "INSERT OVERWRITE %s /*+ OPTIONS('overwrite-mode'='dynamic') */ SELECT 4, 'b'",
          tableName);

      SimpleDataUtil.assertTableRecords(
          partitionedTable,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "a"),
              SimpleDataUtil.createRecord(4, "b"),
              SimpleDataUtil.createRecord(3, "c")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPartitionClauseRejectedInFullOverwrite() {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_full_overwrite_partition_clause";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());

    try {
      assertThatThrownBy(() -> sql("INSERT OVERWRITE %s PARTITION (data='a') SELECT 6", tableName))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "Cannot overwrite PARTITION {data=a} when overwrite-mode is static: "
                  + "a static overwrite replaces the whole table.");
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPartitionClauseRejectedWhenWriteOptionEnablesOverwrite() {
    String tableName = "test_full_overwrite_partition_clause_option";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());

    try {
      assertThatThrownBy(
              () ->
                  sql(
                      "INSERT INTO %s /*+ OPTIONS('overwrite-enabled'='true') */ "
                          + "PARTITION (data='a') SELECT 6",
                      tableName))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "Cannot overwrite PARTITION {data=a} when overwrite-mode is static: "
                  + "a static overwrite replaces the whole table.");
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testFullOverwriteReplacesFilesWrittenUnderOlderSpec() throws Exception {
    assumeThat(isStreamingJob)
        .as("Flink unbounded streaming does not support overwrite operation")
        .isFalse();
    String tableName = "test_full_overwrite_older_spec";
    sql(
        "CREATE TABLE %s(id INT, data VARCHAR) "
            + "WITH ('write.format.default'='%s', 'write.flink.overwrite-mode'='static')",
        tableName, format.name());

    try {
      sql("INSERT INTO %s VALUES (0, 'z')", tableName);
      Table olderSpecTable =
          validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, tableName));
      olderSpecTable.updateSpec().addField("data").commit();
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

      sql("INSERT OVERWRITE %s SELECT 3, 'a'", tableName);

      SimpleDataUtil.assertTableRecords(
          olderSpecTable, Lists.newArrayList(SimpleDataUtil.createRecord(3, "a")));
      assertFullOverwriteSnapshot(olderSpecTable);
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  private static void assertFullOverwriteSnapshot(Table table) {
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(snapshot.summary())
        .doesNotContainKey(SnapshotSummary.REPLACE_PARTITIONS_PROP)
        .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "3")
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1");
  }

  @TestTemplate
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

  // Test multiple IcebergSink instances writing to the same table in separate DAG branches.
  // This ensures the v2 sink can handle multiple sink operators for the same table
  // without naming collisions or operator conflicts when using statement set execution.
  @TestTemplate
  public void testIcebergSinkDifferentDAG() throws Exception {
    assumeThat(useV2Sink).isTrue();

    // Disable sink reuse optimization to force creation of two separate IcebergSink instances
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .setString("table.optimizer.reuse-sink-enabled", "false");

    // Register the rows into a temporary table.
    getTableEnv()
        .createTemporaryView(
            "sourceTable",
            getTableEnv()
                .fromValues(
                    SimpleDataUtil.FLINK_SCHEMA.toSourceRowDataType(),
                    Expressions.row(1, "hello"),
                    Expressions.row(2, "world"),
                    Expressions.row(3, (String) null),
                    Expressions.row(null, "bar")));

    getTableEnv()
        .createTemporaryView(
            "sourceTable1",
            getTableEnv()
                .fromValues(
                    SimpleDataUtil.FLINK_SCHEMA.toSourceRowDataType(),
                    Expressions.row(1, "hello"),
                    Expressions.row(2, "world"),
                    Expressions.row(3, (String) null),
                    Expressions.row(null, "bar")));

    // Redirect the records from source table to destination table.
    sql(
        "EXECUTE STATEMENT SET\n"
            + "BEGIN\n"
            + "INSERT INTO %s /*+ OPTIONS('uid-suffix'='source1') */ SELECT id,data from sourceTable;\n"
            + "INSERT INTO %s /*+ OPTIONS('uid-suffix'='source2') */ SELECT id,data from sourceTable1;\n"
            + "END;",
        TABLE_NAME, TABLE_NAME);

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTable,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hello"),
            SimpleDataUtil.createRecord(2, "world"),
            SimpleDataUtil.createRecord(3, null),
            SimpleDataUtil.createRecord(null, "bar")));
  }
}
