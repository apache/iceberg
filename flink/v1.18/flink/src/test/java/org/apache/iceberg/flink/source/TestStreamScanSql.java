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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
public class TestStreamScanSql extends CatalogTestBase {
  private static final String TABLE = "test_table";
  private static final FileFormat FORMAT = FileFormat.PARQUET;

  private TableEnvironment tEnv;

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings.Builder settingsBuilder =
              EnvironmentSettings.newInstance().inStreamingMode();

          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);

          StreamTableEnvironment streamTableEnv =
              StreamTableEnvironment.create(env, settingsBuilder.build());
          streamTableEnv
              .getConfig()
              .getConfiguration()
              .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
          tEnv = streamTableEnv;
        }
      }
    }
    return tEnv;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  private void insertRows(String partition, Table table, Row... rows) throws IOException {
    insertRows(partition, SnapshotRef.MAIN_BRANCH, table, rows);
  }

  private void insertRows(String partition, String branch, Table table, Row... rows)
      throws IOException {
    GenericAppenderHelper appender = new GenericAppenderHelper(table, FORMAT, temporaryDirectory);

    GenericRecord gRecord = GenericRecord.create(table.schema());
    List<Record> records = Lists.newArrayList();
    for (Row row : rows) {
      records.add(
          gRecord.copy(
              "id", row.getField(0),
              "data", row.getField(1),
              "dt", row.getField(2)));
    }

    if (partition != null) {
      appender.appendToTable(TestHelpers.Row.of(partition, 0), branch, records);
    } else {
      appender.appendToTable(branch, records);
    }
  }

  private void insertRowsInBranch(String branch, Table table, Row... rows) throws IOException {
    insertRows(null, branch, table, rows);
  }

  private void insertRows(Table table, Row... rows) throws IOException {
    insertRows(null, table, rows);
  }

  private void assertRows(List<Row> expectedRows, Iterator<Row> iterator) {
    for (Row expectedRow : expectedRows) {
      assertThat(iterator).hasNext();
      Row actualRow = iterator.next();
      assertThat(actualRow.getArity()).isEqualTo(3);
      assertThat(actualRow.getField(0)).isEqualTo(expectedRow.getField(0));
      assertThat(actualRow.getField(1)).isEqualTo(expectedRow.getField(1));
      assertThat(actualRow.getField(2)).isEqualTo(expectedRow.getField(2));
    }
  }

  @TestTemplate
  public void testUnPartitionedTable() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    TableResult result =
        exec("SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/", TABLE);
    try (CloseableIterator<Row> iterator = result.collect()) {

      Row row1 = Row.of(1, "aaa", "2021-01-01");
      insertRows(table, row1);
      assertRows(ImmutableList.of(row1), iterator);

      Row row2 = Row.of(2, "bbb", "2021-01-01");
      insertRows(table, row2);
      assertRows(ImmutableList.of(row2), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  public void testPartitionedTable() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR) PARTITIONED BY (dt)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    TableResult result =
        exec("SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/", TABLE);
    try (CloseableIterator<Row> iterator = result.collect()) {
      Row row1 = Row.of(1, "aaa", "2021-01-01");
      insertRows("2021-01-01", table, row1);
      assertRows(ImmutableList.of(row1), iterator);

      Row row2 = Row.of(2, "bbb", "2021-01-02");
      insertRows("2021-01-02", table, row2);
      assertRows(ImmutableList.of(row2), iterator);

      Row row3 = Row.of(1, "aaa", "2021-01-02");
      insertRows("2021-01-02", table, row3);
      assertRows(ImmutableList.of(row3), iterator);

      Row row4 = Row.of(2, "bbb", "2021-01-01");
      insertRows("2021-01-01", table, row4);
      assertRows(ImmutableList.of(row4), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  public void testConsumeFromBeginning() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    Row row1 = Row.of(1, "aaa", "2021-01-01");
    Row row2 = Row.of(2, "bbb", "2021-01-01");
    insertRows(table, row1, row2);

    TableResult result =
        exec("SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/", TABLE);
    try (CloseableIterator<Row> iterator = result.collect()) {
      assertRows(ImmutableList.of(row1, row2), iterator);

      Row row3 = Row.of(3, "ccc", "2021-01-01");
      insertRows(table, row3);
      assertRows(ImmutableList.of(row3), iterator);

      Row row4 = Row.of(4, "ddd", "2021-01-01");
      insertRows(table, row4);
      assertRows(ImmutableList.of(row4), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  /**
   * Insert records on the main branch. Then, insert in a named branch. Reads from the main branch
   * and assert that the only records from main are returned
   */
  public void testConsumeFilesFromMainBranch() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    // Produce two snapshots on main branch
    Row row1 = Row.of(1, "aaa", "2021-01-01");
    Row row2 = Row.of(2, "bbb", "2021-01-01");

    insertRows(table, row1, row2);
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName).commit();

    // insert on the 'b1' branch
    Row row3 = Row.of(3, "ccc", "2021-01-01");
    Row row4 = Row.of(4, "ddd", "2021-01-01");

    insertRowsInBranch(branchName, table, row3, row4);

    // read from main
    TableResult result =
        exec("SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/", TABLE);

    try (CloseableIterator<Row> iterator = result.collect()) {
      // the start snapshot(row2) is exclusive.
      assertRows(ImmutableList.of(row1, row2), iterator);

      Row row5 = Row.of(5, "eee", "2021-01-01");
      Row row6 = Row.of(6, "fff", "2021-01-01");
      insertRows(table, row5, row6);
      assertRows(ImmutableList.of(row5, row6), iterator);

      Row row7 = Row.of(7, "ggg", "2021-01-01");
      insertRows(table, row7);
      assertRows(ImmutableList.of(row7), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  /**
   * Insert records on the main branch. Creates a named branch. Insert record on named branch. Then
   * select from the named branch and assert all the records are returned.
   */
  public void testConsumeFilesFromBranch() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    // Produce two snapshots on main branch
    Row row1 = Row.of(1, "aaa", "2021-01-01");
    Row row2 = Row.of(2, "bbb", "2021-01-01");

    insertRows(table, row1, row2);
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName).commit();

    TableResult result =
        exec(
            "SELECT * FROM %s  /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'branch'='%s')*/ ",
            TABLE, branchName);

    try (CloseableIterator<Row> iterator = result.collect()) {
      assertRows(ImmutableList.of(row1, row2), iterator);
      // insert on the 'b1' branch
      Row row3 = Row.of(3, "ccc", "2021-01-01");
      Row row4 = Row.of(4, "ddd", "2021-01-01");
      insertRowsInBranch(branchName, table, row3, row4);
      assertRows(ImmutableList.of(row3, row4), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  /**
   * Insert records on branch b1. Then insert record on b2. Then select from each branch and assert
   * the correct records are returned
   */
  public void testConsumeFilesFromTwoBranches() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    String branch1 = "b1";
    String branch2 = "b2";
    table.manageSnapshots().createBranch(branch1).commit();
    table.manageSnapshots().createBranch(branch2).commit();

    // Produce two snapshots on main branch
    Row row1Branch1 = Row.of(1, "b1", "2021-01-01");
    Row row2Branch1 = Row.of(2, "b1", "2021-01-01");

    Row row1Branch2 = Row.of(2, "b2", "2021-01-01");
    Row row2Branch2 = Row.of(3, "b3", "2021-01-01");

    insertRowsInBranch(branch1, table, row1Branch1, row2Branch1);
    insertRowsInBranch(branch2, table, row1Branch2, row2Branch2);

    TableResult resultBranch1 =
        exec(
            "SELECT * FROM %s  /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'branch'='%s')*/ ",
            TABLE, branch1);

    try (CloseableIterator<Row> iterator = resultBranch1.collect()) {
      assertRows(ImmutableList.of(row1Branch1, row2Branch1), iterator);
      Row another = Row.of(4, "ccc", "2021-01-01");
      insertRowsInBranch(branch1, table, another);
      assertRows(ImmutableList.of(another), iterator);
    }

    TableResult resultBranch2 =
        exec(
            "SELECT * FROM %s  /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'branch'='%s')*/ ",
            TABLE, branch2);
    try (CloseableIterator<Row> iterator = resultBranch2.collect()) {
      assertRows(ImmutableList.of(row1Branch2, row2Branch2), iterator);
      Row another = Row.of(4, "ccc", "2021-01-01");
      insertRowsInBranch(branch2, table, another);
      assertRows(ImmutableList.of(another), iterator);
    }

    resultBranch1.getJobClient().ifPresent(JobClient::cancel);
    resultBranch2.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  public void testConsumeFromStartSnapshotId() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    // Produce two snapshots.
    Row row1 = Row.of(1, "aaa", "2021-01-01");
    Row row2 = Row.of(2, "bbb", "2021-01-01");
    insertRows(table, row1);
    insertRows(table, row2);

    long startSnapshotId = table.currentSnapshot().snapshotId();

    Row row3 = Row.of(3, "ccc", "2021-01-01");
    Row row4 = Row.of(4, "ddd", "2021-01-01");
    insertRows(table, row3, row4);

    TableResult result =
        exec(
            "SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', "
                + "'start-snapshot-id'='%d')*/",
            TABLE, startSnapshotId);
    try (CloseableIterator<Row> iterator = result.collect()) {
      // the start snapshot(row2) is exclusive.
      assertRows(ImmutableList.of(row3, row4), iterator);

      Row row5 = Row.of(5, "eee", "2021-01-01");
      Row row6 = Row.of(6, "fff", "2021-01-01");
      insertRows(table, row5, row6);
      assertRows(ImmutableList.of(row5, row6), iterator);

      Row row7 = Row.of(7, "ggg", "2021-01-01");
      insertRows(table, row7);
      assertRows(ImmutableList.of(row7), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);
  }

  @TestTemplate
  public void testConsumeFromStartTag() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, dt VARCHAR)", TABLE);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE));

    // Produce two snapshots.
    Row row1 = Row.of(1, "aaa", "2021-01-01");
    Row row2 = Row.of(2, "bbb", "2021-01-01");
    insertRows(table, row1);
    insertRows(table, row2);

    String tagName = "t1";
    long startSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagName, startSnapshotId).commit();

    Row row3 = Row.of(3, "ccc", "2021-01-01");
    Row row4 = Row.of(4, "ddd", "2021-01-01");
    insertRows(table, row3, row4);

    TableResult result =
        exec(
            "SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', "
                + "'start-tag'='%s')*/",
            TABLE, tagName);
    try (CloseableIterator<Row> iterator = result.collect()) {
      // the start snapshot(row2) is exclusive.
      assertRows(ImmutableList.of(row3, row4), iterator);

      Row row5 = Row.of(5, "eee", "2021-01-01");
      Row row6 = Row.of(6, "fff", "2021-01-01");
      insertRows(table, row5, row6);
      assertRows(ImmutableList.of(row5, row6), iterator);

      Row row7 = Row.of(7, "ggg", "2021-01-01");
      insertRows(table, row7);
      assertRows(ImmutableList.of(row7), iterator);
    }
    result.getJobClient().ifPresent(JobClient::cancel);

    Assertions.assertThatThrownBy(
            () ->
                exec(
                    "SELECT * FROM %s /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-tag'='%s', "
                        + "'start-snapshot-id'='%d' )*/",
                    TABLE, tagName, startSnapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("START_SNAPSHOT_ID and START_TAG cannot both be set.");
  }
}
