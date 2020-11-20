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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStreamScanSql extends AbstractTestBase {

  private static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()),
      required(3, "dt", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("dt")
      .bucket("id", 1)
      .build();

  private static final FileFormat FORMAT = FileFormat.AVRO;

  private TableEnvironment tEnv;
  private HadoopCatalog catalog;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    Configuration conf = new Configuration();
    String warehouse = "file:" + warehouseFile;
    catalog = new HadoopCatalog(conf, warehouse);

    tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
    tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
        Duration.ofSeconds(1));
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
    tEnv.executeSql(String.format("create catalog iceberg_catalog with (" +
        "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");
  }

  private List<Record> insertRandomData(Table table) throws IOException {
    return insertRandomData(table, null);
  }

  private List<Record> insertRandomData(Table table, String partition) throws IOException {
    List<Record> records = RandomGenericData.generate(SCHEMA, 2, 0L);
    GenericAppenderHelper appender = new GenericAppenderHelper(table, FORMAT, TEMPORARY_FOLDER);
    if (partition != null) {
      records.get(0).set(2, partition);
      records.get(1).set(2, partition);
      appender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(partition, 0), records);
    } else {
      appender.appendToTable(records);
    }

    return records;
  }

  private List<Row> nextRows(Iterator<Row> iterator, int number) {
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < number; i++) {
      if (!iterator.hasNext()) {
        throw new RuntimeException("No more records.");
      }
      rows.add(iterator.next());
    }
    return rows;
  }

  private void insertAndAssert(Iterator<Row> iterator, Table table) throws IOException {
    insertAndAssert(iterator, table, null);
  }

  private void insertAndAssert(Iterator<Row> iterator, Table table, String partition) throws IOException {
    List<Record> records = insertRandomData(table, partition);
    List<Row> rows = nextRows(iterator, records.size());
    TestFlinkScan.assertRecords(rows, records, table.schema());
  }

  @Test
  public void testUnPartitionedTable() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);
    TableResult result = tEnv.executeSql("select * from t /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/");
    try (CloseableIterator<Row> iterator = result.collect()) {
      insertAndAssert(iterator, table);
      insertAndAssert(iterator, table);
      insertAndAssert(iterator, table);
    }
    result.getJobClient().get().cancel();
  }

  @Test
  public void testPartitionedTable() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);
    TableResult result = tEnv.executeSql("select * from t /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/");
    try (CloseableIterator<Row> iterator = result.collect()) {
      insertAndAssert(iterator, table, "2020-03-20");
      insertAndAssert(iterator, table, "2020-03-20");
      insertAndAssert(iterator, table, "2020-03-22");
      insertAndAssert(iterator, table, "2020-03-20");
    }
    result.getJobClient().get().cancel();
  }

  @Test
  public void testExistData() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);
    List<Record> records = insertRandomData(table);
    TableResult result = tEnv.executeSql("select * from t /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/");
    try (CloseableIterator<Row> iterator = result.collect()) {
      // check exist data first
      List<Row> rows = nextRows(iterator, records.size());
      TestFlinkScan.assertRecords(rows, records, SCHEMA);

      insertAndAssert(iterator, table);
      insertAndAssert(iterator, table);
    }
    result.getJobClient().get().cancel();
  }

  @Test
  public void testSpecifySnapshotId() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);

    insertRandomData(table);
    insertRandomData(table);
    long snapshotId = table.currentSnapshot().snapshotId();

    List<Record> records = insertRandomData(table);

    TableResult result = tEnv.executeSql(
        String.format("select * from t /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', " +
            "'start-snapshot-id'='%d')*/", snapshotId));
    try (CloseableIterator<Row> iterator = result.collect()) {
      // check exist data first
      List<Row> rows = nextRows(iterator, records.size());
      TestFlinkScan.assertRecords(rows, records, SCHEMA);

      insertAndAssert(iterator, table);
      insertAndAssert(iterator, table);
    }
    result.getJobClient().get().cancel();
  }
}
