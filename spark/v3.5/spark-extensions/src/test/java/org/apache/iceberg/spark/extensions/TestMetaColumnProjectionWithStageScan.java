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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetaColumnProjectionWithStageScan extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private <T extends ScanTask> void stageTask(
      Table tab, String fileSetID, CloseableIterable<T> tasks) {
    ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
    taskSetManager.stageTasks(tab, fileSetID, Lists.newArrayList(tasks));
  }

  @TestTemplate
  public void testReadStageTableMeta() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));

    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();
    String tableLocation = table.location();

    try (CloseableIterable<ScanTask> tasks = table.newBatchScan().planFiles()) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(table, fileSetID, tasks);
      Dataset<Row> scanDF2 =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_OPEN_COST, "0")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .load(tableLocation);

      assertThat(scanDF2.columns()).hasSize(2);
    }

    try (CloseableIterable<ScanTask> tasks = table.newBatchScan().planFiles()) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(table, fileSetID, tasks);
      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_OPEN_COST, "0")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .load(tableLocation)
              .select("*", "_pos");

      List<Row> rows = scanDF.collectAsList();
      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(row(1L, "a", 0L), row(2L, "b", 1L), row(3L, "c", 2L), row(4L, "d", 3L));
      assertEquals("result should match", expectedRows, rowsToJava(rows));
    }
  }
}
