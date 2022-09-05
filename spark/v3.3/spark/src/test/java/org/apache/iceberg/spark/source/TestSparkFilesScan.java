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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkFilesScan extends SparkCatalogTestBase {

  public TestSparkFilesScan(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testTaskSetLoading() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should produce 1 snapshot", 1, Iterables.size(table.snapshots()));

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();
      String setID = UUID.randomUUID().toString();
      taskSetManager.stageTasks(table, setID, ImmutableList.copyOf(fileScanTasks));

      // load the staged file set
      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, setID)
              .load(tableName);

      // write the records back essentially duplicating data
      scanDF.writeTo(tableName).append();
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(1, "a"), row(2, "b"), row(2, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testTaskSetPlanning() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should produce 2 snapshots", 2, Iterables.size(table.snapshots()));

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();
      String setID = UUID.randomUUID().toString();
      List<FileScanTask> tasks = ImmutableList.copyOf(fileScanTasks);
      taskSetManager.stageTasks(table, setID, tasks);

      // load the staged file set and make sure each file is in a separate split
      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, tasks.get(0).file().fileSizeInBytes())
              .load(tableName);
      Assert.assertEquals("Num partitions should match", 2, scanDF.javaRDD().getNumPartitions());

      // load the staged file set and make sure we combine both files into a single split
      scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, Long.MAX_VALUE)
              .load(tableName);
      Assert.assertEquals("Num partitions should match", 1, scanDF.javaRDD().getNumPartitions());
    }
  }
}
