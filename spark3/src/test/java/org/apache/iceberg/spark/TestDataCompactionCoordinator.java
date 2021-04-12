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

package org.apache.iceberg.spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.DataCompactionJobCoordinator.JobResult;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestDataCompactionCoordinator extends SparkCatalogTestBase {

  public TestDataCompactionCoordinator(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testBinPacking() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    Dataset<Row> df = newDF(1000);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should produce 4 snapshots", 4, Iterables.size(table.snapshots()));

    Dataset<Row> fileDF = spark.read().format("iceberg").load(tableName(tableIdent.name() + ".files"));
    List<Long> fileSizes = fileDF.select("file_size_in_bytes").as(Encoders.LONG()).collectAsList();
    long avgFileSize = fileSizes.stream().mapToLong(i -> i).sum() / fileSizes.size();

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      String jobID = DataCompactionJobCoordinator.stageJob(table, Lists.newArrayList(fileScanTasks));

      // read and pack original 4 files into 2 splits
      Dataset<Row> scanDF = spark.read().format("iceberg")
          .option(SparkReadOptions.COMPACTION_JOB_ID, jobID)
          .option(SparkReadOptions.SPLIT_SIZE, Long.toString(avgFileSize * 2))
          .option(SparkReadOptions.FILE_OPEN_COST, "0")
          .load(tableName);

      // write the packed data back where each split becomes a new file
      scanDF.writeTo(tableName)
          .option(SparkWriteOptions.COMPACTION_JOB_ID, jobID)
          .append();

      JobResult jobResult = DataCompactionJobCoordinator.getJobResult(table, jobID);
      List<DataFile> rewrittenDataFiles = jobResult.rewrittenDataFiles();
      List<DataFile> newDataFiles = jobResult.newDataFiles();
      Assert.assertEquals("Rewritten files count must match", 4, rewrittenDataFiles.size());
      Assert.assertEquals("New files count must match", 2, newDataFiles.size());

      table.newRewrite()
          .rewriteFiles(ImmutableSet.copyOf(rewrittenDataFiles), ImmutableSet.copyOf(newDataFiles))
          .commit();
    }

    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("Deleted files count must match", "4", summary.get("deleted-data-files"));
    Assert.assertEquals("Added files count must match", "2", summary.get("added-data-files"));

    Object rowCount = scalarSql("SELECT count(*) FROM %s", tableName);
    Assert.assertEquals("Row count must match", 4000L, rowCount);
  }

  private Dataset<Row> newDF(int numRecords) {
    List<SimpleRecord> data = Lists.newArrayListWithExpectedSize(numRecords);
    for (int index = 0; index < numRecords; index++) {
      data.add(new SimpleRecord(index, Integer.toString(index)));
    }
    return spark.createDataFrame(data, SimpleRecord.class);
  }
}
