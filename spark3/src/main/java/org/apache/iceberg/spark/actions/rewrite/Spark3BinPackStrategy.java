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

package org.apache.iceberg.spark.actions.rewrite;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

public class Spark3BinPackStrategy extends BinPackStrategy {
  private final Table table;
  private final SparkSession spark;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();

  public Spark3BinPackStrategy(Table table, SparkSession spark) {
    this.table = table;
    this.spark = spark;
  }


  @Override
  public Table table() {
    return table;
  }

  @Override
  public Set<DataFile> rewriteFiles(String groupID, List<FileScanTask> filesToRewrite) {
    manager.stageTasks(table, groupID, filesToRewrite);

    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    SparkSession cloneSession = spark.cloneSession();
    cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

    Dataset<Row> scanDF = cloneSession.read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .option(SparkReadOptions.SPLIT_SIZE, Long.toString(targetFileSize()))
        .option(SparkReadOptions.FILE_OPEN_COST, "0")
        .load(table.name());

    // write the packed data into new files where each split becomes a new file
    FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();
    try {
      scanDF.write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .mode("append")
          .save(table.name());
    } catch (Exception e) {
      try {
        rewriteCoordinator.abortRewrite(table, groupID);
      } finally {
        throw new RuntimeException("Cannot complete rewrite, an exception was thrown during the write operation", e);
      }
    }

    // Actual commit is performed with the groupID
    return rewriteCoordinator.fetchNewDataFiles(table, ImmutableSet.of(groupID));
  }
}
