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
package org.apache.iceberg.spark.actions;

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class SparkBinPackFileRewriteRunner extends SparkDataFileRewriteRunner {

  SparkBinPackFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "BIN-PACK";
  }

  @Override
  protected void doRewrite(String groupId, RewriteFileGroup group) {
    // read the files packing them into splits of the required size
    Dataset<Row> scanDF =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .option(SparkReadOptions.SPLIT_SIZE, group.inputSplitSize())
            .option(SparkReadOptions.FILE_OPEN_COST, "0")
            .load(groupId);

    // write the packed data into new files where each split becomes a new file
    scanDF
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, group.maxOutputFileSize())
        .option(SparkWriteOptions.DISTRIBUTION_MODE, distributionMode(group).modeName())
        .option(SparkWriteOptions.OUTPUT_SPEC_ID, group.outputSpecId())
        .mode("append")
        .save(groupId);
  }

  // invoke a shuffle if the original spec does not match the output spec
  private DistributionMode distributionMode(RewriteFileGroup group) {
    boolean requiresRepartition =
        !group.fileScanTasks().get(0).spec().equals(spec(group.outputSpecId()));
    return requiresRepartition ? DistributionMode.RANGE : DistributionMode.NONE;
  }
}
