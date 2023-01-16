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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.actions.SortStrategy;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.utils.DistributionAndOrderingUtils$;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.internal.SQLConf;

public class SparkSortStrategy extends SortStrategy {

  /**
   * The number of shuffle partitions and consequently the number of output files created by the
   * Spark Sort is based on the size of the input data files used in this rewrite operation. Due to
   * compression, the disk file sizes may not accurately represent the size of files in the output.
   * This parameter lets the user adjust the file size used for estimating actual output data size.
   * A factor greater than 1.0 would generate more files than we would expect based on the on-disk
   * file size. A value less than 1.0 would create fewer files than we would expect due to the
   * on-disk size.
   */
  public static final String COMPRESSION_FACTOR = "compression-factor";

  private final Table table;
  private final SparkSession spark;
  private final SparkTableCache tableCache = SparkTableCache.get();
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  private double sizeEstimateMultiple;

  public SparkSortStrategy(Table table, SparkSession spark) {
    this.table = table;
    this.spark = spark;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COMPRESSION_FACTOR)
        .build();
  }

  @Override
  public RewriteStrategy options(Map<String, String> options) {
    sizeEstimateMultiple = PropertyUtil.propertyAsDouble(options, COMPRESSION_FACTOR, 1.0);

    Preconditions.checkArgument(
        sizeEstimateMultiple > 0,
        "Invalid compression factor: %s (not positive)",
        sizeEstimateMultiple);

    return super.options(options);
  }

  @Override
  public Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite) {
    String groupID = UUID.randomUUID().toString();
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table.spec());

    SortOrder[] ordering;
    if (requiresRepartition) {
      // Build in the requirement for Partition Sorting into our sort order
      ordering =
          SparkDistributionAndOrderingUtil.convert(
              SortOrderUtil.buildSortOrder(table, sortOrder()));
    } else {
      ordering = SparkDistributionAndOrderingUtil.convert(sortOrder());
    }

    Distribution distribution = Distributions.ordered(ordering);

    try {
      tableCache.add(groupID, table);
      manager.stageTasks(table, groupID, filesToRewrite);

      // Reset Shuffle Partitions for our sort
      long numOutputFiles =
          numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple));
      spark.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numOutputFiles));

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
              .load(groupID);

      // write the packed data into new files where each split becomes a new file
      SQLConf sqlConf = spark.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, scanDF.logicalPlan(), sqlConf);
      Dataset<Row> sortedDf = new Dataset<>(spark, sortPlan, scanDF.encoder());

      sortedDf
          .write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
          .mode("append") // This will only write files without modifying the table, see
          // SparkWrite.RewriteFiles
          .save(groupID);

      return rewriteCoordinator.fetchNewDataFiles(table, groupID);
    } finally {
      tableCache.remove(groupID);
      manager.removeTasks(table, groupID);
      rewriteCoordinator.clearRewrite(table, groupID);
    }
  }

  protected SparkSession spark() {
    return this.spark;
  }

  protected LogicalPlan sortPlan(
      Distribution distribution, SortOrder[] ordering, LogicalPlan plan, SQLConf conf) {
    return DistributionAndOrderingUtils$.MODULE$.prepareQuery(distribution, ordering, plan, conf);
  }

  protected double sizeEstimateMultiple() {
    return sizeEstimateMultiple;
  }

  protected SparkTableCache tableCache() {
    return tableCache;
  }

  protected FileScanTaskSetManager manager() {
    return manager;
  }

  protected FileRewriteCoordinator rewriteCoordinator() {
    return rewriteCoordinator;
  }
}
