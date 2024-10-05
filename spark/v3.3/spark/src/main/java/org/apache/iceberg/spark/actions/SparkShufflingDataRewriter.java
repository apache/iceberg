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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.utils.DistributionAndOrderingUtils$;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.internal.SQLConf;

abstract class SparkShufflingDataRewriter extends SparkSizeBasedDataRewriter {

  /**
   * The number of shuffle partitions and consequently the number of output files created by the
   * Spark sort is based on the size of the input data files used in this file rewriter. Due to
   * compression, the disk file sizes may not accurately represent the size of files in the output.
   * This parameter lets the user adjust the file size used for estimating actual output data size.
   * A factor greater than 1.0 would generate more files than we would expect based on the on-disk
   * file size. A value less than 1.0 would create fewer files than we would expect based on the
   * on-disk size.
   */
  public static final String COMPRESSION_FACTOR = "compression-factor";

  public static final double COMPRESSION_FACTOR_DEFAULT = 1.0;

  private double compressionFactor;

  protected SparkShufflingDataRewriter(SparkSession spark, Table table) {
    super(spark, table);
  }

  protected abstract org.apache.iceberg.SortOrder sortOrder();

  /**
   * Retrieves and returns the schema for the rewrite using the current table schema.
   *
   * <p>The schema with all columns required for correctly sorting the table. This may include
   * additional computed columns which are not written to the table but are used for sorting.
   */
  protected Schema sortSchema() {
    return table().schema();
  }

  protected abstract Dataset<Row> sortedDF(Dataset<Row> df, List<FileScanTask> group);

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(COMPRESSION_FACTOR)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.compressionFactor = compressionFactor(options);
  }

  @Override
  public void doRewrite(String groupId, List<FileScanTask> group) {
    // the number of shuffle partition controls the number of output files
    spark().conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), numShufflePartitions(group));

    Dataset<Row> scanDF =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .load(groupId);

    Dataset<Row> sortedDF = sortedDF(scanDF, group);

    sortedDF
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .option(SparkWriteOptions.OUTPUT_SPEC_ID, outputSpecId())
        .mode("append")
        .save(groupId);
  }

  protected Dataset<Row> sort(Dataset<Row> df, org.apache.iceberg.SortOrder sortOrder) {
    SortOrder[] ordering = SparkDistributionAndOrderingUtil.convert(sortOrder);
    OrderedDistribution distribution = Distributions.ordered(ordering);
    SQLConf conf = spark().sessionState().conf();
    LogicalPlan plan = df.logicalPlan();
    LogicalPlan sortPlan =
        DistributionAndOrderingUtils$.MODULE$.prepareQuery(distribution, ordering, plan, conf);
    return new Dataset<>(spark(), sortPlan, df.encoder());
  }

  protected org.apache.iceberg.SortOrder outputSortOrder(
      List<FileScanTask> group, org.apache.iceberg.SortOrder sortOrder) {
    PartitionSpec spec = outputSpec();
    boolean requiresRepartitioning = !group.get(0).spec().equals(spec);
    if (requiresRepartitioning) {
      // build in the requirement for partition sorting into our sort order
      // as the original spec for this group does not match the output spec
      return SortOrderUtil.buildSortOrder(sortSchema(), spec, sortOrder());
    } else {
      return sortOrder;
    }
  }

  private long numShufflePartitions(List<FileScanTask> group) {
    long numOutputFiles = numOutputFiles((long) (inputSize(group) * compressionFactor));
    return Math.max(1, numOutputFiles);
  }

  private double compressionFactor(Map<String, String> options) {
    double value =
        PropertyUtil.propertyAsDouble(options, COMPRESSION_FACTOR, COMPRESSION_FACTOR_DEFAULT);
    Preconditions.checkArgument(
        value > 0, "'%s' is set to %s but must be > 0", COMPRESSION_FACTOR, value);
    return value;
  }
}
