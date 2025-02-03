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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

class SparkBinPackDVRewriter extends SparkBinPackPositionDeletesRewriter {

  /**
   * The minimum deletion ratio that needs to be associated with a Puffin file for it to be
   * considered for rewriting.
   *
   * <p>Defaults to 0.3, which means that if the deletion ratio of a Puffin file reaches or exceeds
   * 30%, it may trigger the rewriting operation.
   */
  public static final String DELETE_RATIO_THRESHOLD = "delete-ratio-threshold";

  public static final double DELETE_RATIO_THRESHOLD_DEFAULT = 0.3;

  private double deleteRatio;

  SparkBinPackDVRewriter(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "BIN-PACK-DVS";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(REWRITE_ALL, DELETE_RATIO_THRESHOLD);
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteRatio =
        PropertyUtil.propertyAsDouble(
            options, DELETE_RATIO_THRESHOLD, DELETE_RATIO_THRESHOLD_DEFAULT);
  }

  @Override
  protected Iterable<List<PositionDeletesScanTask>> filterFileGroups(
      List<List<PositionDeletesScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  private boolean shouldRewrite(List<PositionDeletesScanTask> group) {
    return tooHighDeleteRatio(group);
  }

  private boolean tooHighDeleteRatio(List<PositionDeletesScanTask> group) {
    if (group.isEmpty()) {
      return false;
    }

    long liveDataSize = group.stream().mapToLong(task -> task.file().contentSizeInBytes()).sum();

    String puffinLocation = group.get(0).file().location();
    long totalDataSize;
    try (PuffinReader reader = Puffin.read(table().io().newInputFile(puffinLocation)).build()) {
      totalDataSize = reader.dataSize();
    } catch (IOException e) {
      return false;
    }

    double liveRatio = liveDataSize / (double) totalDataSize;
    return 1.0d - liveRatio >= deleteRatio;
  }

  @Override
  protected void doRewrite(String groupId, List<PositionDeletesScanTask> group) {
    Preconditions.checkArgument(!group.isEmpty(), "Empty group");

    // read the deletes packing them into splits of the required size
    Dataset<Row> posDeletes =
        spark()
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
            .option(SparkReadOptions.FILE_OPEN_COST, "0")
            .load(groupId);

    // keep only valid position deletes
    Dataset<Row> dataFiles =
        SparkTableUtil.loadMetadataTable(spark(), table(), MetadataTableType.DATA_FILES);
    Column joinCond = posDeletes.col("file_path").equalTo(dataFiles.col("file_path"));
    Dataset<Row> validDeletes = posDeletes.join(dataFiles, joinCond, "leftsemi");

    // write the packed deletes into new files where each split becomes a new file
    validDeletes
        .sortWithinPartitions("file_path", "pos")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .mode("append")
        .save(groupId);
  }
}
