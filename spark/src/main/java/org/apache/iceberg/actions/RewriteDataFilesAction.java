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

package org.apache.iceberg.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.source.RowDataRewriter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteDataFilesAction
    extends BaseSnapshotUpdateAction<RewriteDataFilesAction, RewriteDataFilesActionResult> {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesAction.class);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Table table;
  private final FileIO fileIO;
  private final EncryptionManager encryptionManager;
  private long targetSizeInBytes;
  private int splitLookback;

  private PartitionSpec spec = null;
  private Expression filter;

  RewriteDataFilesAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.table = table;
    this.spec = table.spec();
    this.filter = Expressions.alwaysTrue();

    long splitSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    long targetFileSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetSizeInBytes = Math.min(splitSize, targetFileSize);

    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);

    if (table.io() instanceof HadoopFileIO) {
      // we need to use Spark's SerializableConfiguration to avoid issues with Kryo serialization
      SerializableConfiguration conf = new SerializableConfiguration(((HadoopFileIO) table.io()).conf());
      this.fileIO = new HadoopFileIO(conf::value);
    } else {
      this.fileIO = table.io();
    }
    this.encryptionManager = table.encryption();
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }

  @Override
  protected Table table() {
    return table;
  }

  /**
   * Pass a PartitionSepc id to specify which PartitionSpec should be used in DataFile rewrite
   *
   * @param specId PartitionSpec id to rewrite
   * @return this for method chaining
   */
  public RewriteDataFilesAction outputSpecId(int specId) {
    Preconditions.checkArgument(table.specs().containsKey(specId), "Invalid spec id %d", specId);
    this.spec = table.specs().get(specId);
    return this;
  }

  /**
   * Specify the target rewrite data file size in bytes
   *
   * @param targetSize size in bytes of rewrite data file
   * @return this for method chaining
   */
  public RewriteDataFilesAction targetSizeInBytes(long targetSize) {
    Preconditions.checkArgument(targetSize > 0L, "Invalid target rewrite data file size in bytes %d",
        targetSize);
    this.targetSizeInBytes = targetSize;
    return this;
  }

  /**
   * Specify the number of "bins" considered when trying to pack the next file split into a task.
   * Increasing this usually makes tasks a bit more even by considering more ways to pack file regions into a single
   * task with extra planning cost.
   *
   * This configuration can reorder the incoming file regions, to preserve order for lower/upper bounds in file
   * metadata, user can use a lookback of 1.
   *
   * @param lookback number of "bins" considered when trying to pack the next file split into a task.
   * @return this for method chaining
   */
  public RewriteDataFilesAction splitLookback(int lookback) {
    Preconditions.checkArgument(lookback > 0L, "Invalid split lookback %d", lookback);
    this.splitLookback = lookback;
    return this;
  }

  /**
   * Pass a row Expression to filter DataFiles to be rewritten. Note that all files that may contain data matching the
   * filter may be rewritten.
   *
   * @param expr Expression to filter out DataFiles
   * @return this for method chaining
   */
  public RewriteDataFilesAction filter(Expression expr) {
    this.filter = Expressions.and(filter, expr);
    return this;
  }

  @Override
  public RewriteDataFilesActionResult execute() {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .filter(filter)
        .planFiles();

    Map<StructLikeWrapper, List<FileScanTask>> groupedTasks = groupTasksByPartition(fileScanTasks.iterator());
    Map<StructLikeWrapper, List<FileScanTask>> filteredGroupedTasks = groupedTasks.entrySet().stream()
        .filter(kv -> kv.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Nothing to rewrite if there's only one DataFile in each partition.
    if (filteredGroupedTasks.isEmpty()) {
      return RewriteDataFilesActionResult.empty();
    }

    long openFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    // Split and combine tasks under each partition
    List<CombinedScanTask> combinedScanTasks = filteredGroupedTasks.values().stream()
        .map(scanTasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(scanTasks), targetSizeInBytes);
          return TableScanUtil.planTasks(splitTasks, targetSizeInBytes, splitLookback, openFileCost);
        })
        .flatMap(Streams::stream)
        .collect(Collectors.toList());

    JavaRDD<CombinedScanTask> taskRDD = sparkContext.parallelize(combinedScanTasks, combinedScanTasks.size());

    Broadcast<FileIO> io = sparkContext.broadcast(fileIO);
    Broadcast<EncryptionManager> encryption = sparkContext.broadcast(encryptionManager);
    boolean caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive", "false"));

    RowDataRewriter rowDataRewriter =
        new RowDataRewriter(table, spec, caseSensitive, io, encryption, targetSizeInBytes);

    List<DataFile> addedDataFiles = rowDataRewriter.rewriteDataForTasks(taskRDD);
    List<DataFile> currentDataFiles = filteredGroupedTasks.values().stream()
        .flatMap(tasks -> tasks.stream().map(FileScanTask::file))
        .collect(Collectors.toList());
    replaceDataFiles(currentDataFiles, addedDataFiles);

    return new RewriteDataFilesActionResult(currentDataFiles, addedDataFiles);
  }

  private Map<StructLikeWrapper, List<FileScanTask>> groupTasksByPartition(CloseableIterator<FileScanTask> tasksIter) {
    Map<StructLikeWrapper, List<FileScanTask>> tasksGroupedByPartition = Maps.newHashMap();

    try {
      tasksIter.forEachRemaining(task -> {
        StructLikeWrapper structLike = StructLikeWrapper.wrap(task.file().partition());
        List<FileScanTask> taskList = tasksGroupedByPartition.getOrDefault(structLike, Lists.newArrayList());
        taskList.add(task);
        tasksGroupedByPartition.put(structLike, taskList);
      });
    } finally {
      try {
        tasksIter.close();
      } catch (IOException ioe) {
        LOG.warn("Faile to close task iterator", ioe);
      }
    }

    return tasksGroupedByPartition;
  }

  private void replaceDataFiles(Iterable<DataFile> deletedDataFiles, Iterable<DataFile> addedDataFiles) {
    boolean threw = true;

    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.rewriteFiles(Sets.newHashSet(deletedDataFiles), Sets.newHashSet(addedDataFiles));
      commit(rewriteFiles);
      threw = false;

    } finally {
      if (threw) {
        Tasks.foreach(Iterables.transform(addedDataFiles, f -> f.path().toString()))
            .noRetry()
            .suppressFailureWhenFinished()
            .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
            .run(fileIO::deleteFile);
      }
    }
  }
}
