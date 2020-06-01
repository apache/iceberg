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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.source.RowDataRewriter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
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
  private final long targetDataFileSizeBytes;

  private PartitionSpec spec = null;
  private Expression filter;

  RewriteDataFilesAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.table = table;
    this.spec = table.spec();
    this.filter = Expressions.alwaysTrue();
    this.targetDataFileSizeBytes = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

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

  public RewriteDataFilesAction specId(int specId) {
    Preconditions.checkArgument(table.specs().containsKey(specId), "Invalid spec id %d", specId);
    this.spec = table.specs().get(specId);
    return this;
  }

  public RewriteDataFilesAction filter(Expression expr) {
    this.filter = Expressions.and(filter, expr);
    return this;
  }

  @Override
  public RewriteDataFilesActionResult execute() {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .filter(filter)
        .planFiles();
    List<DataFile> currentDataFiles =
        Lists.newArrayList(CloseableIterable.transform(fileScanTasks, FileScanTask::file));
    if (currentDataFiles.isEmpty()) {
      return RewriteDataFilesActionResult.empty();
    }

    List<CloseableIterable<FileScanTask>> groupedTasks = groupTasksByPartition(fileScanTasks);

    long splitSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    int lookbak = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    long openFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);

    List<CloseableIterable<CombinedScanTask>> groupedCombinedTasks = groupedTasks.stream()
        .map(tasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(tasks, splitSize);
          return TableScanUtil.planTasks(splitTasks, splitSize, lookbak, openFileCost);
        })
        .collect(Collectors.toList());
    CloseableIterable<CombinedScanTask> combinedScanTasks = CloseableIterable.concat(groupedCombinedTasks);

    Broadcast<FileIO> io = sparkContext.broadcast(fileIO);
    Broadcast<EncryptionManager> encryption = sparkContext.broadcast(encryptionManager);
    boolean caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive", "false"));

    RowDataRewriter rowDataRewriter = new RowDataRewriter(table, sparkContext, spec,
        caseSensitive, io, encryption, targetDataFileSizeBytes);
    List<DataFile> addedDataFiles = rowDataRewriter.rewriteDataForTasks(combinedScanTasks);

    replaceDataFiles(currentDataFiles, addedDataFiles);

    return new RewriteDataFilesActionResult(currentDataFiles, addedDataFiles);
  }

  private List<CloseableIterable<FileScanTask>> groupTasksByPartition(CloseableIterable<FileScanTask> tasks) {
    Map<StructLike, List<FileScanTask>> tasksGroupedByPartition = Maps.newHashMap();

    tasks.forEach(task -> {
      List<FileScanTask> taskList = tasksGroupedByPartition.getOrDefault(task.file().partition(), Lists.newArrayList());
      taskList.add(task);
      tasksGroupedByPartition.put(task.file().partition(), taskList);
    });

    return tasksGroupedByPartition.values().stream()
        .map(list -> CloseableIterable.combine(list, tasks))
        .collect(Collectors.toList());
  }

  private void replaceDataFiles(Iterable<DataFile> deletedDataFiles, Iterable<DataFile> addedDataFiles) {
    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.rewriteFiles(Sets.newHashSet(deletedDataFiles), Sets.newHashSet(addedDataFiles));
      commit(rewriteFiles);
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(addedDataFiles, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
    }
  }
}
