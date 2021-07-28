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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDeleteStrategy;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.source.EqualityDeleteRewriter;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertEqDeletesStrategy implements RewriteDeleteStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(ConvertEqDeletesStrategy.class);

  private final Table table;
  private long deleteTargetSizeInBytes;
  private int splitLookback;
  private long splitOpenFileCost;

  private CloseableIterable<FileScanTask> tasksWithEqDelete;
  private Iterable<DeleteFile> deletesToReplace;
  private final JavaSparkContext sparkContext;

  /**
   * Defines whether to split out the result position deletes by data file names.
   *
   * This should be used in EqualityDeleteRewriter.
   */
  public static final String SPLIT_POSITION_DELETE = "split-position-delete";

  public ConvertEqDeletesStrategy(SparkSession spark, Table table) {
    this.table = table;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.deleteTargetSizeInBytes = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.DELETE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    this.splitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  @Override
  public String name() {
    return "CONVERT-EQUALITY-DELETES";
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Iterable<DeleteFile> selectDeletes() {
    CloseableIterable<FileScanTask> fileScanTasks = null;
    try {
      fileScanTasks = table.newScan()
          .ignoreResiduals()
          .planFiles();
    } finally {
      try {
        if (fileScanTasks != null) {
          fileScanTasks.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to close task iterable", ioe);
      }
    }

    tasksWithEqDelete = CloseableIterable.filter(fileScanTasks, scan ->
        scan.deletes().stream().anyMatch(delete -> delete.content().equals(FileContent.EQUALITY_DELETES))
    );

    Set<DeleteFile> eqDeletes = Sets.newHashSet();
    tasksWithEqDelete.forEach(task -> {
      eqDeletes.addAll(task.deletes().stream()
          .filter(deleteFile -> deleteFile.content().equals(FileContent.EQUALITY_DELETES))
          .collect(Collectors.toList()));
    });

    deletesToReplace = eqDeletes;

    return deletesToReplace;
  }

  @Override
  public Iterable<DeleteFile> rewriteDeletes() {
    Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks =
        TableScanUtil.groupTasksByPartition(table.spec(), tasksWithEqDelete.iterator());

    // Split and combine tasks under each partition
    List<Pair<StructLike, CombinedScanTask>> combinedScanTasks = groupedTasks.entrySet().stream()
        .map(entry -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(entry.getValue()), deleteTargetSizeInBytes);
          return Pair.of(entry.getKey().get(),
              TableScanUtil.planTasks(splitTasks, deleteTargetSizeInBytes, splitLookback, splitOpenFileCost));
        })
        .flatMap(pair -> StreamSupport.stream(CloseableIterable
            .transform(pair.second(), task -> Pair.of(pair.first(), task)).spliterator(), false)
        )
        .collect(Collectors.toList());

    if (!combinedScanTasks.isEmpty()) {
      JavaRDD<Pair<StructLike, CombinedScanTask>> taskRDD = sparkContext.parallelize(combinedScanTasks,
          combinedScanTasks.size());
      Broadcast<FileIO> io = sparkContext.broadcast(table.io());
      Broadcast<EncryptionManager> encryption = sparkContext.broadcast(table.encryption());

      EqualityDeleteRewriter deleteRewriter = new EqualityDeleteRewriter(table, true, io, encryption);

      return deleteRewriter.toPosDeletes(taskRDD);
    }

    return Lists.newArrayList();
  }

  public RewriteDeleteStrategy options(Map<String, String> options) {
    // TODO: parse the options
    return this;
  }

  public Set<String> validOptions() {
    return ImmutableSet.of(
        SPLIT_POSITION_DELETE
    );
  }
}
