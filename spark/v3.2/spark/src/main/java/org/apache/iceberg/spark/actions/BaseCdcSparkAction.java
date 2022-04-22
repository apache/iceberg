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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestGroup;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Cdc;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.lit;

public class BaseCdcSparkAction extends BaseSparkAction<Cdc, Cdc.Result> implements Cdc {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCdcSparkAction.class);
  public static final String RECORD_TYPE = "_record_type";
  public static final String COMMIT_SNAPSHOT_ID = "_commit_snapshot_id";
  public static final String COMMIT_TIMESTAMP = "_commit_timestamp";
  public static final String COMMIT_ORDER = "_commit_order";

  private final List<Long> snapshotIds = Lists.newLinkedList();
  private final Table table;
  private final List<Dataset<Row>> dfs = Lists.newLinkedList();

  private boolean ignoreRowsDeletedWithinSnapshot = true;
  private Expression filter = Expressions.alwaysTrue();

  protected BaseCdcSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  public Result execute() {
    for (int i = 0; i < snapshotIds.size(); i++) {
      generateCdcRecordsPerSnapshot(snapshotIds.get(i), i);
    }

    Dataset<Row> outputDf = null;
    for (Dataset<Row> df : dfs) {
      if (outputDf == null) {
        outputDf = df;
      } else {
        outputDf = outputDf.unionByName(df, true);
      }
    }
    return new BaseCdcSparkActionResult(outputDf);
  }

  private void generateCdcRecordsPerSnapshot(long snapshotId, int commitOrder) {
    Snapshot snapshot = table.snapshot(snapshotId);
    if (snapshot.operation().equals(DataOperations.REPLACE)) {
      return;
    }

    // metadata deleted data files
    Dataset<Row> deletedDf = readMetadataDeletedFiles(snapshotId, commitOrder);
    if (deletedDf != null) {
      dfs.add(deletedDf);
    }

    // pos and eq deletes
    Dataset<Row> rowLevelDeleteDf = readRowLevelDeletes(snapshotId, commitOrder);
    if (rowLevelDeleteDf != null) {
      dfs.add(rowLevelDeleteDf);
    }

    // new data file as the insert
    Dataset<Row> df = readAppendDataFiles(snapshotId, commitOrder);
    if (df != null) {
      dfs.add(df);
    }
  }

  private Dataset<Row> readAppendDataFiles(long snapshotId, int commitOrder) {
    List<FileScanTask> fileScanTasks = planAppendedFiles(snapshotId);
    if (fileScanTasks.isEmpty()) {
      return null;
    }

    String groupID = UUID.randomUUID().toString();
    FileScanTaskSetManager manager = FileScanTaskSetManager.get();
    manager.stageTasks(table, groupID, fileScanTasks);
    Dataset<Row> scanDF = spark().read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table.name());

    return withCdcColumns(scanDF, snapshotId, "I", commitOrder);
  }

  private List<FileScanTask> planAppendedFiles(long snapshotId) {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .useSnapshot(snapshotId)
        .filter(filter)
        .ignoreResiduals()
        .planFiles();

    Set<CharSequence> dataFiles = Sets.newHashSet();
    for (DataFile dataFile : table.snapshot(snapshotId).addedFiles()) {
      dataFiles.add(dataFile.path());
    }

    List<FileScanTask> appendedFiles = Lists.newLinkedList();
    if (dataFiles.isEmpty()) {
      return appendedFiles;
    }

    fileScanTasks.forEach(fileScanTask -> {
      if (fileScanTask.file().content().equals(FileContent.DATA) && dataFiles.contains(fileScanTask.file().path())) {
        FileScanTask newFileScanTask = fileScanTask;
        if (!ignoreRowsDeletedWithinSnapshot) {
          // remove delete files so that no delete will apply to the data file
          Preconditions.checkArgument(fileScanTask instanceof BaseFileScanTask,
              "Object fileScanTask should be an instance of BaseFileScanTask");
          newFileScanTask = ((BaseFileScanTask) fileScanTask).cloneWithoutDeletes();
        }
        appendedFiles.add(newFileScanTask);
      }
    });

    return appendedFiles;
  }

  private Dataset<Row> readMetadataDeletedFiles(long snapshotId, int commitOrder) {
    List<FileScanTask> fileScanTasks = planMetadataDeletedFiles(snapshotId);
    if (fileScanTasks.isEmpty()) {
      return null;
    }

    String groupID = UUID.randomUUID().toString();
    FileScanTaskSetManager manager = FileScanTaskSetManager.get();
    manager.stageTasks(table, groupID, fileScanTasks);
    Dataset<Row> scanDF = spark().read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table.name());

    return withCdcColumns(scanDF, snapshotId, "D", commitOrder);
  }

  private List<FileScanTask> planMetadataDeletedFiles(long snapshotId) {
    Snapshot snapshot = table.snapshot(snapshotId);
    ManifestGroup manifestGroup = new ManifestGroup(table.io(), snapshot.dataManifests(), snapshot.deleteManifests())
        .filterData(filter)
        .ignoreAdded()
        .specsById(((HasTableOperations) table).operations().current().specsById());

    return ImmutableList.copyOf(manifestGroup.planFiles());
  }

  private Dataset<Row> readRowLevelDeletes(long snapshotId, int commitOrder) {
    List<FileScanTask> fileScanTasks = planRowLevelDeleteFiles(snapshotId);
    if (fileScanTasks.isEmpty()) {
      return null;
    }

    String groupID = UUID.randomUUID().toString();
    FileScanTaskSetManager manager = FileScanTaskSetManager.get();
    manager.stageTasks(table, groupID, fileScanTasks);
    Dataset<Row> scanDF = spark().read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table.name())
        .filter(functions.column(MetadataColumns.IS_DELETED.name()).equalTo(true));

    return withCdcColumns(scanDF, snapshotId, "D", commitOrder);
  }

  private List<FileScanTask> planRowLevelDeleteFiles(long snapshotId) {
    Snapshot snapshot = table.snapshot(snapshotId);
    List<ManifestFile> manifestFiles = snapshot.deleteManifests().stream()
        .filter(manifestFile -> manifestFile.snapshotId().equals(snapshotId))
        .collect(Collectors.toList());

    // todo create a partition filter to filter out unrelated partitions
    ManifestGroup manifestGroup = new ManifestGroup(table.io(), snapshot.dataManifests(), manifestFiles)
        .filterData(filter)
        .onlyWithRowLevelDeletes()
        .specsById(((HasTableOperations) table).operations().current().specsById());

    return ImmutableList.copyOf(manifestGroup.planFiles());
  }

  private Dataset<Row> withCdcColumns(Dataset<Row> df, long snapshotId, String cdcType, int commitOrder) {
    return df.withColumn(RECORD_TYPE, lit(cdcType))
        .withColumn(COMMIT_SNAPSHOT_ID, lit(snapshotId))
        .withColumn(COMMIT_TIMESTAMP, lit(table.snapshot(snapshotId).timestampMillis()))
        .withColumn(COMMIT_ORDER, lit(commitOrder));
  }

  @Override
  public Cdc ofSnapshot(long snapshotId) {
    if (table.snapshot(snapshotId) != null) {
      snapshotIds.clear();
      snapshotIds.add(snapshotId);
    }
    return this;
  }

  @Override
  public Cdc ofCurrentSnapshot() {
    if (table.currentSnapshot() != null) {
      snapshotIds.clear();
      snapshotIds.add(table.currentSnapshot().snapshotId());
    }
    return this;
  }

  @Override
  public Cdc between(long fromSnapshotId, long toSnapshotId) {
    Preconditions.checkArgument(table.snapshot(fromSnapshotId) != null,
        "The fromSnapshotId(%s) is invalid", fromSnapshotId);
    Preconditions.checkArgument(table.snapshot(toSnapshotId) != null,
        "The toSnapshotId(%s) is invalid", toSnapshotId);
    Preconditions.checkArgument(SnapshotUtil.isAncestorOf(table, toSnapshotId, fromSnapshotId),
        "The fromSnapshot(%s) is not an ancestor of the toSnapshot(%s)", fromSnapshotId, toSnapshotId);

    snapshotIds.clear();
    // include the fromSnapshotId
    snapshotIds.add(fromSnapshotId);
    SnapshotUtil.ancestorIdsBetween(toSnapshotId, fromSnapshotId, table::snapshot).forEach(snapshotIds::add);
    return this;
  }

  @Override
  protected Cdc self() {
    return this;
  }
}
