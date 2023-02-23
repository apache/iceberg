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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.IsolationLevel.SERIALIZABLE;
import static org.apache.iceberg.IsolationLevel.SNAPSHOT;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.OutputMetrics;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

abstract class SparkWrite implements Write, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkWrite.class);

  private final JavaSparkContext sparkContext;
  private final SparkWriteConf writeConf;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final String applicationId;
  private final boolean wapEnabled;
  private final String wapId;
  private final String branch;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final boolean partitionedFanoutEnabled;
  private final Distribution requiredDistribution;
  private final SortOrder[] requiredOrdering;

  private boolean cleanupOnAbort = true;

  SparkWrite(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      LogicalWriteInfo writeInfo,
      String applicationId,
      Schema writeSchema,
      StructType dsSchema,
      Distribution requiredDistribution,
      SortOrder[] requiredOrdering) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.writeConf = writeConf;
    this.queryId = writeInfo.queryId();
    this.format = writeConf.dataFileFormat();
    this.applicationId = applicationId;
    this.wapEnabled = writeConf.wapEnabled();
    this.wapId = writeConf.wapId();
    this.branch = writeConf.branch();
    this.targetFileSize = writeConf.targetDataFileSize();
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
    this.partitionedFanoutEnabled = writeConf.fanoutWriterEnabled();
    this.requiredDistribution = requiredDistribution;
    this.requiredOrdering = requiredOrdering;
  }

  @Override
  public Distribution requiredDistribution() {
    return requiredDistribution;
  }

  @Override
  public SortOrder[] requiredOrdering() {
    return requiredOrdering;
  }

  BatchWrite asBatchAppend() {
    return new BatchAppend();
  }

  BatchWrite asDynamicOverwrite() {
    return new DynamicOverwrite();
  }

  BatchWrite asOverwriteByFilter(Expression overwriteExpr) {
    return new OverwriteByFilter(overwriteExpr);
  }

  BatchWrite asCopyOnWriteOperation(SparkCopyOnWriteScan scan, IsolationLevel isolationLevel) {
    return new CopyOnWriteOperation(scan, isolationLevel);
  }

  BatchWrite asRewrite(String fileSetID) {
    return new RewriteFiles(fileSetID);
  }

  BatchWrite asPositionDeletesRewrite(String fileSetId) {
    return new PositionDeleteBatchWrite(fileSetId);
  }

  StreamingWrite asStreamingAppend() {
    return new StreamingAppend();
  }

  StreamingWrite asStreamingOverwrite() {
    return new StreamingOverwrite();
  }

  // the writer factory works for both batch and streaming
  private WriterFactory createWriterFactory() {
    // broadcast the table metadata as the writer factory will be sent to executors
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    return new WriterFactory(
        tableBroadcast,
        queryId,
        format,
        targetFileSize,
        writeSchema,
        dsSchema,
        partitionedFanoutEnabled);
  }

  private void commitOperation(SnapshotUpdate<?> operation, String description) {
    LOG.info("Committing {} to table {}", description, table);
    if (applicationId != null) {
      operation.set("spark.app.id", applicationId);
    }

    if (!extraSnapshotMetadata.isEmpty()) {
      extraSnapshotMetadata.forEach(operation::set);
    }

    if (!CommitMetadata.commitProperties().isEmpty()) {
      CommitMetadata.commitProperties().forEach(operation::set);
    }

    if (wapEnabled && wapId != null) {
      // write-audit-publish is enabled for this table and job
      // stage the changes without changing the current snapshot
      operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
      operation.stageOnly();
    }

    if (branch != null) {
      operation.toBranch(branch);
    }

    try {
      long start = System.currentTimeMillis();
      operation.commit(); // abort is automatically called if this fails
      long duration = System.currentTimeMillis() - start;
      LOG.info("Committed in {} ms", duration);
    } catch (CommitStateUnknownException commitStateUnknownException) {
      cleanupOnAbort = false;
      throw commitStateUnknownException;
    }
  }

  private void abort(WriterCommitMessage[] messages) {
    if (cleanupOnAbort) {
      SparkCleanupUtil.deleteFiles("job abort", table.io(), files(messages));
    } else {
      LOG.warn("Skipping cleanup of written files");
    }
  }

  private List<DataFile> files(WriterCommitMessage[] messages) {
    List<DataFile> files = Lists.newArrayList();

    for (WriterCommitMessage message : messages) {
      if (message != null) {
        TaskCommit taskCommit = (TaskCommit) message;
        files.addAll(Arrays.asList(taskCommit.files()));
      }
    }

    return files;
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }

  private abstract class BaseBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return createWriterFactory();
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      SparkWrite.this.abort(messages);
    }

    @Override
    public String toString() {
      return String.format("IcebergBatchWrite(table=%s, format=%s)", table, format);
    }
  }

  private class BatchAppend extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
      AppendFiles append = table.newAppend();

      int numFiles = 0;
      for (DataFile file : files(messages)) {
        numFiles += 1;
        append.appendFile(file);
      }

      commitOperation(append, String.format("append with %d new data files", numFiles));
    }
  }

  private class DynamicOverwrite extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<DataFile> files = files(messages);

      if (files.isEmpty()) {
        LOG.info("Dynamic overwrite is empty, skipping commit");
        return;
      }

      ReplacePartitions dynamicOverwrite = table.newReplacePartitions();
      IsolationLevel isolationLevel = writeConf.isolationLevel();
      Long validateFromSnapshotId = writeConf.validateFromSnapshotId();

      if (isolationLevel != null && validateFromSnapshotId != null) {
        dynamicOverwrite.validateFromSnapshot(validateFromSnapshotId);
      }

      if (isolationLevel == SERIALIZABLE) {
        dynamicOverwrite.validateNoConflictingData();
        dynamicOverwrite.validateNoConflictingDeletes();

      } else if (isolationLevel == SNAPSHOT) {
        dynamicOverwrite.validateNoConflictingDeletes();
      }

      int numFiles = 0;
      for (DataFile file : files) {
        numFiles += 1;
        dynamicOverwrite.addFile(file);
      }

      commitOperation(
          dynamicOverwrite,
          String.format("dynamic partition overwrite with %d new data files", numFiles));
    }
  }

  private class OverwriteByFilter extends BaseBatchWrite {
    private final Expression overwriteExpr;

    private OverwriteByFilter(Expression overwriteExpr) {
      this.overwriteExpr = overwriteExpr;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();
      overwriteFiles.overwriteByRowFilter(overwriteExpr);

      int numFiles = 0;
      for (DataFile file : files(messages)) {
        numFiles += 1;
        overwriteFiles.addFile(file);
      }

      IsolationLevel isolationLevel = writeConf.isolationLevel();
      Long validateFromSnapshotId = writeConf.validateFromSnapshotId();

      if (isolationLevel != null && validateFromSnapshotId != null) {
        overwriteFiles.validateFromSnapshot(validateFromSnapshotId);
      }

      if (isolationLevel == SERIALIZABLE) {
        overwriteFiles.validateNoConflictingDeletes();
        overwriteFiles.validateNoConflictingData();

      } else if (isolationLevel == SNAPSHOT) {
        overwriteFiles.validateNoConflictingDeletes();
      }

      String commitMsg =
          String.format("overwrite by filter %s with %d new data files", overwriteExpr, numFiles);
      commitOperation(overwriteFiles, commitMsg);
    }
  }

  private class CopyOnWriteOperation extends BaseBatchWrite {
    private final SparkCopyOnWriteScan scan;
    private final IsolationLevel isolationLevel;

    private CopyOnWriteOperation(SparkCopyOnWriteScan scan, IsolationLevel isolationLevel) {
      this.scan = scan;
      this.isolationLevel = isolationLevel;
    }

    private List<DataFile> overwrittenFiles() {
      if (scan == null) {
        return ImmutableList.of();
      } else {
        return scan.tasks().stream().map(FileScanTask::file).collect(Collectors.toList());
      }
    }

    private Expression conflictDetectionFilter() {
      // the list of filter expressions may be empty but is never null
      List<Expression> scanFilterExpressions = scan.filterExpressions();

      Expression filter = Expressions.alwaysTrue();

      for (Expression expr : scanFilterExpressions) {
        filter = Expressions.and(filter, expr);
      }

      return filter;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();

      List<DataFile> overwrittenFiles = overwrittenFiles();
      int numOverwrittenFiles = overwrittenFiles.size();
      for (DataFile overwrittenFile : overwrittenFiles) {
        overwriteFiles.deleteFile(overwrittenFile);
      }

      int numAddedFiles = 0;
      for (DataFile file : files(messages)) {
        numAddedFiles += 1;
        overwriteFiles.addFile(file);
      }

      // the scan may be null if the optimizer replaces it with an empty relation (e.g. false cond)
      // no validation is needed in this case as the command does not depend on the table state
      if (scan != null) {
        if (isolationLevel == SERIALIZABLE) {
          commitWithSerializableIsolation(overwriteFiles, numOverwrittenFiles, numAddedFiles);
        } else if (isolationLevel == SNAPSHOT) {
          commitWithSnapshotIsolation(overwriteFiles, numOverwrittenFiles, numAddedFiles);
        } else {
          throw new IllegalArgumentException("Unsupported isolation level: " + isolationLevel);
        }

      } else {
        commitOperation(
            overwriteFiles,
            String.format("overwrite with %d new data files (no validation)", numAddedFiles));
      }
    }

    private void commitWithSerializableIsolation(
        OverwriteFiles overwriteFiles, int numOverwrittenFiles, int numAddedFiles) {
      Long scanSnapshotId = scan.snapshotId();
      if (scanSnapshotId != null) {
        overwriteFiles.validateFromSnapshot(scanSnapshotId);
      }

      Expression conflictDetectionFilter = conflictDetectionFilter();
      overwriteFiles.conflictDetectionFilter(conflictDetectionFilter);
      overwriteFiles.validateNoConflictingData();
      overwriteFiles.validateNoConflictingDeletes();

      String commitMsg =
          String.format(
              "overwrite of %d data files with %d new data files, scanSnapshotId: %d, conflictDetectionFilter: %s",
              numOverwrittenFiles, numAddedFiles, scanSnapshotId, conflictDetectionFilter);
      commitOperation(overwriteFiles, commitMsg);
    }

    private void commitWithSnapshotIsolation(
        OverwriteFiles overwriteFiles, int numOverwrittenFiles, int numAddedFiles) {
      Long scanSnapshotId = scan.snapshotId();
      if (scanSnapshotId != null) {
        overwriteFiles.validateFromSnapshot(scanSnapshotId);
      }

      Expression conflictDetectionFilter = conflictDetectionFilter();
      overwriteFiles.conflictDetectionFilter(conflictDetectionFilter);
      overwriteFiles.validateNoConflictingDeletes();

      String commitMsg =
          String.format(
              "overwrite of %d data files with %d new data files",
              numOverwrittenFiles, numAddedFiles);
      commitOperation(overwriteFiles, commitMsg);
    }
  }

  private class RewriteFiles extends BaseBatchWrite {
    private final String fileSetID;

    private RewriteFiles(String fileSetID) {
      this.fileSetID = fileSetID;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
      coordinator.stageRewrite(table, fileSetID, ImmutableSet.copyOf(files(messages)));
    }
  }

  private abstract class BaseStreamingWrite implements StreamingWrite {
    private static final String QUERY_ID_PROPERTY = "spark.sql.streaming.queryId";
    private static final String EPOCH_ID_PROPERTY = "spark.sql.streaming.epochId";

    protected abstract String mode();

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
      return createWriterFactory();
    }

    @Override
    public final void commit(long epochId, WriterCommitMessage[] messages) {
      LOG.info("Committing epoch {} for query {} in {} mode", epochId, queryId, mode());

      table.refresh();

      Long lastCommittedEpochId = findLastCommittedEpochId();
      if (lastCommittedEpochId != null && epochId <= lastCommittedEpochId) {
        LOG.info("Skipping epoch {} for query {} as it was already committed", epochId, queryId);
        return;
      }

      doCommit(epochId, messages);
    }

    protected abstract void doCommit(long epochId, WriterCommitMessage[] messages);

    protected <T> void commit(SnapshotUpdate<T> snapshotUpdate, long epochId, String description) {
      snapshotUpdate.set(QUERY_ID_PROPERTY, queryId);
      snapshotUpdate.set(EPOCH_ID_PROPERTY, Long.toString(epochId));
      commitOperation(snapshotUpdate, description);
    }

    private Long findLastCommittedEpochId() {
      Snapshot snapshot = table.currentSnapshot();
      Long lastCommittedEpochId = null;
      while (snapshot != null) {
        Map<String, String> summary = snapshot.summary();
        String snapshotQueryId = summary.get(QUERY_ID_PROPERTY);
        if (queryId.equals(snapshotQueryId)) {
          lastCommittedEpochId = Long.valueOf(summary.get(EPOCH_ID_PROPERTY));
          break;
        }
        Long parentSnapshotId = snapshot.parentId();
        snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
      }
      return lastCommittedEpochId;
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
      SparkWrite.this.abort(messages);
    }

    @Override
    public String toString() {
      return String.format("IcebergStreamingWrite(table=%s, format=%s)", table, format);
    }
  }

  private class StreamingAppend extends BaseStreamingWrite {
    @Override
    protected String mode() {
      return "append";
    }

    @Override
    protected void doCommit(long epochId, WriterCommitMessage[] messages) {
      AppendFiles append = table.newFastAppend();
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        append.appendFile(file);
        numFiles++;
      }
      commit(append, epochId, String.format("streaming append with %d new data files", numFiles));
    }
  }

  private class StreamingOverwrite extends BaseStreamingWrite {
    @Override
    protected String mode() {
      return "complete";
    }

    @Override
    public void doCommit(long epochId, WriterCommitMessage[] messages) {
      OverwriteFiles overwriteFiles = table.newOverwrite();
      overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
      int numFiles = 0;
      for (DataFile file : files(messages)) {
        overwriteFiles.addFile(file);
        numFiles++;
      }
      commit(
          overwriteFiles,
          epochId,
          String.format("streaming complete overwrite with %d new data files", numFiles));
    }
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] taskFiles;

    TaskCommit(DataFile[] taskFiles) {
      this.taskFiles = taskFiles;
    }

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    // Can only be called in executor.
    void reportOutputMetrics() {
      long bytesWritten = 0L;
      long recordsWritten = 0L;
      for (DataFile dataFile : taskFiles) {
        bytesWritten += dataFile.fileSizeInBytes();
        recordsWritten += dataFile.recordCount();
      }

      TaskContext taskContext = TaskContext$.MODULE$.get();
      if (taskContext != null) {
        OutputMetrics outputMetrics = taskContext.taskMetrics().outputMetrics();
        outputMetrics.setBytesWritten(bytesWritten);
        outputMetrics.setRecordsWritten(recordsWritten);
      }
    }

    DataFile[] files() {
      return taskFiles;
    }
  }

  public static class DeleteTaskCommit implements WriterCommitMessage {
    private final DeleteFile[] taskFiles;
    private final CharSequence[] referencedDataFiles;

    DeleteTaskCommit(List<DeleteFile> deleteFiles, List<CharSequence> referencedDataFiles) {
      this.taskFiles = deleteFiles.toArray(new DeleteFile[0]);
      this.referencedDataFiles = referencedDataFiles.toArray(new CharSequence[0]);
    }

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    // Can only be called in executor.
    void reportOutputMetrics() {
      long bytesWritten = 0L;
      long recordsWritten = 0L;
      for (DeleteFile dataFile : taskFiles) {
        bytesWritten += dataFile.fileSizeInBytes();
        recordsWritten += dataFile.recordCount();
      }

      TaskContext taskContext = TaskContext$.MODULE$.get();
      if (taskContext != null) {
        OutputMetrics outputMetrics = taskContext.taskMetrics().outputMetrics();
        outputMetrics.setBytesWritten(bytesWritten);
        outputMetrics.setRecordsWritten(recordsWritten);
      }
    }

    DeleteFile[] files() {
      return taskFiles;
    }

    CharSequence[] referencedDataFiles() {
      return referencedDataFiles;
    }
  }

  private static class WriterFactory implements DataWriterFactory, StreamingDataWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final FileFormat format;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final boolean partitionedFanoutEnabled;
    private final String queryId;

    protected WriterFactory(
        Broadcast<Table> tableBroadcast,
        String queryId,
        FileFormat format,
        long targetFileSize,
        Schema writeSchema,
        StructType dsSchema,
        boolean partitionedFanoutEnabled) {
      this.tableBroadcast = tableBroadcast;
      this.format = format;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.partitionedFanoutEnabled = partitionedFanoutEnabled;
      this.queryId = queryId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return createWriter(partitionId, taskId, 0);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      Table table = tableBroadcast.value();
      PartitionSpec spec = table.spec();
      FileIO io = table.io();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(format)
              .operationId(queryId)
              .build();
      SparkFileWriterFactory writerFactory =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(format)
              .dataSchema(writeSchema)
              .dataSparkType(dsSchema)
              .build();

      if (spec.isUnpartitioned()) {
        return new UnpartitionedDataWriter(writerFactory, fileFactory, io, spec, targetFileSize);

      } else {
        return new PartitionedDataWriter(
            writerFactory,
            fileFactory,
            io,
            spec,
            writeSchema,
            dsSchema,
            targetFileSize,
            partitionedFanoutEnabled);
      }
    }
  }

  private static class UnpartitionedDataWriter implements DataWriter<InternalRow> {
    private final FileWriter<InternalRow, DataWriteResult> delegate;
    private final FileIO io;

    private UnpartitionedDataWriter(
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        PartitionSpec spec,
        long targetFileSize) {
      this.delegate =
          new RollingDataWriter<>(writerFactory, fileFactory, io, targetFileSize, spec, null);
      this.io = io;
    }

    @Override
    public void write(InternalRow record) throws IOException {
      delegate.write(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      DataWriteResult result = delegate.result();
      TaskCommit taskCommit = new TaskCommit(result.dataFiles().toArray(new DataFile[0]));
      taskCommit.reportOutputMetrics();
      return taskCommit;
    }

    @Override
    public void abort() throws IOException {
      close();

      DataWriteResult result = delegate.result();
      SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles());
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static class PartitionedDataWriter implements DataWriter<InternalRow> {
    private final PartitioningWriter<InternalRow, DataWriteResult> delegate;
    private final FileIO io;
    private final PartitionSpec spec;
    private final PartitionKey partitionKey;
    private final InternalRowWrapper internalRowWrapper;

    private PartitionedDataWriter(
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        PartitionSpec spec,
        Schema dataSchema,
        StructType dataSparkType,
        long targetFileSize,
        boolean fanoutEnabled) {
      if (fanoutEnabled) {
        this.delegate = new FanoutDataWriter<>(writerFactory, fileFactory, io, targetFileSize);
      } else {
        this.delegate = new ClusteredDataWriter<>(writerFactory, fileFactory, io, targetFileSize);
      }
      this.io = io;
      this.spec = spec;
      this.partitionKey = new PartitionKey(spec, dataSchema);
      this.internalRowWrapper = new InternalRowWrapper(dataSparkType);
    }

    @Override
    public void write(InternalRow row) throws IOException {
      partitionKey.partition(internalRowWrapper.wrap(row));
      delegate.write(row, spec, partitionKey);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      DataWriteResult result = delegate.result();
      TaskCommit taskCommit = new TaskCommit(result.dataFiles().toArray(new DataFile[0]));
      taskCommit.reportOutputMetrics();
      return taskCommit;
    }

    @Override
    public void abort() throws IOException {
      close();

      DataWriteResult result = delegate.result();
      SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles());
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  class PositionDeleteBatchWrite implements BatchWrite {

    private String fileSetID;

    private PositionDeleteBatchWrite(String fileSetID) {
      this.fileSetID = fileSetID;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      // broadcast the table metadata as the writer factory will be sent to executors
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      return new PositionDeltaWriteFactory(
          tableBroadcast, queryId, format, targetFileSize, writeSchema, dsSchema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      PositionDeletesRewriteCoordinator coordinator = PositionDeletesRewriteCoordinator.get();
      coordinator.stageRewrite(table, fileSetID, ImmutableSet.copyOf(files(messages)));
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      if (cleanupOnAbort) {
        SparkCleanupUtil.deleteFiles("job abort", table.io(), files(messages));
      } else {
        LOG.warn("Skipping cleanup of written files");
      }
    }

    private List<DeleteFile> files(WriterCommitMessage[] messages) {
      List<DeleteFile> files = Lists.newArrayList();

      for (WriterCommitMessage message : messages) {
        if (message != null) {
          DeleteTaskCommit taskCommit = (DeleteTaskCommit) message;
          files.addAll(Arrays.asList(taskCommit.files()));
        }
      }

      return files;
    }
  }

  static class PositionDeltaWriteFactory implements DataWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final String queryId;
    private final FileFormat format;
    private final Long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;

    PositionDeltaWriteFactory(
        Broadcast<Table> tableBroadcast,
        String queryId,
        FileFormat format,
        long targetFileSize,
        Schema writeSchema,
        StructType dsSchema) {
      this.tableBroadcast = tableBroadcast;
      this.queryId = queryId;
      this.format = format;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();

      OutputFileFactory deleteFileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(format)
              .operationId(queryId)
              .suffix("deletes")
              .build();

      Schema positionDeleteRowSchema =
          new Schema(
              writeSchema
                  .findField(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME)
                  .type()
                  .asStructType()
                  .fields());
      StructType deleteFileType =
          new StructType(
              new StructField[] {
                dsSchema.apply(MetadataColumns.DELETE_FILE_PATH.name()),
                dsSchema.apply(MetadataColumns.DELETE_FILE_POS.name()),
                dsSchema.apply(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME)
              });

      SparkFileWriterFactory writerFactoryWithRow =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(format)
              .dataSchema(writeSchema)
              .dataSparkType(dsSchema)
              .deleteFileFormat(format)
              .positionDeleteRowSchema(positionDeleteRowSchema)
              .positionDeleteSparkType(deleteFileType)
              .build();

      SparkFileWriterFactory writerFactoryWithoutRow =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(format)
              .dataSchema(writeSchema)
              .dataSparkType(dsSchema)
              .deleteFileFormat(format)
              .positionDeleteSparkType(deleteFileType)
              .build();

      return new DeleteWriter(
          table,
          writerFactoryWithRow,
          writerFactoryWithoutRow,
          deleteFileFactory,
          targetFileSize,
          dsSchema);
    }
  }

  private static class DeleteWriter implements DataWriter<InternalRow> {
    private final ClusteredPositionDeleteWriter<InternalRow> writerWithRow;
    private final ClusteredPositionDeleteWriter<InternalRow> writerWithoutRow;
    private final PositionDelete<InternalRow> positionDelete;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specs;
    private final InternalRowWrapper partitionRowWrapper;
    private final Map<Integer, StructProjection> partitionProjections;
    private final int specIdOrdinal;
    private final Option<Integer> partitionOrdinalOption;
    private final int fileOrdinal;
    private final int positionOrdinal;
    private final int rowOrdinal;
    private final int rowSize;

    private boolean closed = false;

    /**
     * Writer for position deletes metadata table.
     *
     * <p>Delete files need to either have 'row' as required field, or omit 'row' altogether, for
     * delete file stats accuracy Hence, this is a fanout writer, redirecting rows with null 'row'
     * to one delegate, and non-null 'row' to another
     *
     * @param table position deletes metadata table
     * @param writerFactoryWithRow writer factory for deletes with non-null 'row'
     * @param writerFactoryWithoutRow writer factory for deletes with null 'row'
     * @param deleteFileFactory delete file factory
     * @param targetFileSize target file size
     * @param dsSchema schema of incoming dataset
     */
    DeleteWriter(
        Table table,
        SparkFileWriterFactory writerFactoryWithRow,
        SparkFileWriterFactory writerFactoryWithoutRow,
        OutputFileFactory deleteFileFactory,
        long targetFileSize,
        StructType dsSchema) {
      this.writerWithRow =
          new ClusteredPositionDeleteWriter<>(
              writerFactoryWithRow, deleteFileFactory, table.io(), targetFileSize);
      this.writerWithoutRow =
          new ClusteredPositionDeleteWriter<>(
              writerFactoryWithoutRow, deleteFileFactory, table.io(), targetFileSize);
      this.positionDelete = PositionDelete.create();
      this.io = table.io();
      this.specs = table.specs();

      Types.StructType partitionType = Partitioning.partitionType(table);

      this.specIdOrdinal = dsSchema.fieldIndex(PositionDeletesTable.SPEC_ID);
      this.partitionOrdinalOption =
          dsSchema.getFieldIndex(PositionDeletesTable.PARTITION).map(a -> (Integer) a);
      this.partitionRowWrapper = initPartitionRowWrapper(partitionType);
      this.partitionProjections = buildPartitionProjections(partitionType, specs);
      this.fileOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_PATH.name());
      this.positionOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_POS.name());

      this.rowOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME);
      DataType type = dsSchema.apply(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME).dataType();
      Preconditions.checkArgument(
          type instanceof StructType, "Expected row as struct type but was %s", type);
      this.rowSize = ((StructType) type).size();
    }

    @Override
    public void write(InternalRow record) throws IOException {
      int specId = record.getInt(specIdOrdinal);
      PartitionSpec spec = specs.get(specId);

      InternalRow partition = null;
      if (partitionOrdinalOption.isDefined()) {
        int partitionOrdinal = partitionOrdinalOption.get();
        partition = record.getStruct(partitionOrdinal, partitionRowWrapper.size());
      }
      StructProjection partitionProjection = partitionProjections.get(specId);
      partitionProjection.wrap(partitionRowWrapper.wrap(partition));

      String file = record.getString(fileOrdinal);
      long position = record.getLong(positionOrdinal);
      InternalRow row = record.getStruct(rowOrdinal, rowSize);
      if (row != null) {
        positionDelete.set(file, position, row);
        writerWithRow.write(positionDelete, spec, partitionProjection);
      } else {
        positionDelete.set(file, position, null);
        writerWithoutRow.write(positionDelete, spec, partitionProjection);
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      DeleteWriteResult resultWithRow = writerWithRow.result();
      DeleteWriteResult resultWithoutRow = writerWithoutRow.result();
      List<DeleteFile> allDeleteFiles =
          Lists.newArrayList(
              Iterables.concat(resultWithRow.deleteFiles(), resultWithoutRow.deleteFiles()));
      List<CharSequence> allReferencedDataFiles =
          Lists.newArrayList(
              Iterables.concat(
                  resultWithRow.referencedDataFiles(), resultWithoutRow.referencedDataFiles()));
      return new DeleteTaskCommit(allDeleteFiles, allReferencedDataFiles);
    }

    @Override
    public void abort() throws IOException {
      close();

      DeleteWriteResult resultWithRow = writerWithRow.result();
      DeleteWriteResult resultWithoutRow = writerWithoutRow.result();
      SparkCleanupUtil.deleteTaskFiles(
          io,
          Lists.newArrayList(
              Iterables.concat(resultWithRow.deleteFiles(), resultWithoutRow.deleteFiles())));
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        writerWithRow.close();
        writerWithoutRow.close();
        this.closed = true;
      }
    }

    protected InternalRowWrapper initPartitionRowWrapper(Types.StructType partitionType) {
      StructType sparkPartitionType = (StructType) SparkSchemaUtil.convert(partitionType);
      return new InternalRowWrapper(sparkPartitionType);
    }

    protected Map<Integer, StructProjection> buildPartitionProjections(
        Types.StructType partitionType, Map<Integer, PartitionSpec> partitionSpecs) {
      Map<Integer, StructProjection> result = Maps.newHashMap();
      partitionSpecs.forEach(
          (specID, spec) ->
              result.put(specID, StructProjection.create(partitionType, spec.partitionType())));
      return result;
    }
  }
}
