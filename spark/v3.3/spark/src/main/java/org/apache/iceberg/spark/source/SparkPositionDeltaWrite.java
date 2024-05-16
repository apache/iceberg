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
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.BasePositionDeltaWriter;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.PositionDeltaWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.iceberg.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.iceberg.write.DeltaWrite;
import org.apache.spark.sql.connector.iceberg.write.DeltaWriter;
import org.apache.spark.sql.connector.iceberg.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.iceberg.write.ExtendedLogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkPositionDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPositionDeltaWrite.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final Command command;
  private final SparkBatchQueryScan scan;
  private final IsolationLevel isolationLevel;
  private final Context context;
  private final String applicationId;
  private final boolean wapEnabled;
  private final String wapId;
  private final String branch;
  private final Map<String, String> extraSnapshotMetadata;
  private final Distribution requiredDistribution;
  private final SortOrder[] requiredOrdering;

  private boolean cleanupOnAbort = true;

  SparkPositionDeltaWrite(
      SparkSession spark,
      Table table,
      Command command,
      SparkBatchQueryScan scan,
      IsolationLevel isolationLevel,
      SparkWriteConf writeConf,
      ExtendedLogicalWriteInfo info,
      Schema dataSchema,
      Distribution requiredDistribution,
      SortOrder[] requiredOrdering) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.command = command;
    this.scan = scan;
    this.isolationLevel = isolationLevel;
    this.context = new Context(dataSchema, writeConf, info);
    this.applicationId = spark.sparkContext().applicationId();
    this.wapEnabled = writeConf.wapEnabled();
    this.wapId = writeConf.wapId();
    this.branch = writeConf.branch();
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
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

  @Override
  public DeltaBatchWrite toBatch() {
    return new PositionDeltaBatchWrite();
  }

  private class PositionDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      // broadcast the table metadata as the writer factory will be sent to executors
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      return new PositionDeltaWriteFactory(tableBroadcast, command, context);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      RowDelta rowDelta = table.newRowDelta();

      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

      int addedDataFilesCount = 0;
      int addedDeleteFilesCount = 0;

      for (WriterCommitMessage message : messages) {
        DeltaTaskCommit taskCommit = (DeltaTaskCommit) message;

        for (DataFile dataFile : taskCommit.dataFiles()) {
          rowDelta.addRows(dataFile);
          addedDataFilesCount += 1;
        }

        for (DeleteFile deleteFile : taskCommit.deleteFiles()) {
          rowDelta.addDeletes(deleteFile);
          addedDeleteFilesCount += 1;
        }

        referencedDataFiles.addAll(Arrays.asList(taskCommit.referencedDataFiles()));
      }

      // the scan may be null if the optimizer replaces it with an empty relation
      // no validation is needed in this case as the command is independent of the table state
      if (scan != null) {
        Expression conflictDetectionFilter = conflictDetectionFilter(scan);
        rowDelta.conflictDetectionFilter(conflictDetectionFilter);

        rowDelta.validateDataFilesExist(referencedDataFiles);

        if (scan.snapshotId() != null) {
          // set the read snapshot ID to check only snapshots that happened after the table was read
          // otherwise, the validation will go through all snapshots present in the table
          rowDelta.validateFromSnapshot(scan.snapshotId());
        }

        if (command == UPDATE || command == MERGE) {
          rowDelta.validateDeletedFiles();
          rowDelta.validateNoConflictingDeleteFiles();
        }

        if (isolationLevel == SERIALIZABLE) {
          rowDelta.validateNoConflictingDataFiles();
        }

        String commitMsg =
            String.format(
                "position delta with %d data files and %d delete files "
                    + "(scanSnapshotId: %d, conflictDetectionFilter: %s, isolationLevel: %s)",
                addedDataFilesCount,
                addedDeleteFilesCount,
                scan.snapshotId(),
                conflictDetectionFilter,
                isolationLevel);
        commitOperation(rowDelta, commitMsg);

      } else {
        String commitMsg =
            String.format(
                "position delta with %d data files and %d delete files (no validation required)",
                addedDataFilesCount, addedDeleteFilesCount);
        commitOperation(rowDelta, commitMsg);
      }
    }

    private Expression conflictDetectionFilter(SparkBatchQueryScan queryScan) {
      Expression filter = Expressions.alwaysTrue();

      for (Expression expr : queryScan.filterExpressions()) {
        filter = Expressions.and(filter, expr);
      }

      return filter;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      if (cleanupOnAbort) {
        SparkCleanupUtil.deleteFiles("job abort", table.io(), files(messages));
      } else {
        LOG.warn("Skipping cleanup of written files");
      }
    }

    private List<ContentFile<?>> files(WriterCommitMessage[] messages) {
      List<ContentFile<?>> files = Lists.newArrayList();

      for (WriterCommitMessage message : messages) {
        if (message != null) {
          DeltaTaskCommit taskCommit = (DeltaTaskCommit) message;
          files.addAll(Arrays.asList(taskCommit.dataFiles()));
          files.addAll(Arrays.asList(taskCommit.deleteFiles()));
        }
      }

      return files;
    }

    private void commitOperation(SnapshotUpdate<?> operation, String description) {
      LOG.info("Committing {} to table {}", description, table);
      if (applicationId != null) {
        operation.set("spark.app.id", applicationId);
      }

      extraSnapshotMetadata.forEach(operation::set);

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
  }

  public static class DeltaTaskCommit implements WriterCommitMessage {
    private final DataFile[] dataFiles;
    private final DeleteFile[] deleteFiles;
    private final CharSequence[] referencedDataFiles;

    DeltaTaskCommit(WriteResult result) {
      this.dataFiles = result.dataFiles();
      this.deleteFiles = result.deleteFiles();
      this.referencedDataFiles = result.referencedDataFiles();
    }

    DeltaTaskCommit(DeleteWriteResult result) {
      this.dataFiles = new DataFile[0];
      this.deleteFiles = result.deleteFiles().toArray(new DeleteFile[0]);
      this.referencedDataFiles = result.referencedDataFiles().toArray(new CharSequence[0]);
    }

    DataFile[] dataFiles() {
      return dataFiles;
    }

    DeleteFile[] deleteFiles() {
      return deleteFiles;
    }

    CharSequence[] referencedDataFiles() {
      return referencedDataFiles;
    }
  }

  private static class PositionDeltaWriteFactory implements DeltaWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final Command command;
    private final Context context;

    PositionDeltaWriteFactory(Broadcast<Table> tableBroadcast, Command command, Context context) {
      this.tableBroadcast = tableBroadcast;
      this.command = command;
      this.context = context;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();

      OutputFileFactory dataFileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(context.dataFileFormat())
              .operationId(context.queryId())
              .build();
      OutputFileFactory deleteFileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(context.deleteFileFormat())
              .operationId(context.queryId())
              .suffix("deletes")
              .build();

      SparkFileWriterFactory writerFactory =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(context.dataFileFormat())
              .dataSchema(context.dataSchema())
              .dataSparkType(context.dataSparkType())
              .deleteFileFormat(context.deleteFileFormat())
              .positionDeleteSparkType(context.deleteSparkType())
              .build();

      if (command == DELETE) {
        return new DeleteOnlyDeltaWriter(table, writerFactory, deleteFileFactory, context);

      } else if (table.spec().isUnpartitioned()) {
        return new UnpartitionedDeltaWriter(
            table, writerFactory, dataFileFactory, deleteFileFactory, context);

      } else {
        return new PartitionedDeltaWriter(
            table, writerFactory, dataFileFactory, deleteFileFactory, context);
      }
    }
  }

  private abstract static class BaseDeltaWriter implements DeltaWriter<InternalRow> {

    protected InternalRowWrapper initPartitionRowWrapper(Types.StructType partitionType) {
      StructType sparkPartitionType = (StructType) SparkSchemaUtil.convert(partitionType);
      return new InternalRowWrapper(sparkPartitionType, partitionType);
    }

    protected Map<Integer, StructProjection> buildPartitionProjections(
        Types.StructType partitionType, Map<Integer, PartitionSpec> specs) {
      Map<Integer, StructProjection> partitionProjections = Maps.newHashMap();
      specs.forEach(
          (specID, spec) ->
              partitionProjections.put(
                  specID, StructProjection.create(partitionType, spec.partitionType())));
      return partitionProjections;
    }
  }

  private static class DeleteOnlyDeltaWriter extends BaseDeltaWriter {
    private final ClusteredPositionDeleteWriter<InternalRow> delegate;
    private final PositionDelete<InternalRow> positionDelete;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specs;
    private final InternalRowWrapper partitionRowWrapper;
    private final Map<Integer, StructProjection> partitionProjections;
    private final int specIdOrdinal;
    private final int partitionOrdinal;
    private final int fileOrdinal;
    private final int positionOrdinal;

    private boolean closed = false;

    DeleteOnlyDeltaWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory deleteFileFactory,
        Context context) {

      this.delegate =
          new ClusteredPositionDeleteWriter<>(
              writerFactory, deleteFileFactory, table.io(), context.targetDeleteFileSize());
      this.positionDelete = PositionDelete.create();
      this.io = table.io();
      this.specs = table.specs();

      Types.StructType partitionType = Partitioning.partitionType(table);
      this.partitionRowWrapper = initPartitionRowWrapper(partitionType);
      this.partitionProjections = buildPartitionProjections(partitionType, specs);

      this.specIdOrdinal = context.metadataSparkType().fieldIndex(MetadataColumns.SPEC_ID.name());
      this.partitionOrdinal =
          context.metadataSparkType().fieldIndex(MetadataColumns.PARTITION_COLUMN_NAME);
      this.fileOrdinal = context.deleteSparkType().fieldIndex(MetadataColumns.FILE_PATH.name());
      this.positionOrdinal =
          context.deleteSparkType().fieldIndex(MetadataColumns.ROW_POSITION.name());
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      int specId = metadata.getInt(specIdOrdinal);
      PartitionSpec spec = specs.get(specId);

      InternalRow partition = metadata.getStruct(partitionOrdinal, partitionRowWrapper.size());
      StructProjection partitionProjection = partitionProjections.get(specId);
      partitionProjection.wrap(partitionRowWrapper.wrap(partition));

      String file = id.getString(fileOrdinal);
      long position = id.getLong(positionOrdinal);
      positionDelete.set(file, position, null);
      delegate.write(positionDelete, spec, partitionProjection);
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement update");
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement insert");
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      DeleteWriteResult result = delegate.result();
      return new DeltaTaskCommit(result);
    }

    @Override
    public void abort() throws IOException {
      close();

      DeleteWriteResult result = delegate.result();
      SparkCleanupUtil.deleteTaskFiles(io, result.deleteFiles());
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        this.closed = true;
      }
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class DeleteAndDataDeltaWriter extends BaseDeltaWriter {
    protected final PositionDeltaWriter<InternalRow> delegate;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specs;
    private final InternalRowWrapper deletePartitionRowWrapper;
    private final Map<Integer, StructProjection> deletePartitionProjections;
    private final int specIdOrdinal;
    private final int partitionOrdinal;
    private final int fileOrdinal;
    private final int positionOrdinal;

    private boolean closed = false;

    DeleteAndDataDeltaWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory dataFileFactory,
        OutputFileFactory deleteFileFactory,
        Context context) {
      this.delegate =
          new BasePositionDeltaWriter<>(
              newInsertWriter(table, writerFactory, dataFileFactory, context),
              newUpdateWriter(table, writerFactory, dataFileFactory, context),
              newDeleteWriter(table, writerFactory, deleteFileFactory, context));
      this.io = table.io();
      this.specs = table.specs();

      Types.StructType partitionType = Partitioning.partitionType(table);
      this.deletePartitionRowWrapper = initPartitionRowWrapper(partitionType);
      this.deletePartitionProjections = buildPartitionProjections(partitionType, specs);

      this.specIdOrdinal = context.metadataSparkType().fieldIndex(MetadataColumns.SPEC_ID.name());
      this.partitionOrdinal =
          context.metadataSparkType().fieldIndex(MetadataColumns.PARTITION_COLUMN_NAME);
      this.fileOrdinal = context.deleteSparkType().fieldIndex(MetadataColumns.FILE_PATH.name());
      this.positionOrdinal =
          context.deleteSparkType().fieldIndex(MetadataColumns.ROW_POSITION.name());
    }

    @Override
    public void delete(InternalRow meta, InternalRow id) throws IOException {
      int specId = meta.getInt(specIdOrdinal);
      PartitionSpec spec = specs.get(specId);

      InternalRow partition = meta.getStruct(partitionOrdinal, deletePartitionRowWrapper.size());
      StructProjection partitionProjection = deletePartitionProjections.get(specId);
      partitionProjection.wrap(deletePartitionRowWrapper.wrap(partition));

      String file = id.getString(fileOrdinal);
      long position = id.getLong(positionOrdinal);
      delegate.delete(file, position, spec, partitionProjection);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      WriteResult result = delegate.result();
      return new DeltaTaskCommit(result);
    }

    @Override
    public void abort() throws IOException {
      close();

      WriteResult result = delegate.result();
      SparkCleanupUtil.deleteTaskFiles(io, files(result));
    }

    private List<ContentFile<?>> files(WriteResult result) {
      List<ContentFile<?>> files = Lists.newArrayList();
      files.addAll(Arrays.asList(result.dataFiles()));
      files.addAll(Arrays.asList(result.deleteFiles()));
      return files;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        this.closed = true;
      }
    }

    private PartitioningWriter<InternalRow, DataWriteResult> newInsertWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        Context context) {
      long targetFileSize = context.targetDataFileSize();

      if (table.spec().isPartitioned() && context.fanoutWriterEnabled()) {
        return new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), targetFileSize);
      } else {
        return new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), targetFileSize);
      }
    }

    private PartitioningWriter<InternalRow, DataWriteResult> newUpdateWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        Context context) {
      long targetFileSize = context.targetDataFileSize();

      if (table.spec().isPartitioned()) {
        // use a fanout writer for partitioned tables to write updates as they may be out of order
        return new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), targetFileSize);
      } else {
        return new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), targetFileSize);
      }
    }

    private ClusteredPositionDeleteWriter<InternalRow> newDeleteWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        Context context) {
      long targetFileSize = context.targetDeleteFileSize();
      return new ClusteredPositionDeleteWriter<>(
          writerFactory, fileFactory, table.io(), targetFileSize);
    }
  }

  private static class UnpartitionedDeltaWriter extends DeleteAndDataDeltaWriter {
    private final PartitionSpec dataSpec;

    UnpartitionedDeltaWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory dataFileFactory,
        OutputFileFactory deleteFileFactory,
        Context context) {
      super(table, writerFactory, dataFileFactory, deleteFileFactory, context);
      this.dataSpec = table.spec();
    }

    @Override
    public void update(InternalRow meta, InternalRow id, InternalRow row) throws IOException {
      delete(meta, id);
      delegate.update(row, dataSpec, null);
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      delegate.insert(row, dataSpec, null);
    }
  }

  private static class PartitionedDeltaWriter extends DeleteAndDataDeltaWriter {
    private final PartitionSpec dataSpec;
    private final PartitionKey dataPartitionKey;
    private final InternalRowWrapper internalRowDataWrapper;

    PartitionedDeltaWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory dataFileFactory,
        OutputFileFactory deleteFileFactory,
        Context context) {
      super(table, writerFactory, dataFileFactory, deleteFileFactory, context);

      this.dataSpec = table.spec();
      this.dataPartitionKey = new PartitionKey(dataSpec, context.dataSchema());
      this.internalRowDataWrapper =
          new InternalRowWrapper(context.dataSparkType(), context.dataSchema().asStruct());
    }

    @Override
    public void update(InternalRow meta, InternalRow id, InternalRow row) throws IOException {
      delete(meta, id);
      dataPartitionKey.partition(internalRowDataWrapper.wrap(row));
      delegate.update(row, dataSpec, dataPartitionKey);
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      dataPartitionKey.partition(internalRowDataWrapper.wrap(row));
      delegate.insert(row, dataSpec, dataPartitionKey);
    }
  }

  // a serializable helper class for common parameters required to configure writers
  private static class Context implements Serializable {
    private final Schema dataSchema;
    private final StructType dataSparkType;
    private final FileFormat dataFileFormat;
    private final long targetDataFileSize;
    private final StructType deleteSparkType;
    private final StructType metadataSparkType;
    private final FileFormat deleteFileFormat;
    private final long targetDeleteFileSize;
    private final boolean fanoutWriterEnabled;
    private final String queryId;

    Context(Schema dataSchema, SparkWriteConf writeConf, ExtendedLogicalWriteInfo info) {
      this.dataSchema = dataSchema;
      this.dataSparkType = info.schema();
      this.dataFileFormat = writeConf.dataFileFormat();
      this.targetDataFileSize = writeConf.targetDataFileSize();
      this.deleteSparkType = info.rowIdSchema();
      this.deleteFileFormat = writeConf.deleteFileFormat();
      this.targetDeleteFileSize = writeConf.targetDeleteFileSize();
      this.metadataSparkType = info.metadataSchema();
      this.fanoutWriterEnabled = writeConf.fanoutWriterEnabled();
      this.queryId = info.queryId();
    }

    Schema dataSchema() {
      return dataSchema;
    }

    StructType dataSparkType() {
      return dataSparkType;
    }

    FileFormat dataFileFormat() {
      return dataFileFormat;
    }

    long targetDataFileSize() {
      return targetDataFileSize;
    }

    StructType deleteSparkType() {
      return deleteSparkType;
    }

    StructType metadataSparkType() {
      return metadataSparkType;
    }

    FileFormat deleteFileFormat() {
      return deleteFileFormat;
    }

    long targetDeleteFileSize() {
      return targetDeleteFileSize;
    }

    boolean fanoutWriterEnabled() {
      return fanoutWriterEnabled;
    }

    String queryId() {
      return queryId;
    }
  }
}
