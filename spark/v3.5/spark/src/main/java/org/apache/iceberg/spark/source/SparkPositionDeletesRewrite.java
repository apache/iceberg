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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * {@link Write} class for rewriting position delete files from Spark. Responsible for creating
 * {@link SparkPositionDeletesRewrite.PositionDeleteBatchWrite}
 *
 * <p>This class is meant to be used for an action to rewrite position delete files. Hence, it
 * assumes all position deletes to rewrite have come from {@link ScanTaskSetManager} and that all
 * have the same partition spec id and partition values.
 *
 * <p>Writing position delete with row data is no longer supported since iceberg 1.12. Such position
 * delete files have to be removed during data compaction or rewritten as DV when upgrade to v3,
 * before this action can run.
 */
public class SparkPositionDeletesRewrite implements Write {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final long targetFileSize;
  private final DeleteGranularity deleteGranularity;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final String fileSetId;
  private final int specId;
  private final StructLike partition;
  private final Map<String, String> writeProperties;

  /**
   * Constructs a {@link SparkPositionDeletesRewrite}.
   *
   * @param spark Spark session
   * @param table instance of {@link PositionDeletesTable}
   * @param writeConf Spark write config
   * @param writeInfo Spark write info
   * @param writeSchema Iceberg output schema
   * @param dsSchema schema of original incoming position deletes dataset
   * @param specId spec id of position deletes
   * @param partition partition value of position deletes
   */
  SparkPositionDeletesRewrite(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      LogicalWriteInfo writeInfo,
      Schema writeSchema,
      StructType dsSchema,
      int specId,
      StructLike partition) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.queryId = writeInfo.queryId();
    this.format = writeConf.deleteFileFormat();
    this.targetFileSize = writeConf.targetDeleteFileSize();
    this.deleteGranularity = writeConf.deleteGranularity();
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.fileSetId = writeConf.rewrittenFileSetId();
    this.specId = specId;
    this.partition = partition;
    this.writeProperties = writeConf.writeProperties();
  }

  @Override
  public BatchWrite toBatch() {
    return new PositionDeleteBatchWrite();
  }

  /** {@link BatchWrite} class for rewriting position deletes files from Spark */
  class PositionDeleteBatchWrite implements BatchWrite {

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      // broadcast the table metadata as the writer factory will be sent to executors
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      return new PositionDeletesWriterFactory(
          tableBroadcast,
          queryId,
          format,
          targetFileSize,
          deleteGranularity,
          writeSchema,
          dsSchema,
          specId,
          partition,
          writeProperties);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      PositionDeletesRewriteCoordinator coordinator = PositionDeletesRewriteCoordinator.get();
      coordinator.stageRewrite(table, fileSetId, DeleteFileSet.of(files(messages)));
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      SparkCleanupUtil.deleteFiles("job abort", table.io(), files(messages));
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

  /**
   * Writer factory for position deletes metadata table. Responsible for creating {@link
   * DeleteWriter}.
   *
   * <p>This writer is meant to be used for an action to rewrite delete files. Hence, it makes an
   * assumption that all incoming deletes belong to the same partition, and that incoming dataset is
   * from {@link ScanTaskSetManager}.
   */
  static class PositionDeletesWriterFactory implements DataWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final String queryId;
    private final FileFormat format;
    private final Long targetFileSize;
    private final DeleteGranularity deleteGranularity;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final int specId;
    private final StructLike partition;
    private final Map<String, String> writeProperties;

    PositionDeletesWriterFactory(
        Broadcast<Table> tableBroadcast,
        String queryId,
        FileFormat format,
        long targetFileSize,
        DeleteGranularity deleteGranularity,
        Schema writeSchema,
        StructType dsSchema,
        int specId,
        StructLike partition,
        Map<String, String> writeProperties) {
      this.tableBroadcast = tableBroadcast;
      this.queryId = queryId;
      this.format = format;
      this.targetFileSize = targetFileSize;
      this.deleteGranularity = deleteGranularity;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.specId = specId;
      this.partition = partition;
      this.writeProperties = writeProperties;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();
      int formatVersion = TableUtil.formatVersion(table);

      OutputFileFactory deleteFileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(formatVersion >= 3 ? FileFormat.PUFFIN : format)
              .operationId(queryId)
              .suffix("deletes")
              .build();

      if (formatVersion >= 3) {
        return new DVWriter(table, deleteFileFactory, dsSchema, specId, partition);
      } else {
        SparkFileWriterFactory writerFactory =
            SparkFileWriterFactory.builderFor(table)
                .deleteFileFormat(format)
                .writeProperties(writeProperties)
                .build();

        return new DeleteWriter(
            table,
            writerFactory,
            deleteFileFactory,
            targetFileSize,
            deleteGranularity,
            dsSchema,
            specId,
            partition);
      }
    }
  }

  /**
   * Writer for position deletes metadata table.
   *
   * <p>Position deletes that carry row data are rejected. The rewritten file does not include the
   * row column.
   *
   * <p>This writer is meant to be used for an action to rewrite delete files. Hence, it makes an
   * assumption that all incoming deletes belong to the same partition.
   */
  private static class DeleteWriter implements DataWriter<InternalRow> {
    private final SparkFileWriterFactory writerFactory;
    private final OutputFileFactory deleteFileFactory;
    private final long targetFileSize;
    private final DeleteGranularity deleteGranularity;
    private final PositionDelete<InternalRow> positionDelete;
    private final FileIO io;
    private final PartitionSpec spec;
    private final int fileOrdinal;
    private final int positionOrdinal;
    private final int rowOrdinal;
    private final int rowSize;
    private final StructLike partition;

    private ClusteredPositionDeleteWriter<InternalRow> writer;
    private boolean closed = false;

    /**
     * Constructs a {@link DeleteWriter}.
     *
     * @param table position deletes metadata table
     * @param writerFactory writer factory for position deletes
     * @param deleteFileFactory delete file factory
     * @param targetFileSize target file size
     * @param dsSchema schema of incoming dataset of position deletes
     * @param specId partition spec id of incoming position deletes. All incoming partition deletes
     *     are required to have the same spec id.
     * @param partition partition value of incoming position delete. All incoming partition deletes
     *     are required to have the same partition.
     */
    DeleteWriter(
        Table table,
        SparkFileWriterFactory writerFactory,
        OutputFileFactory deleteFileFactory,
        long targetFileSize,
        DeleteGranularity deleteGranularity,
        StructType dsSchema,
        int specId,
        StructLike partition) {
      this.deleteFileFactory = deleteFileFactory;
      this.targetFileSize = targetFileSize;
      this.deleteGranularity = deleteGranularity;
      this.writerFactory = writerFactory;
      this.positionDelete = PositionDelete.create();
      this.io = table.io();
      this.spec = table.specs().get(specId);
      this.partition = partition;

      this.fileOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_PATH.name());
      this.positionOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_POS.name());
      this.rowOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME);
      DataType rowType = dsSchema.apply(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME).dataType();
      Preconditions.checkArgument(
          rowType instanceof StructType, "Expected row as struct type but was %s", rowType);
      this.rowSize = ((StructType) rowType).size();
    }

    @Override
    public void write(InternalRow record) {
      String file = record.getString(fileOrdinal);
      long position = record.getLong(positionOrdinal);
      Preconditions.checkArgument(
          record.getStruct(rowOrdinal, rowSize) == null,
          "Cannot rewrite position deletes with row data for data file %s at position %s",
          file,
          position);

      positionDelete.set(file, position);
      lazyWriter().write(positionDelete, spec, partition);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();
      return new DeleteTaskCommit(allDeleteFiles());
    }

    @Override
    public void abort() throws IOException {
      close();
      SparkCleanupUtil.deleteTaskFiles(io, allDeleteFiles());
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        if (writer != null) {
          writer.close();
        }
        this.closed = true;
      }
    }

    private ClusteredPositionDeleteWriter<InternalRow> lazyWriter() {
      if (writer == null) {
        this.writer =
            new ClusteredPositionDeleteWriter<>(
                writerFactory, deleteFileFactory, io, targetFileSize, deleteGranularity);
      }
      return writer;
    }

    private List<DeleteFile> allDeleteFiles() {
      List<DeleteFile> allDeleteFiles = Lists.newArrayList();
      if (writer != null) {
        allDeleteFiles.addAll(writer.result().deleteFiles());
      }
      return allDeleteFiles;
    }
  }

  /**
   * DV Writer for position deletes metadata table.
   *
   * <p>This writer is meant to be used for an action to rewrite delete files when the table
   * supports DVs.
   */
  private static class DVWriter implements DataWriter<InternalRow> {
    private final PositionDelete<InternalRow> positionDelete;
    private final FileIO io;
    private final PartitionSpec spec;
    private final int fileOrdinal;
    private final int positionOrdinal;
    private final StructLike partition;
    private final PartitioningDVWriter<InternalRow> dvWriter;
    private boolean closed = false;

    /**
     * Constructs a {@link DeleteWriter}.
     *
     * @param table position deletes metadata table
     * @param deleteFileFactory delete file factory
     * @param dsSchema schema of incoming dataset of position deletes
     * @param specId partition spec id of incoming position deletes. All incoming partition deletes
     *     are required to have the same spec id.
     * @param partition partition value of incoming position delete. All incoming partition deletes
     *     are required to have the same partition.
     */
    DVWriter(
        Table table,
        OutputFileFactory deleteFileFactory,
        StructType dsSchema,
        int specId,
        StructLike partition) {
      this.positionDelete = PositionDelete.create();
      this.io = table.io();
      this.spec = table.specs().get(specId);
      this.partition = partition;
      this.fileOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_PATH.name());
      this.positionOrdinal = dsSchema.fieldIndex(MetadataColumns.DELETE_FILE_POS.name());
      this.dvWriter = new PartitioningDVWriter<>(deleteFileFactory, p -> null);
    }

    @Override
    public void write(InternalRow record) {
      String file = record.getString(fileOrdinal);
      long position = record.getLong(positionOrdinal);
      positionDelete.set(file, position);
      dvWriter.write(positionDelete, spec, partition);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();
      return new DeleteTaskCommit(allDeleteFiles());
    }

    @Override
    public void abort() throws IOException {
      close();
      SparkCleanupUtil.deleteTaskFiles(io, allDeleteFiles());
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        if (null != dvWriter) {
          dvWriter.close();
        }
        this.closed = true;
      }
    }

    private List<DeleteFile> allDeleteFiles() {
      return dvWriter.result().deleteFiles();
    }
  }

  public static class DeleteTaskCommit implements WriterCommitMessage {
    private final DeleteFile[] taskFiles;

    DeleteTaskCommit(List<DeleteFile> deleteFiles) {
      this.taskFiles = deleteFiles.toArray(new DeleteFile[0]);
    }

    DeleteFile[] files() {
      return taskFiles;
    }
  }
}
