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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final FileFormat format;
  private final boolean replacePartitions;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final boolean partitionedFanoutEnabled;

  private boolean cleanupOnAbort = true;

  Writer(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      boolean replacePartitions,
      String applicationId,
      Schema writeSchema,
      StructType dsSchema) {
    this(spark, table, writeConf, replacePartitions, applicationId, null, writeSchema, dsSchema);
  }

  Writer(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      boolean replacePartitions,
      String applicationId,
      String wapId,
      Schema writeSchema,
      StructType dsSchema) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.format = writeConf.dataFileFormat();
    this.replacePartitions = replacePartitions;
    this.applicationId = applicationId;
    this.wapId = wapId;
    this.targetFileSize = writeConf.targetDataFileSize();
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
    this.partitionedFanoutEnabled = writeConf.fanoutWriterEnabled();
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(
        table
            .properties()
            .getOrDefault(
                TableProperties.WRITE_AUDIT_PUBLISH_ENABLED,
                TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    // broadcast the table metadata as the writer factory will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    return new WriterFactory(
        tableBroadcast, format, targetFileSize, writeSchema, dsSchema, partitionedFanoutEnabled);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (replacePartitions) {
      replacePartitions(messages);
    } else {
      append(messages);
    }
  }

  protected void commitOperation(SnapshotUpdate<?> operation, int numFiles, String description) {
    LOG.info("Committing {} with {} files to table {}", description, numFiles, table);
    if (applicationId != null) {
      operation.set("spark.app.id", applicationId);
    }

    if (!extraSnapshotMetadata.isEmpty()) {
      extraSnapshotMetadata.forEach(operation::set);
    }

    if (isWapTable() && wapId != null) {
      // write-audit-publish is enabled for this table and job
      // stage the changes without changing the current snapshot
      operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
      operation.stageOnly();
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

  private void append(WriterCommitMessage[] messages) {
    AppendFiles append = table.newAppend();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "append");
  }

  private void replacePartitions(WriterCommitMessage[] messages) {
    Iterable<DataFile> files = files(messages);

    if (!files.iterator().hasNext()) {
      LOG.info("Dyanmic overwrite is empty, skipping commit");
      return;
    }

    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (DataFile file : files) {
      numFiles += 1;
      dynamicOverwrite.addFile(file);
    }

    commitOperation(dynamicOverwrite, numFiles, "dynamic partition overwrite");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    if (cleanupOnAbort) {
      Map<String, String> props = table.properties();
      Tasks.foreach(files(messages))
          .retry(PropertyUtil.propertyAsInt(props, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              PropertyUtil.propertyAsInt(
                  props, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(
                  props, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              PropertyUtil.propertyAsInt(
                  props, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .throwFailureWhenFinished()
          .run(
              file -> {
                table.io().deleteFile(file.path().toString());
              });
    } else {
      LOG.warn(
          "Skipping cleaning up of data files because Iceberg was unable to determine the final commit state");
    }
  }

  protected Table table() {
    return table;
  }

  protected Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return Iterables.concat(
          Iterables.transform(
              Arrays.asList(messages),
              message ->
                  message != null
                      ? ImmutableList.copyOf(((TaskCommit) message).files())
                      : ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }

  private static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] taskFiles;

    TaskCommit(DataFile[] files) {
      this.taskFiles = files;
    }

    DataFile[] files() {
      return this.taskFiles;
    }
  }

  static class WriterFactory implements DataWriterFactory<InternalRow> {
    private final Broadcast<Table> tableBroadcast;
    private final FileFormat format;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final boolean partitionedFanoutEnabled;

    WriterFactory(
        Broadcast<Table> tableBroadcast,
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
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
      Table table = tableBroadcast.value();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId).format(format).build();
      SparkAppenderFactory appenderFactory =
          SparkAppenderFactory.builderFor(table, writeSchema, dsSchema).build();

      PartitionSpec spec = table.spec();
      FileIO io = table.io();

      if (spec.isUnpartitioned()) {
        return new Unpartitioned24Writer(
            spec, format, appenderFactory, fileFactory, io, targetFileSize);
      } else if (partitionedFanoutEnabled) {
        return new PartitionedFanout24Writer(
            spec, format, appenderFactory, fileFactory, io, targetFileSize, writeSchema, dsSchema);
      } else {
        return new Partitioned24Writer(
            spec, format, appenderFactory, fileFactory, io, targetFileSize, writeSchema, dsSchema);
      }
    }
  }

  private static class Unpartitioned24Writer extends UnpartitionedWriter<InternalRow>
      implements DataWriter<InternalRow> {
    Unpartitioned24Writer(
        PartitionSpec spec,
        FileFormat format,
        SparkAppenderFactory appenderFactory,
        OutputFileFactory fileFactory,
        FileIO fileIo,
        long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class Partitioned24Writer extends SparkPartitionedWriter
      implements DataWriter<InternalRow> {

    Partitioned24Writer(
        PartitionSpec spec,
        FileFormat format,
        SparkAppenderFactory appenderFactory,
        OutputFileFactory fileFactory,
        FileIO fileIo,
        long targetFileSize,
        Schema schema,
        StructType sparkSchema) {
      super(
          spec, format, appenderFactory, fileFactory, fileIo, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }

  private static class PartitionedFanout24Writer extends SparkPartitionedFanoutWriter
      implements DataWriter<InternalRow> {

    PartitionedFanout24Writer(
        PartitionSpec spec,
        FileFormat format,
        SparkAppenderFactory appenderFactory,
        OutputFileFactory fileFactory,
        FileIO fileIo,
        long targetFileSize,
        Schema schema,
        StructType sparkSchema) {
      super(
          spec, format, appenderFactory, fileFactory, fileIo, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      return new TaskCommit(dataFiles());
    }
  }
}
