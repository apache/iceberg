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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

class SparkBatchWrite implements BatchWrite {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchWrite.class);

  private final Table table;
  private final FileFormat format;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final boolean overwriteDynamic;
  private final boolean overwriteByFilter;
  private final Expression overwriteExpr;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;

  SparkBatchWrite(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
                  CaseInsensitiveStringMap options, boolean overwriteDynamic, boolean overwriteByFilter,
                  Expression overwriteExpr, String applicationId, String wapId, Schema writeSchema,
                  StructType dsSchema) {
    this.table = table;
    this.format = getFileFormat(table.properties(), options);
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.overwriteDynamic = overwriteDynamic;
    this.overwriteByFilter = overwriteByFilter;
    this.overwriteExpr = overwriteExpr;
    this.applicationId = applicationId;
    this.wapId = wapId;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = Maps.newHashMap();

    options.forEach((key, value) -> {
      if (key.startsWith(SnapshotSummary.EXTRA_METADATA_PREFIX)) {
        extraSnapshotMetadata.put(key.substring(SnapshotSummary.EXTRA_METADATA_PREFIX.length()), value);
      }
    });

    long tableTargetFileSize = PropertyUtil.propertyAsLong(
        table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = options.getLong("target-file-size-bytes", tableTargetFileSize);
  }

  protected FileFormat getFileFormat(Map<String, String> tableProperties, Map<String, String> options) {
    Optional<String> formatOption = Optional.ofNullable(options.get("write-format"));
    String formatString = formatOption
        .orElse(tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(table.properties().getOrDefault(
        TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  @Override
  public WriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new WriterFactory(
        table.spec(), format, table.locationProvider(), table.properties(), io, encryptionManager, targetFileSize,
        writeSchema, dsSchema);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (overwriteDynamic) {
      replacePartitions(messages);
    } else if (overwriteByFilter) {
      overwrite(messages);
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
      extraSnapshotMetadata.forEach((key, value) -> operation.set(key, value));
    }

    if (isWapTable() && wapId != null) {
      // write-audit-publish is enabled for this table and job
      // stage the changes without changing the current snapshot
      operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
      operation.stageOnly();
    }

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
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
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      dynamicOverwrite.addFile(file);
    }

    commitOperation(dynamicOverwrite, numFiles, "dynamic partition overwrite");
  }

  private void overwrite(WriterCommitMessage[] messages) {
    OverwriteFiles overwriteFiles = table.newOverwrite();
    overwriteFiles.overwriteByRowFilter(overwriteExpr);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      overwriteFiles.addFile(file);
    }

    commitOperation(overwriteFiles, numFiles, "overwrite by filter");
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    Map<String, String> props = table.properties();
    Tasks.foreach(files(messages))
        .retry(PropertyUtil.propertyAsInt(props, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(props, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(props, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(props, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .throwFailureWhenFinished()
        .run(file -> {
          io.value().deleteFile(file.path().toString());
        });
  }

  protected Table table() {
    return table;
  }

  protected Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return Iterables.concat(Iterables.transform(Arrays.asList(messages), message -> message != null ?
          ImmutableList.copyOf(((TaskCommit) message).files()) :
          ImmutableList.of()));
    }
    return ImmutableList.of();
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] taskFiles;

    TaskCommit(DataFile[] taskFiles) {
      this.taskFiles = taskFiles;
    }

    DataFile[] files() {
      return taskFiles;
    }
  }

  private static class WriterFactory implements DataWriterFactory, StreamingDataWriterFactory {
    private final PartitionSpec spec;
    private final FileFormat format;
    private final LocationProvider locations;
    private final Map<String, String> properties;
    private final Broadcast<FileIO> io;
    private final Broadcast<EncryptionManager> encryptionManager;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;

    protected WriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
                            Map<String, String> properties, Broadcast<FileIO> io,
                            Broadcast<EncryptionManager> encryptionManager, long targetFileSize,
                            Schema writeSchema, StructType dsSchema) {
      this.spec = spec;
      this.format = format;
      this.locations = locations;
      this.properties = properties;
      this.io = io;
      this.encryptionManager = encryptionManager;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return createWriter(partitionId, taskId, 0);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
      OutputFileFactory fileFactory = new OutputFileFactory(
          spec, format, locations, io.value(), encryptionManager.value(), partitionId, taskId);
      SparkAppenderFactory appenderFactory = new SparkAppenderFactory(properties, writeSchema, dsSchema);
      if (spec.fields().isEmpty()) {
        return new Unpartitioned3Writer(spec, format, appenderFactory, fileFactory, io.value(), targetFileSize);
      } else {
        return new Partitioned3Writer(
            spec, format, appenderFactory, fileFactory, io.value(), targetFileSize, writeSchema, dsSchema);
      }
    }
  }

  private static class Unpartitioned3Writer extends UnpartitionedWriter<InternalRow>
      implements DataWriter<InternalRow> {
    Unpartitioned3Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                         OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      this.close();

      return new TaskCommit(complete());
    }
  }

  private static class Partitioned3Writer extends SparkPartitionedWriter implements DataWriter<InternalRow> {
    Partitioned3Writer(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
                       OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                       Schema schema, StructType sparkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, sparkSchema);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      this.close();

      return new TaskCommit(complete());
    }
  }
}
