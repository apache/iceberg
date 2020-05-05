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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
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

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final Table table;
  private final FileFormat format;
  private final Broadcast<FileIO> io;
  private final Broadcast<EncryptionManager> encryptionManager;
  private final boolean replacePartitions;
  private final String applicationId;
  private final String wapId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;

  Writer(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
         DataSourceOptions options, boolean replacePartitions, String applicationId, Schema writeSchema,
         StructType dsSchema) {
    this(table, io, encryptionManager, options, replacePartitions, applicationId, null, writeSchema, dsSchema);
  }

  Writer(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
         DataSourceOptions options, boolean replacePartitions, String applicationId, String wapId,
         Schema writeSchema, StructType dsSchema) {
    this.table = table;
    this.format = getFileFormat(table.properties(), options);
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.replacePartitions = replacePartitions;
    this.applicationId = applicationId;
    this.wapId = wapId;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;

    long tableTargetFileSize = PropertyUtil.propertyAsLong(
        table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    this.targetFileSize = options.getLong("target-file-size-bytes", tableTargetFileSize);
  }

  private FileFormat getFileFormat(Map<String, String> tableProperties, DataSourceOptions options) {
    Optional<String> formatOption = options.get("write-format");
    String formatString = formatOption
        .orElse(tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private boolean isWapTable() {
    return Boolean.parseBoolean(table.properties().getOrDefault(
        TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED_DEFAULT));
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new WriterFactory(
        table.spec(), format, table.locationProvider(), table.properties(), io, encryptionManager, targetFileSize,
        writeSchema, dsSchema);
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

  @Override
  public void abort(WriterCommitMessage[] messages) {
    Tasks.foreach(files(messages))
        .retry(propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
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

  private int propertyAsInt(String property, int defaultValue) {
    Map<String, String> properties = table.properties();
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  @Override
  public String toString() {
    return String.format("IcebergWrite(table=%s, format=%s)", table, format);
  }


  private static class TaskCommit implements WriterCommitMessage {
    private final DataFile[] files;

    TaskCommit() {
      this.files = new DataFile[0];
    }

    TaskCommit(DataFile file) {
      this.files = new DataFile[] { file };
    }

    TaskCommit(List<DataFile> files) {
      this.files = files.toArray(new DataFile[files.size()]);
    }

    DataFile[] files() {
      return files;
    }
  }

  private static class WriterFactory implements DataWriterFactory<InternalRow> {
    private final PartitionSpec spec;
    private final FileFormat format;
    private final LocationProvider locations;
    private final Map<String, String> properties;
    private final Broadcast<FileIO> io;
    private final Broadcast<EncryptionManager> encryptionManager;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;

    WriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
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
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
      OutputFileFactory fileFactory = new OutputFileFactory(partitionId, taskId, epochId);
      AppenderFactory<InternalRow> appenderFactory = new SparkAppenderFactory();

      if (spec.fields().isEmpty()) {
        return new UnpartitionedWriter(spec, format, appenderFactory, fileFactory, io.value(), targetFileSize);
      } else {
        return new PartitionedWriter(
            spec, format, appenderFactory, fileFactory, io.value(), targetFileSize, writeSchema);
      }
    }

    private class SparkAppenderFactory implements AppenderFactory<InternalRow> {
      @Override
      public FileAppender<InternalRow> newAppender(OutputFile file, FileFormat fileFormat) {
        MetricsConfig metricsConfig = MetricsConfig.fromProperties(properties);
        try {
          switch (fileFormat) {
            case PARQUET:
              return Parquet.write(file)
                  .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(dsSchema, msgType))
                  .setAll(properties)
                  .metricsConfig(metricsConfig)
                  .schema(writeSchema)
                  .overwrite()
                  .build();

            case AVRO:
              return Avro.write(file)
                  .createWriterFunc(ignored -> new SparkAvroWriter(dsSchema))
                  .setAll(properties)
                  .schema(writeSchema)
                  .overwrite()
                  .build();

            case ORC:
              return ORC.write(file)
                  .createWriterFunc(SparkOrcWriter::new)
                  .setAll(properties)
                  .schema(writeSchema)
                  .overwrite()
                  .build();

            default:
              throw new UnsupportedOperationException("Cannot write unknown format: " + fileFormat);
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }

    private class OutputFileFactory {
      private final int partitionId;
      private final long taskId;
      // The purpose of this uuid is to be able to know from two paths that they were written by the same operation.
      // That's useful, for example, if a Spark job dies and leaves files in the file system, you can identify them all
      // with a recursive listing and grep.
      private final String uuid = UUID.randomUUID().toString();
      private int fileCount;

      OutputFileFactory(int partitionId, long taskId, long epochId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.fileCount = 0;
      }

      private String generateFilename() {
        return format.addExtension(String.format("%05d-%d-%s-%05d", partitionId, taskId, uuid, fileCount++));
      }

      /**
       * Generates EncryptedOutputFile for UnpartitionedWriter.
       */
      public EncryptedOutputFile newOutputFile() {
        OutputFile file = io.value().newOutputFile(locations.newDataLocation(generateFilename()));
        return encryptionManager.value().encrypt(file);
      }

      /**
       * Generates EncryptedOutputFile for PartitionedWriter.
       */
      public EncryptedOutputFile newOutputFile(PartitionKey key) {
        String newDataLocation = locations.newDataLocation(spec, key, generateFilename());
        OutputFile rawOutputFile = io.value().newOutputFile(newDataLocation);
        return encryptionManager.value().encrypt(rawOutputFile);
      }
    }
  }

  private interface AppenderFactory<T> {
    FileAppender<T> newAppender(OutputFile file, FileFormat format);
  }

  private abstract static class BaseWriter implements DataWriter<InternalRow> {
    protected static final int ROWS_DIVISOR = 1000;

    private final List<DataFile> completedFiles = Lists.newArrayList();
    private final PartitionSpec spec;
    private final FileFormat format;
    private final AppenderFactory<InternalRow> appenderFactory;
    private final WriterFactory.OutputFileFactory fileFactory;
    private final FileIO fileIo;
    private final long targetFileSize;
    private PartitionKey currentKey = null;
    private FileAppender<InternalRow> currentAppender = null;
    private EncryptedOutputFile currentFile = null;
    private long currentRows = 0;

    BaseWriter(PartitionSpec spec, FileFormat format, AppenderFactory<InternalRow> appenderFactory,
               WriterFactory.OutputFileFactory fileFactory, FileIO fileIo, long targetFileSize) {
      this.spec = spec;
      this.format = format;
      this.appenderFactory = appenderFactory;
      this.fileFactory = fileFactory;
      this.fileIo = fileIo;
      this.targetFileSize = targetFileSize;
    }

    @Override
    public abstract void write(InternalRow row) throws IOException;

    public void writeInternal(InternalRow row)  throws IOException {
      //TODO: ORC file now not support target file size before closed
      if  (!format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && currentAppender.length() >= targetFileSize) {
        closeCurrent();
        openCurrent();
      }

      currentAppender.add(row);
      currentRows++;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      closeCurrent();

      return new TaskCommit(completedFiles);
    }

    @Override
    public void abort() throws IOException {
      closeCurrent();

      // clean up files created by this writer
      Tasks.foreach(completedFiles)
          .throwFailureWhenFinished()
          .noRetry()
          .run(file -> fileIo.deleteFile(file.path().toString()));
    }

    protected void openCurrent() {
      if (spec.fields().size() == 0) {
        // unpartitioned
        currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        currentFile = fileFactory.newOutputFile(currentKey);
      }
      currentAppender = appenderFactory.newAppender(currentFile.encryptingOutputFile(), format);
      currentRows = 0;
    }

    protected void closeCurrent() throws IOException {
      if (currentAppender != null) {
        currentAppender.close();
        // metrics are only valid after the appender is closed
        Metrics metrics = currentAppender.metrics();
        long fileSizeInBytes = currentAppender.length();
        List<Long> splitOffsets = currentAppender.splitOffsets();
        this.currentAppender = null;

        if (metrics.recordCount() == 0L) {
          fileIo.deleteFile(currentFile.encryptingOutputFile());
        } else {
          DataFile dataFile = DataFiles.builder(spec)
              .withEncryptionKeyMetadata(currentFile.keyMetadata())
              .withPath(currentFile.encryptingOutputFile().location())
              .withFileSizeInBytes(fileSizeInBytes)
              .withPartition(spec.fields().size() == 0 ? null : currentKey) // set null if unpartitioned
              .withMetrics(metrics)
              .withSplitOffsets(splitOffsets)
              .build();
          completedFiles.add(dataFile);
        }

        this.currentFile = null;
      }
    }

    protected PartitionKey getCurrentKey() {
      return currentKey;
    }

    protected void setCurrentKey(PartitionKey currentKey) {
      this.currentKey = currentKey;
    }
  }

  private static class UnpartitionedWriter extends BaseWriter {
    UnpartitionedWriter(
        PartitionSpec spec,
        FileFormat format,
        AppenderFactory<InternalRow> appenderFactory,
        WriterFactory.OutputFileFactory fileFactory,
        FileIO fileIo,
        long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize);

      openCurrent();
    }

    @Override
    public void write(InternalRow row) throws IOException {
      writeInternal(row);
    }
  }

  private static class PartitionedWriter extends BaseWriter {
    private final PartitionKey key;
    private final Set<PartitionKey> completedPartitions = Sets.newHashSet();

    PartitionedWriter(
        PartitionSpec spec,
        FileFormat format,
        AppenderFactory<InternalRow> appenderFactory,
        WriterFactory.OutputFileFactory fileFactory,
        FileIO fileIo,
        long targetFileSize,
        Schema writeSchema) {
      super(spec, format, appenderFactory, fileFactory, fileIo, targetFileSize);
      this.key = new PartitionKey(spec, writeSchema);
    }

    @Override
    public void write(InternalRow row) throws IOException {
      key.partition(row);

      PartitionKey currentKey = getCurrentKey();
      if (!key.equals(currentKey)) {
        closeCurrent();
        completedPartitions.add(currentKey);

        if (completedPartitions.contains(key)) {
          // if rows are not correctly grouped, detect and fail the write
          PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
          LOG.warn("Duplicate key: {} == {}", existingKey, key);
          throw new IllegalStateException("Already closed files for partition: " + key.toPath());
        }

        setCurrentKey(key.copy());
        openCurrent();
      }

      writeInternal(row);
    }
  }
}
