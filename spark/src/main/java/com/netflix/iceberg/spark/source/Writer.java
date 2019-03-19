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

package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.AppendFiles;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.encryption.EncryptedOutputFile;
import com.netflix.iceberg.encryption.EncryptionManager;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.LocationProvider;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.data.SparkAvroWriter;
import com.netflix.iceberg.util.Tasks;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static com.netflix.iceberg.spark.SparkSchemaUtil.convert;

// TODO: parameterize DataSourceWriter with subclass of WriterCommitMessage
class Writer implements DataSourceWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final Table table;
  private final FileFormat format;
  private final FileIO fileIo;
  private final EncryptionManager encryptionManager;

  Writer(Table table, FileFormat format) {
    this.table = table;
    this.format = format;
    this.fileIo = table.io();
    this.encryptionManager = table.encryption();
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new WriterFactory(
        table.spec(), format, table.locationProvider(), table.properties(), fileIo, encryptionManager);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    AppendFiles append = table.newAppend();

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    LOG.info("Appending {} files to {}", numFiles, table);
    long start = System.currentTimeMillis();
    append.commit(); // abort is automatically called if this fails
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    Tasks.foreach(files(messages))
        .retry(propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */ )
        .throwFailureWhenFinished()
        .run(file -> {
          fileIo.deleteFile(file.path().toString());
        });
  }

  private Iterable<DataFile> files(WriterCommitMessage[] messages) {
    if (messages.length > 0) {
      return concat(transform(Arrays.asList(messages), message -> message != null
          ? ImmutableList.copyOf(((TaskCommit) message).files())
          : ImmutableList.of()));
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
    return String.format("IcebergWrite(table=%s, type=%s, format=%s)",
        table, table.schema().asStruct(), format);
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
    private final String uuid = UUID.randomUUID().toString();
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;

    WriterFactory(PartitionSpec spec, FileFormat format, LocationProvider locations,
                  Map<String, String> properties, FileIO fileIo, EncryptionManager encryptionManager) {
      this.spec = spec;
      this.format = format;
      this.locations = locations;
      this.properties = properties;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
      String filename = format.addExtension(String.format("%05d-%d-%s", partitionId, taskId, uuid));
      AppenderFactory<InternalRow> factory = new SparkAppenderFactory();
      if (spec.fields().isEmpty()) {
        OutputFile outputFile = fileIo.newOutputFile(locations.newDataLocation(filename));
        return new UnpartitionedWriter(encryptionManager.encrypt(outputFile), format, factory, fileIo);
      } else {
        Function<PartitionKey, EncryptedOutputFile> newOutputFileForKey =
            key -> {
              OutputFile rawOutputFile = fileIo.newOutputFile(locations.newDataLocation(spec, key, filename));
              return encryptionManager.encrypt(rawOutputFile);
            };
        return new PartitionedWriter(spec, format, factory, newOutputFileForKey, fileIo);
      }
    }

    private class SparkAppenderFactory implements AppenderFactory<InternalRow> {
      public FileAppender<InternalRow> newAppender(OutputFile file, FileFormat format) {
        Schema schema = spec.schema();
        try {
          switch (format) {
            case PARQUET:
              String jsonSchema = convert(schema).json();
              return Parquet.write(file)
                  .writeSupport(new ParquetWriteSupport())
                  .set("org.apache.spark.sql.parquet.row.attributes", jsonSchema)
                  .set("spark.sql.parquet.writeLegacyFormat", "false")
                  .set("spark.sql.parquet.binaryAsString", "false")
                  .set("spark.sql.parquet.int96AsTimestamp", "false")
                  .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
                  .setAll(properties)
                  .schema(schema)
                  .build();

            case AVRO:
              return Avro.write(file)
                  .createWriterFunc(ignored -> new SparkAvroWriter(schema))
                  .setAll(properties)
                  .schema(schema)
                  .build();

            default:
              throw new UnsupportedOperationException("Cannot write unknown format: " + format);
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }
  }

  private interface AppenderFactory<T> {
    FileAppender<T> newAppender(OutputFile file, FileFormat format);
  }

  private static class UnpartitionedWriter implements DataWriter<InternalRow>, Closeable {
    private final FileIO fileIo;
    private FileAppender<InternalRow> appender = null;
    private Metrics metrics = null;
    private final EncryptedOutputFile file;

    UnpartitionedWriter(
        EncryptedOutputFile outputFile,
        FileFormat format,
        AppenderFactory<InternalRow> factory,
        FileIO fileIo) {
      this.fileIo = fileIo;
      this.file = outputFile;
      this.appender = factory.newAppender(file.encryptingOutputFile(), format);
    }

    @Override
    public void write(InternalRow record) {
      appender.add(record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      Preconditions.checkArgument(appender != null, "Commit called on a closed writer: %s", this);

      close();

      if (metrics.recordCount() == 0L) {
        fileIo.deleteFile(file.encryptingOutputFile());
        return new TaskCommit();
      }

      DataFile dataFile = DataFiles.fromEncryptedOutputFile(file, null, metrics);

      return new TaskCommit(dataFile);
    }

    @Override
    public void abort() throws IOException {
      Preconditions.checkArgument(appender != null, "Abort called on a closed writer: %s", this);

      close();
      fileIo.deleteFile(file.encryptingOutputFile());
    }

    @Override
    public void close() throws IOException {
      if (this.appender != null) {
        this.appender.close();
        this.metrics = appender.metrics();
        this.appender = null;
      }
    }
  }

  private static class PartitionedWriter implements DataWriter<InternalRow> {
    private final Set<PartitionKey> completedPartitions = Sets.newHashSet();
    private final List<DataFile> completedFiles = Lists.newArrayList();
    private final PartitionSpec spec;
    private final FileFormat format;
    private final AppenderFactory<InternalRow> factory;
    private final Function<PartitionKey, EncryptedOutputFile> newOutputFileForKey;
    private final PartitionKey key;
    private final FileIO fileIo;

    private PartitionKey currentKey = null;
    private FileAppender<InternalRow> currentAppender = null;
    private EncryptedOutputFile currentFile = null;

    PartitionedWriter(
        PartitionSpec spec,
        FileFormat format,
        AppenderFactory<InternalRow> factory,
        Function<PartitionKey, EncryptedOutputFile> newOutputFileForKey,
        FileIO fileIo) {
      this.spec = spec;
      this.format = format;
      this.factory = factory;
      this.newOutputFileForKey = newOutputFileForKey;
      this.key = new PartitionKey(spec);
      this.fileIo = fileIo;
    }

    @Override
    public void write(InternalRow row) throws IOException {
      key.partition(row);

      if (!key.equals(currentKey)) {
        closeCurrent();

        if (completedPartitions.contains(key)) {
          // if rows are not correctly grouped, detect and fail the write
          PartitionKey existingKey = Iterables.find(completedPartitions, key::equals, null);
          LOG.warn("Duplicate key: {} == {}", existingKey, key);
          throw new IllegalStateException("Already closed file for partition: " + key.toPath());
        }

        this.currentKey = key.copy();
        this.currentFile = newOutputFileForKey.apply(currentKey);
        this.currentAppender = factory.newAppender(currentFile.encryptingOutputFile(), format);
      }

      currentAppender.add(row);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      closeCurrent();
      return new TaskCommit(completedFiles);
    }

    @Override
    public void abort() throws IOException {
      // clean up files created by this writer
      Tasks.foreach(completedFiles)
          .throwFailureWhenFinished()
          .noRetry()
          .run(file -> fileIo.deleteFile(file.path().toString()));

      if (currentAppender != null) {
        currentAppender.close();
        this.currentAppender = null;
        fileIo.deleteFile(currentFile.encryptingOutputFile());
      }
    }

    private void closeCurrent() throws IOException {
      if (currentAppender != null) {
        currentAppender.close();
        // metrics are only valid after the appender is closed
        Metrics metrics = currentAppender.metrics();
        this.currentAppender = null;

        DataFile dataFile = DataFiles.builder(spec)
            .withEncryptedOutputFile(currentFile)
            .withPartition(currentKey)
            .withMetrics(metrics)
            .build();

        completedPartitions.add(currentKey);
        completedFiles.add(dataFile);
      }
    }
  }
}
