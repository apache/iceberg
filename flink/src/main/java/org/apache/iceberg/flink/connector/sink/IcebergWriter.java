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

package org.apache.iceberg.flink.connector.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@SuppressWarnings("checkstyle:ClassTypeParameterName")
public class IcebergWriter<IN> extends AbstractStreamOperator<FlinkDataFile>
    implements OneInputStreamOperator<IN, FlinkDataFile> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

  private final RecordSerializer<IN> serializer;
  private final String namespace;
  private final String tableName;
  private final FileFormat format;
  private final boolean skipIncompatibleRecord;
  private final Schema schema;
  private final PartitionSpec spec;
  private final LocationProvider locations;
  private final FileIO io;
  private final Map<String, String> properties;
  private final String timestampFeild;
  private final TimeUnit timestampUnit;
  private final long maxFileSize;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;
  private transient Map<String, FileWriter> openPartitionFiles;
  private transient int subtaskId;
  private transient ProcessingTimeService timerService;
  private transient Partitioner<Record> partitioner;

  public IcebergWriter(Table table, @Nullable RecordSerializer<IN> serializer, Configuration config) {
    this.serializer = serializer;
    skipIncompatibleRecord = config.getBoolean(IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD,
        IcebergConnectorConstant.DEFAULT_SKIP_INCOMPATIBLE_RECORD);
    timestampFeild = config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_FIELD, "");
    timestampUnit = TimeUnit.valueOf(config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_UNIT,
        IcebergConnectorConstant.DEFAULT_WATERMARK_TIMESTAMP_UNIT));
    maxFileSize = config.getLong(IcebergConnectorConstant.MAX_FILE_SIZE,
        IcebergConnectorConstant.DEFAULT_MAX_FILE_SIZE);

    namespace = config.getString(IcebergConnectorConstant.NAMESPACE, "");
    tableName = config.getString(IcebergConnectorConstant.TABLE, "");

    schema = table.schema();
    spec = table.spec();
    locations = table.locationProvider();
    io = table.io();
    properties = table.properties();
    format = FileFormat.valueOf(
        properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH));

    LOG.info("Iceberg writer {}.{} data file location: {}",
        namespace, tableName, locations.newDataLocation(""));
    LOG.info("Iceberg writer {}.{} created with sink config", namespace, tableName);
    LOG.info("Iceberg writer {}.{} loaded table: schema = {}\npartition spec = {}",
        namespace, tableName, schema, spec);

    // default ChainingStrategy is set to HEAD
    // we prefer chaining to avoid the huge serialization and deserializatoin overhead.
    super.setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() throws Exception {
    super.open();

    hadoopConf = new org.apache.hadoop.conf.Configuration();
    subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    timerService = getProcessingTimeService();
    openPartitionFiles = new HashMap<>();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    LOG.info("Iceberg writer {}.{} subtask {} begin preparing for checkpoint {}",
        namespace, tableName, subtaskId, checkpointId);
    // close all open files and emit files to downstream committer operator
    flush(true);
    LOG.info("Iceberg writer {}.{} subtask {} completed preparing for checkpoint {}",
        namespace, tableName, subtaskId, checkpointId);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @VisibleForTesting
  List<FlinkDataFile> flush(boolean emit) throws IOException {
    List<FlinkDataFile> dataFiles = new ArrayList<>(openPartitionFiles.size());
    for (Map.Entry<String, FileWriter> entry : openPartitionFiles.entrySet()) {
      FileWriter writer = entry.getValue();
      FlinkDataFile flinkDataFile = closeWriter(writer);
      dataFiles.add(flinkDataFile);
      if (emit) {
        emit(flinkDataFile);
      }
    }
    LOG.info("Iceberg writer {}.{} subtask {} flushed {} open files",
        namespace, tableName, subtaskId, openPartitionFiles.size());
    openPartitionFiles.clear();
    return dataFiles;
  }

  FlinkDataFile closeWriter(FileWriter writer) throws IOException {
    FlinkDataFile flinkDataFile = writer.close();
    LOG.info(
        "Iceberg writer {}.{} subtask {} uploaded to Iceberg table {}.{} with {} records and {} bytes on this path: {}",
        namespace, tableName, subtaskId, namespace, tableName, flinkDataFile.getIcebergDataFile().recordCount(),
        flinkDataFile.getIcebergDataFile().fileSizeInBytes(), flinkDataFile.getIcebergDataFile().path());
    return flinkDataFile;
  }

  void emit(FlinkDataFile flinkDataFile) {
    output.collect(new StreamRecord<>(flinkDataFile));
    LOG.debug("Iceberg writer {}.{} subtask {} emitted uploaded file to committer for Iceberg table {}.{}" +
        " with {} records and {} bytes on this path: {}",
        namespace, tableName, subtaskId, namespace, tableName, flinkDataFile.getIcebergDataFile().recordCount(),
        flinkDataFile.getIcebergDataFile().fileSizeInBytes(), flinkDataFile.getIcebergDataFile().path());
  }

  @Override
  public void close() throws Exception {
    super.close();

    LOG.info("Iceberg writer {}.{} subtask {} begin close", namespace, tableName, subtaskId);
    // close all open files without emitting to downstream committer
    flush(false);
    LOG.info("Iceberg writer {}.{} subtask {} completed close", namespace, tableName, subtaskId);
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();

    LOG.info("Iceberg writer {}.{} subtask {} begin dispose", namespace, tableName, subtaskId);
    abort();
    LOG.info("Iceberg writer {}.{} subtask {} completed dispose", namespace, tableName, subtaskId);
  }

  private void abort() {
    LOG.info("Iceberg writer {}.{} subtask {} has {} open files to abort",
        namespace, tableName, subtaskId, openPartitionFiles.size());
    // close all open files without sending DataFile list to downstream committer operator.
    // because there are not checkpointed,
    // we don't want to commit these files.
    for (Map.Entry<String, FileWriter> entry : openPartitionFiles.entrySet()) {
      final FileWriter writer = entry.getValue();
      final Path path = writer.getPath();
      try {
        LOG.debug("Iceberg writer {}.{} subtask {} start to abort file: {}",
            namespace, tableName, subtaskId, path);
        writer.abort();
        LOG.info("Iceberg writer {}.{} subtask {} completed aborting file: {}",
            namespace, tableName, subtaskId, path);
      } catch (Throwable t) {
//                LOG.error(String.format("Iceberg writer %s.%s subtask %d failed to abort open file: %s",
//                        namespace, tableName, subtaskId, path.toString()), t);
        LOG.error("Iceberg writer {}.{} subtask {} failed to abort open file: {}. Throwable = {}",
            namespace, tableName, subtaskId, path.toString(), t);
        continue;
      }

      try {
        LOG.debug("Iceberg writer {}.{} subtask {} deleting aborted file: {}",
            namespace, tableName, subtaskId, path);
        io.deleteFile(path.toString());
        LOG.info("Iceberg writer {}.{} subtask {} deleted aborted file: {}",
            namespace, tableName, subtaskId, path);
      } catch (Throwable t) {
//                LOG.error(String.format(
//                        "Iceberg writer %s.%s subtask %d failed to delete aborted file: %s",
//                        namespace, tableName, subtaskId, path.toString()), t);
        LOG.error("Iceberg writer {}.{} subtask {} failed to delete aborted file: {}. Throwable = {}",
            namespace, tableName, subtaskId, path.toString(), t);
      }
    }
    LOG.info("Iceberg writer {}.{} subtask {} aborted {} open files",
        namespace, tableName, subtaskId, openPartitionFiles.size());
    openPartitionFiles.clear();
  }

  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    IN value = element.getValue();
    try {
      processInternal(value);
    } catch (Exception t) {
      if (!skipIncompatibleRecord) {
        throw t;
      }
    }
  }

  @VisibleForTesting
  void processInternal(IN value) throws Exception {
    Record record = serializer.serialize(value, schema);
    processRecord(record);
  }

  /**
   * process element as {@link Record}
   */
  private void processRecord(Record record) throws Exception {
    if (partitioner == null) {
      partitioner = new RecordPartitioner(spec);
    }
    partitioner.partition(record);
    final String partitionPath = locations.newDataLocation(spec, partitioner, "");
    if (!openPartitionFiles.containsKey(partitionPath)) {
      final Path path = new Path(partitionPath, generateFileName());
      FileWriter writer = newWriter(path, partitioner);
      openPartitionFiles.put(partitionPath, writer);  // TODO: 1 writer for 1 partition path?
      LOG.info("Iceberg writer {}.{} subtask {} opened a new file: {}",
          namespace, tableName, subtaskId, path.toString());
    }
    final FileWriter writer = openPartitionFiles.get(partitionPath);
    final long fileSize = writer.write(record);
    // Rotate the file if over size limit.
    // This is mainly to avoid the 5 GB size limit of copying object in S3
    // that auto-lift otherwise can run into.
    // We still rely on presto-s3fs for progressive upload
    // that uploads a ~100 MB part whenever filled,
    // which achieves smoother outbound/upload network traffic.
    if (fileSize >= maxFileSize) {
      FlinkDataFile flinkDataFile = closeWriter(writer);
      emit(flinkDataFile);
      openPartitionFiles.remove(partitionPath);
    }
  }

  private String generateFileName() {
    return format.addExtension(
        String.format("%d_%d_%s", subtaskId, System.currentTimeMillis(), UUID.randomUUID().toString()));
  }

  private FileWriter newWriter(final Path path, final Partitioner<Record> part) throws Exception {
    FileAppender<Record> appender = newAppender(io.newOutputFile(path.toString()));
    FileWriter writer = FileWriter.builder()
        .withFileFormat(format)
        .withPath(path)
        .withProcessingTimeService(timerService)
        .withPartitioner(part.copy())
        .withAppender(appender)
        .withHadooopConfig(hadoopConf)
        .withSpec(spec)
        .withSchema(schema)
        .withTimestampField(timestampFeild)
        .withTimestampUnit(timestampUnit)
        .build();
    return writer;
  }


  private FileAppender<Record> newAppender(OutputFile file) throws Exception {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(properties);
    try {
      switch (format) {
        case PARQUET:
          return Parquet.write(file)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .setAll(properties)
              .metricsConfig(metricsConfig)
              .schema(schema)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write unknown format: " + format);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
}
