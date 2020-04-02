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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT;

public class IcebergWriter<T> extends AbstractStreamOperator<FlinkDataFile>
    implements OneInputStreamOperator<T, FlinkDataFile> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);
  private static final String FILE_NAME_SEPARATOR = "_";

  private final AvroSerializer serializer;
  private final Configuration config;
  private final String namespace;
  private final String tableName;
  private final FileFormat format;
  private final boolean skipIncompatibleRecord;
  private final org.apache.iceberg.Schema icebergSchema;
  private final PartitionSpec spec;
  private final String s3BasePath;
  private final Map<String, String> tableProperties;
  private final String timestampFeild;
  private final TimeUnit timestampUnit;
  private final long maxFileSize;

  private transient String instanceId;
  private transient String titusTaskId;
  private transient Schema avroSchema;
  private transient org.apache.hadoop.conf.Configuration hadoopConfig;
  private transient Map<String, FileWriter> openPartitionFiles;
  private transient int subtaskId;
  private transient ProcessingTimeService timerService;
  private transient Partitioner partitioner;
  private transient FileSystem fs;

  public IcebergWriter(@Nullable AvroSerializer<T> serializer,
                       Configuration config) {
    this.serializer = serializer;
    this.config = config;
    format = FileFormat.valueOf(config.getString(IcebergConnectorConstant.FORMAT,
        FileFormat.PARQUET.name()));
    skipIncompatibleRecord = config.getBoolean(IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD,
        IcebergConnectorConstant.DEFAULT_SKIP_INCOMPATIBLE_RECORD);
    // TODO: different from IcebergCommitter, line 147, in which, "" is taken as default
    timestampFeild = config.getString(IcebergConnectorConstant.VTTS_WATERMARK_TIMESTAMP_FIELD,
        IcebergConnectorConstant.DEFAULT_VTTS_WATERMARK_TIMESTAMP_UNIT);
    timestampUnit = TimeUnit.valueOf(config.getString(IcebergConnectorConstant.VTTS_WATERMARK_TIMESTAMP_UNIT,
        IcebergConnectorConstant.DEFAULT_VTTS_WATERMARK_TIMESTAMP_UNIT));
    maxFileSize = config.getLong(IcebergConnectorConstant.MAX_FILE_SIZE,
        IcebergConnectorConstant.DEFAULT_MAX_FILE_SIZE);

    // TODO: duplicate logic, to extract
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    Catalog catalog = null;
    String catalogType = config.getString(IcebergConnectorConstant.CATALOG_TYPE,
                                          IcebergConnectorConstant.CATALOG_TYPE_DEFAULT);
    switch (catalogType.toUpperCase()) {
      case IcebergConnectorConstant.HIVE_CATALOG:
        hadoopConf.set(ConfVars.METASTOREURIS.varname, config.getString(ConfVars.METASTOREURIS.varname, ""));
        catalog = HiveCatalogs.loadCatalog(hadoopConf);
        break;

      case IcebergConnectorConstant.HADOOP_CATALOG:
        catalog = new HadoopCatalog(hadoopConf,
                                    config.getString(IcebergConnectorConstant.HADOOP_CATALOG_WAREHOUSE_LOCATION, ""));
        break;

      default:
        throw new UnsupportedOperationException("Unknown catalog type or not set: " + catalogType);
    }

    namespace = config.getString(IcebergConnectorConstant.NAMESPACE, "");
    tableName = config.getString(IcebergConnectorConstant.TABLE, "");
    Table table = catalog.loadTable(TableIdentifier.parse(namespace + "." + tableName));

    ImmutableMap.Builder<String, String> tablePropsBuilder = ImmutableMap.<String, String>builder()
        .putAll(table.properties());
    if (!table.properties().containsKey(PARQUET_COMPRESSION)) {
      // if compression is not set in table properties,
      // Flink writer defaults it to BROTLI
      //TODO: org.apache.hadoop.io.compress.BrotliCodec, class not found
      //tablePropsBuilder.put(PARQUET_COMPRESSION, CompressionCodecName.BROTLI.name());
      tablePropsBuilder.put(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT);
    }
    tableProperties = tablePropsBuilder.build();
    icebergSchema = table.schema();
    spec = table.spec();
    //s3BasePath = getS3BasePath(table.location());
    s3BasePath = table.locationProvider().newDataLocation("");  // data location of the Iceberg table
    LOG.info("Iceberg writer {}.{} has S3 base path: {}", namespace, tableName, s3BasePath);
    LOG.info("Iceberg writer {}.{} created with sink config", namespace, tableName);
    LOG.info("Iceberg writer {}.{} loaded table: schema = {}\npartition spec = {}",
        namespace, tableName, icebergSchema, spec);

    // default ChainingStrategy is set to HEAD
    // we prefer chaining to avoid the huge serialization and deserializatoin overhead.
    super.setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  /**
   * @param location location from table metadata
   *                 e.g. s3n://bucket/hive/warehouse/database_name.db/table_name
   */
  private String getS3BasePath(final String location) {
    String region = System.getenv("EC2_REGION");
    if (Strings.isNullOrEmpty(region)) {
      region = "us-east-1";
      LOG.info("default to us-east-1 region");
    }
    String regionLocation = location;
    if (!region.equals("us-east-1")) {
      final String regionPrefix = region + ".";
      regionLocation = regionLocation.replaceAll("s3n://", "s3n://" + regionPrefix);
    }
    // convention is put under /data child dir
    return regionLocation + "/data/";
  }

  @Override
  public void open() throws Exception {
    super.open();
    init(getRuntimeContext().getIndexOfThisSubtask(),
        getProcessingTimeService());
  }

  void init(int subtaskId1, ProcessingTimeService timerService1) throws IOException {
    instanceId = System.getenv("EC2_INSTANCE_ID");
    titusTaskId = System.getenv("TITUS_TASK_ID");
    avroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);

    hadoopConfig = new org.apache.hadoop.conf.Configuration();

    this.subtaskId = subtaskId1;
    this.timerService = timerService1;
    openPartitionFiles = new HashMap<>();

    // Create a file system with the user-defined {@code HDFS} configuration.
    // supply a dummy config for nowm until we find a need to customize
    //final Configuration fsConfig = new Configuration();
    if (fs == null) {
      Path path = new Path(s3BasePath);
      // TODO: change to use StreamingFileSink and uncomment fs.delete() in abort()
      //fs = BucketingSink.createHadoopFileSystem(path, fsConfig);
      LOG.info("Iceberg writer {}.{} subtask {} created file system with base path: {}",
          namespace, tableName, subtaskId1, path.toString());
    }
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

  /**
   * This method is called after all records have been added to the operators via the methods
   * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
   * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
   * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.
   *
   * <p>The method is expected to flush all remaining buffered data. Exceptions during this
   * flushing of buffered should be propagated, in order to cause the operation to be recognized
   * as failed, because the last data items are not processed properly.
   *
   * @throws java.lang.Exception An exception in this method causes the operator to fail.
   */
  @Override
  public void close() throws Exception {
    super.close();

    LOG.info("Iceberg writer {}.{} subtask {} begin close", namespace, tableName, subtaskId);
    // close all open files without emitting to downstream committer
    flush(false);
    LOG.info("Iceberg writer {}.{} subtask {} completed close", namespace, tableName, subtaskId);
  }

  /**
   * This method is called at the very end of the operator's life, both in the case of a successful
   * completion of the operation, and in the case of a failure and canceling.
   *
   * <p>This method is expected to make a thorough effort to release all resources
   * that the operator has acquired.
   */
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

      // it is ok if deletion failed,
      // as S3 retention should eventually kick in.
      try {
        LOG.debug("Iceberg writer {}.{} subtask {} deleting aborted file: {}",
            namespace, tableName, subtaskId, path);
        //fs.delete(path, false);  // TODO
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
  public void processElement(StreamRecord<T> element) throws Exception {
    T value = element.getValue();
    try {
      processInternal(value);
    } catch (Exception t) {
      if (!skipIncompatibleRecord) {
        throw t;
      }
    }
  }

  @VisibleForTesting
  void processInternal(T value) throws Exception {
    try {
      AvroSerializer<T> ser = getSerializer(value);
      IndexedRecord avroRecord = ser.serialize(value, avroSchema);
      processIndexedRecord(avroRecord);
    } catch (AvroTypeException e) {
      throw e;
    }
  }

  /**
   * This is mainly for backward compatibility for the short term.
   * TODO: We should ask user to explicitly set the serializer in the IcebergSinkAppender and simplify the code here.
   */
  private AvroSerializer getSerializer(T value) {
    if (null != this.serializer) {
      return this.serializer;
    }

    // backward compatible part
    if (value instanceof IndexedRecord) {
      return PassThroughAvroSerializer.getInstance();
    } else if (value instanceof Map) {
      return MapAvroSerializer.getInstance();
    } else {
      return PojoAvroSerializer.getInstance();
    }
  }

  /**
   * process element as avro IndexedRecord
   */
  private void processIndexedRecord(IndexedRecord record) throws Exception {
    if (null == partitioner) {
      partitioner = new IndexedRecordPartitioner(spec);
    }
    partitioner.partition(record);
    final String partitionPath = s3BasePath + partitioner.toPath();
    if (!openPartitionFiles.containsKey(partitionPath)) {
      final Path path = new Path(partitionPath, generateFileName());
      FileWriter writer = newWriter(path, partitioner);
      openPartitionFiles.put(partitionPath, writer);
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
    final String filename = new StringBuilder()
        .append(subtaskId)
        .append(FILE_NAME_SEPARATOR)
        .append(instanceId)
        .append(FILE_NAME_SEPARATOR)
        .append(titusTaskId)
        .append(FILE_NAME_SEPARATOR)
        .append(System.currentTimeMillis())
        .append(FILE_NAME_SEPARATOR)
        .append(UUID.randomUUID().toString())
        .toString();
    return format.addExtension(filename);
  }

  private FileWriter newWriter(final Path path, final Partitioner part) throws Exception {
    FileAppender<IndexedRecord> appender = newAppender(path);
    FileWriter writer = FileWriter.builder()
        .withFileFormat(format)
        .withPath(path)
        .withProcessingTimeService(timerService)
        .withPartitioner(part.copy())
        .withAppender(appender)
        .withHadooopConfig(hadoopConfig)
        .withSpec(spec)
        .withSchema(avroSchema)
        .withVttsTimestampField(timestampFeild)
        .withVttsTimestampUnit(timestampUnit)
        .build();
    return writer;
  }

  private <D> FileAppender<D> newAppender(final Path path) throws Exception {
    OutputFile outputFile = HadoopOutputFile.fromPath(path, hadoopConfig);
    if (FileFormat.PARQUET == format) {
      return Parquet.write(outputFile)
          .named(tableName)
          .schema(icebergSchema)
          .setAll(tableProperties)
          .metricsConfig(MetricsConfig.fromProperties(tableProperties))
          .overwrite()
          .build();
    } else {
      throw new UnsupportedOperationException("not supported file format: " + format.name());
    }
  }
}
