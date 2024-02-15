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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.*;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SerializableSupplier;

public class IcebergMultiTableStreamWriter<T> extends AbstractStreamOperator<TableAwareWriteResult>
    implements OneInputStreamOperator<T, TableAwareWriteResult>, BoundedOneInput {
  private static final long serialVersionUID = 1L;

  private final String fullTableName;
  private final Map<String, TaskWriterFactory<T>> taskWriterFactories;
  private final PayloadSinkProvider<T> payloadSinkProvider;
  private final CatalogLoader catalogLoader;
  private final FlinkWriteConf conf;
  private List<String> equalityFieldColumns;

  private transient Map<String, TaskWriter<T>> writers;
  private transient Map<String, SerializableTable> tableSupplierMap;
  private transient int subTaskId;
  private transient int attemptId;
  private transient Map<String, IcebergStreamWriterMetrics> writerMetrics;

  IcebergMultiTableStreamWriter(
      String fullTableName,
      PayloadSinkProvider<T> payloadSinkProvider,
      CatalogLoader catalogLoader,
      FlinkWriteConf writeConf,
      List<String> equalityFieldColumns) {
    this.fullTableName = fullTableName;
    this.taskWriterFactories = new HashMap<>();
    this.writers = new HashMap<>();
    this.writerMetrics = new HashMap<>();
    this.payloadSinkProvider = payloadSinkProvider;
    this.catalogLoader = catalogLoader;
    this.conf = writeConf;
    this.equalityFieldColumns = equalityFieldColumns;
    this.tableSupplierMap = new HashMap<>();
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    //        this.writerMetrics = new IcebergStreamWriterMetrics(super.metrics, fullTableName);

    // Initialize the task writer factory.
    //        this.taskWriterFactory.initialize(subTaskId, attemptId);

    // Initialize the task writer.
    //        this.writer = taskWriterFactory.create();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    //        this.writer = taskWriterFactory.create();
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {

    String identifier = payloadSinkProvider.getIdentifierFromPayload(element);
    String table = payloadSinkProvider.sinkTableName(fullTableName, identifier);
    if (!writers.containsKey(table)) {
      if (!taskWriterFactories.containsKey(table)) {
        TableIdentifier tableIdentifier =
            TableIdentifier.of(payloadSinkProvider.sinkDatabaseName(), table);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        Table sinkTable = tableLoader.loadTable();
        tableSupplierMap.put(table, (SerializableTable) SerializableTable.copyOf(sinkTable));
        TaskWriterFactory taskWriterFactory =
            new RowDataTaskWriterFactory(
                sinkTable,
                FlinkSink.toFlinkRowType(sinkTable.schema(), null),
                conf.targetDataFileSize(),
                conf.dataFileFormat(),
                writeProperties(sinkTable, conf.dataFileFormat(), conf),
                checkAndGetEqualityFieldIds(sinkTable),
                conf.upsertMode());
        taskWriterFactory.initialize(subTaskId, attemptId);
        taskWriterFactories.put(table, taskWriterFactory);
      }
      TaskWriter<T> taskWriter = taskWriterFactories.get(table).create();
      writers.put(table, taskWriter);
      IcebergStreamWriterMetrics tableWriteMetrics =
          new IcebergStreamWriterMetrics(super.metrics, table);
      writerMetrics.put(table, tableWriteMetrics);
    }
    writers.get(table).write(element.getValue());
  }

  @Override
  public void close() throws Exception {
    super.close();
    Iterator<Map.Entry<String, TaskWriter<T>>> writeIterator = writers.entrySet().iterator();
    while (writeIterator.hasNext()) {
      Map.Entry<String, TaskWriter<T>> writer = writeIterator.next();
      writer.getValue().close();
    }
    writers.clear();
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.
    flush();
  }

  @Override
  public String toString() {
    // TODO: Modify if required
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  /** close all open files and emit files to downstream committer operator */
  private void flush() throws IOException {
    if (writers.isEmpty()) {
      return;
    }
    Iterator<Map.Entry<String, TaskWriter<T>>> iterator = writers.entrySet().iterator();
    while (iterator.hasNext()) {
      long startNano = System.nanoTime();
      Map.Entry<String, TaskWriter<T>> writerMap = iterator.next();
      String table = writerMap.getKey();
      TaskWriter<T> writer = writerMap.getValue();
      // TODO: Add table information in write result
      WriteResult result = writer.complete();
      TableAwareWriteResult tableAwareWriteResult = new TableAwareWriteResult(result, tableSupplierMap.get(table));
      IcebergStreamWriterMetrics metrics = writerMetrics.get(table);
      output.collect(new StreamRecord<>(tableAwareWriteResult));
      metrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    }

    // Set writer to null to prevent duplicate flushes in the corner case of
    // prepareSnapshotPreBarrier happening after endInput.
    writers.clear();
  }

  private static Map<String, String> writeProperties(
      Table table, FileFormat format, FlinkWriteConf conf) {
    Map<String, String> writeProperties = Maps.newHashMap(table.properties());

    switch (format) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
        String parquetCompressionLevel = conf.parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }

        break;
      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
        String avroCompressionLevel = conf.avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
        }

        break;
      case ORC:
        writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown file format %s", format));
    }

    return writeProperties;
  }

  private List<Integer> checkAndGetEqualityFieldIds(Table table) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());
    if (equalityFieldColumns != null && !equalityFieldColumns.isEmpty()) {
      Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
      for (String column : equalityFieldColumns) {
        org.apache.iceberg.types.Types.NestedField field = table.schema().findField(column);
        Preconditions.checkNotNull(
            field,
            "Missing required equality field column '%s' in table schema %s",
            column,
            table.schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
        LOG.warn(
            "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                + " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet,
            table.schema().identifierFieldIds());
      }
      equalityFieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return equalityFieldIds;
  }
}
