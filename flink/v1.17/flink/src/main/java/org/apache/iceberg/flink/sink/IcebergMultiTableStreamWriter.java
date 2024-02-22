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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class IcebergMultiTableStreamWriter<T> extends AbstractStreamOperator<TableAwareWriteResult>
    implements OneInputStreamOperator<T, TableAwareWriteResult>, BoundedOneInput {
  private static final long serialVersionUID = 1L;

  private final Map<TableIdentifier, TaskWriterFactory<T>> taskWriterFactories;
  private final PayloadTableSinkProvider<T> payloadTableSinkProvider;
  private final CatalogLoader catalogLoader;
  private final FlinkWriteConf conf;
  private List<String> equalityFieldColumns;

  private transient Map<TableIdentifier, TaskWriter<T>> writers;
  private transient int subTaskId;
  private transient int attemptId;
  private transient Map<TableIdentifier, IcebergStreamWriterMetrics> writerMetrics;

  IcebergMultiTableStreamWriter(
      PayloadTableSinkProvider<T> payloadTableSinkProvider,
      CatalogLoader catalogLoader,
      FlinkWriteConf writeConf,
      List<String> equalityFieldColumns) {
    this.taskWriterFactories = Maps.newHashMap();
    this.writers = Maps.newHashMap();
    this.writerMetrics = Maps.newHashMap();
    this.payloadTableSinkProvider = payloadTableSinkProvider;
    this.catalogLoader = catalogLoader;
    this.conf = writeConf;
    this.equalityFieldColumns = equalityFieldColumns;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {

    TableIdentifier tableIdentifier = payloadTableSinkProvider.getOrCreateTable(element);
    initializeTableIfRequired(tableIdentifier);
    writers.get(tableIdentifier).write(element.getValue());
  }

  @Override
  public void close() throws Exception {
    super.close();
    Iterator<Map.Entry<TableIdentifier, TaskWriter<T>>> writeIterator = writers.entrySet().iterator();
    while (writeIterator.hasNext()) {
      Map.Entry<TableIdentifier, TaskWriter<T>> writer = writeIterator.next();
      try {
        writer.getValue().close();
      }
      catch (Exception exception) {
        LOG.error("Error occurred while closing the writer {}: ", writer.getValue(), exception);
      }
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
    String tableList = writers.keySet().size() > 0 ?
            String.join(";", writers.keySet().stream().map(TableIdentifier::name).collect(Collectors.toList())) : "empty_writer";
    return MoreObjects.toStringHelper(this)
        .add("tables", tableList)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  private void initializeTableIfRequired(TableIdentifier tableIdentifier) {
    if (!writers.containsKey(tableIdentifier)) {
      String tableName = tableIdentifier.name();
      LOG.info("Writer not found for table: {}", tableName);
      if (!taskWriterFactories.containsKey(tableIdentifier)) {
        LOG.info("Task Writer Factory not found for table: {}", tableName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        Table sinkTable = tableLoader.loadTable();
        TaskWriterFactory taskWriterFactory =
                new RowDataTaskWriterFactory(
                        sinkTable,
                        FlinkSink.toFlinkRowType(sinkTable.schema(), null),
                        conf.targetDataFileSize(),
                        conf.dataFileFormat(),
                        TablePropertyUtil.writeProperties(sinkTable, conf.dataFileFormat(), conf),
                        TablePropertyUtil.checkAndGetEqualityFieldIds(sinkTable, equalityFieldColumns),
                        conf.upsertMode());
        taskWriterFactory.initialize(subTaskId, attemptId);
        taskWriterFactories.put(tableIdentifier, taskWriterFactory);
      }
      TaskWriter<T> taskWriter = taskWriterFactories.get(tableIdentifier).create();
      writers.put(tableIdentifier, taskWriter);
      IcebergStreamWriterMetrics tableWriteMetrics =
              new IcebergStreamWriterMetrics(super.metrics, tableName);
      writerMetrics.put(tableIdentifier, tableWriteMetrics);
    }
  }

  /** close all open files and emit files to downstream committer operator */
  private void flush() throws IOException {
    if (writers.isEmpty()) {
      return;
    }
    Iterator<Map.Entry<TableIdentifier, TaskWriter<T>>> iterator = writers.entrySet().iterator();
    while (iterator.hasNext()) {
      long startNano = System.nanoTime();
      Map.Entry<TableIdentifier, TaskWriter<T>> writerMap = iterator.next();
      TableIdentifier tableIdentifier = writerMap.getKey();
      TaskWriter<T> writer = writerMap.getValue();
      WriteResult result = writer.complete();
      TableAwareWriteResult tableAwareWriteResult =
          new TableAwareWriteResult(result, tableIdentifier);
      IcebergStreamWriterMetrics metrics = writerMetrics.get(tableIdentifier);
      output.collect(new StreamRecord<>(tableAwareWriteResult));
      metrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    }

    // Set writer to null to prevent duplicate flushes in the corner case of
    // prepareSnapshotPreBarrier happening after endInput.
    writers.clear();
  }
  
}
