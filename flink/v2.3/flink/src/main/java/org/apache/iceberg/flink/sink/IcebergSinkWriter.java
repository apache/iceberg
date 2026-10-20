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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg writer implementation for the {@link SinkWriter} interface. Used by the {@link
 * org.apache.iceberg.flink.sink.IcebergSink} (SinkV2). Writes out the data to the final place, and
 * emits {@link SinkWriteResult}s at every checkpoint for every data/delete file created by this
 * writer.
 */
class IcebergSinkWriter implements CommittingSinkWriter<RowData, SinkWriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkWriter.class);

  /** Bounds the size of a single serialized {@link SinkWriteResult} of the DV-only write path. */
  static final int MAX_PAYLOAD_ENTRIES = 100_000;

  private static final WriteResult EMPTY_WRITE_RESULT = WriteResult.builder().build();

  private final String fullTableName;
  private final TaskWriterFactory<RowData> taskWriterFactory;
  private final IcebergStreamWriterMetrics metrics;
  private TaskWriter<RowData> writer;
  private final int subTaskId;
  private final int attemptId;

  IcebergSinkWriter(
      String fullTableName,
      TaskWriterFactory<RowData> taskWriterFactory,
      IcebergStreamWriterMetrics metrics,
      int subTaskId,
      int attemptId) {
    this.fullTableName = fullTableName;
    this.taskWriterFactory = taskWriterFactory;
    // Initialize the task writer factory.
    taskWriterFactory.initialize(subTaskId, attemptId);
    // Initialize the task writer.
    this.writer = taskWriterFactory.create();
    this.metrics = metrics;
    this.subTaskId = subTaskId;
    this.attemptId = attemptId;
    LOG.debug(
        "Created Stream Writer for table {} subtask {} attemptId {}",
        fullTableName,
        subTaskId,
        attemptId);
  }

  @Override
  public void write(RowData element, Context context) throws IOException, InterruptedException {
    writer.write(element);
  }

  @Override
  public void flush(boolean endOfInput) {
    // flush is used to handle flush/endOfInput, so no action is taken here.
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableName", fullTableName)
        .add("subTaskId", subTaskId)
        .add("attemptId", attemptId)
        .toString();
  }

  @Override
  public Collection<SinkWriteResult> prepareCommit() throws IOException {
    long startNano = System.nanoTime();
    TaskWriter<RowData> completing = writer;
    WriteResult result = completing.complete();
    this.writer = taskWriterFactory.create();
    metrics.updateFlushResult(result);
    metrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    LOG.debug(
        "Iceberg writer subtask {} attempt {} flushed {} data files and {} delete files",
        subTaskId,
        attemptId,
        result.dataFiles().length,
        result.deleteFiles().length);
    return toSinkWriteResults(completing, result, MAX_PAYLOAD_ENTRIES);
  }

  static List<SinkWriteResult> toSinkWriteResults(
      TaskWriter<RowData> completed, WriteResult result, int maxEntries) {
    if (!(completed instanceof DvOnlyDeltaWriter dvOnlyWriter)) {
      return Lists.newArrayList(new SinkWriteResult(result));
    }

    List<List<SerializedEqualityValues>> deleteKeys =
        split(Lists.newArrayList(dvOnlyWriter.deleteKeys()), maxEntries);
    List<List<PkIndexEntry>> liveRows = split(dvOnlyWriter.liveRows(), maxEntries);

    int count = Math.max(1, Math.max(deleteKeys.size(), liveRows.size()));
    List<SinkWriteResult> results = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      results.add(
          new SinkWriteResult(
              i == 0 ? result : EMPTY_WRITE_RESULT,
              i < deleteKeys.size() ? deleteKeys.get(i) : ImmutableList.of(),
              i < liveRows.size() ? liveRows.get(i) : ImmutableList.of()));
    }

    return results;
  }

  private static <T> List<List<T>> split(List<T> list, int maxEntries) {
    List<List<T>> chunks = Lists.newArrayList();
    for (List<T> chunk : Lists.partition(list, maxEntries)) {
      chunks.add(Lists.newArrayList(chunk));
    }

    return chunks;
  }
}
