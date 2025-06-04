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
package org.apache.iceberg.flink.sink.dynamic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg writer implementation for the {@link SinkWriter} interface. Used by the
 * DynamicIcebergSink. Writes out the data to the final place, and emits {@link DynamicWriteResult}
 * for every unique {@link WriteTarget} at checkpoint time.
 */
class DynamicWriter implements CommittingSinkWriter<DynamicRecordInternal, DynamicWriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicWriter.class);

  private final Cache<WriteTarget, RowDataTaskWriterFactory> taskWriterFactories;
  private final Map<WriteTarget, TaskWriter<RowData>> writers;
  private final DynamicWriterMetrics metrics;
  private final int subTaskId;
  private final int attemptId;
  private final Catalog catalog;
  private final FileFormat dataFileFormat;
  private final long targetDataFileSize;
  private final Map<String, String> commonWriteProperties;

  DynamicWriter(
      Catalog catalog,
      FileFormat dataFileFormat,
      long targetDataFileSize,
      Map<String, String> commonWriteProperties,
      int cacheMaximumSize,
      DynamicWriterMetrics metrics,
      int subTaskId,
      int attemptId) {
    this.catalog = catalog;
    this.dataFileFormat = dataFileFormat;
    this.targetDataFileSize = targetDataFileSize;
    this.commonWriteProperties = commonWriteProperties;
    this.metrics = metrics;
    this.subTaskId = subTaskId;
    this.attemptId = attemptId;
    this.taskWriterFactories = Caffeine.newBuilder().maximumSize(cacheMaximumSize).build();
    this.writers = Maps.newHashMap();

    LOG.debug("DynamicIcebergSinkWriter created for subtask {} attemptId {}", subTaskId, attemptId);
  }

  @Override
  public void write(DynamicRecordInternal element, Context context)
      throws IOException, InterruptedException {
    writers
        .computeIfAbsent(
            new WriteTarget(
                element.tableName(),
                element.branch(),
                element.schema().schemaId(),
                element.spec().specId(),
                element.upsertMode(),
                element.equalityFields()),
            writerKey -> {
              RowDataTaskWriterFactory taskWriterFactory =
                  taskWriterFactories.get(
                      writerKey,
                      factoryKey -> {
                        Table table =
                            catalog.loadTable(TableIdentifier.parse(factoryKey.tableName()));

                        // TODO: Handle precedence correctly for the write properties coming from
                        // the sink conf and from the table defaults
                        Map<String, String> tableWriteProperties =
                            Maps.newHashMap(commonWriteProperties);
                        tableWriteProperties.putAll(table.properties());

                        List<Integer> equalityFieldIds =
                            getEqualityFields(table, element.equalityFields());
                        if (element.upsertMode()) {
                          Preconditions.checkState(
                              !equalityFieldIds.isEmpty(),
                              "Equality field columns shouldn't be empty when configuring to use UPSERT data.");
                          if (!table.spec().isUnpartitioned()) {
                            for (PartitionField partitionField : table.spec().fields()) {
                              Preconditions.checkState(
                                  equalityFieldIds.contains(partitionField.sourceId()),
                                  "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
                                  partitionField,
                                  equalityFieldIds);
                            }
                          }
                        }

                        LOG.debug("Creating new writer factory for table '{}'", table.name());
                        return new RowDataTaskWriterFactory(
                            () -> table,
                            FlinkSchemaUtil.convert(element.schema()),
                            targetDataFileSize,
                            dataFileFormat,
                            tableWriteProperties,
                            equalityFieldIds,
                            element.upsertMode(),
                            element.schema(),
                            element.spec());
                      });

              taskWriterFactory.initialize(subTaskId, attemptId);
              return taskWriterFactory.create();
            })
        .write(element.rowData());
  }

  @Override
  public void flush(boolean endOfInput) {
    // flush is used to handle flush/endOfInput, so no action is taken here.
  }

  @Override
  public void close() throws Exception {
    for (TaskWriter<RowData> writer : writers.values()) {
      writer.close();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("subtaskId", subTaskId)
        .add("attemptId", attemptId)
        .add("dataFileFormat", dataFileFormat)
        .add("targetDataFileSize", targetDataFileSize)
        .add("writeProperties", commonWriteProperties)
        .toString();
  }

  @Override
  public Collection<DynamicWriteResult> prepareCommit() throws IOException {
    List<DynamicWriteResult> result = Lists.newArrayList();
    for (Map.Entry<WriteTarget, TaskWriter<RowData>> entry : writers.entrySet()) {
      long startNano = System.nanoTime();
      WriteResult writeResult = entry.getValue().complete();
      WriteTarget writeTarget = entry.getKey();
      metrics.updateFlushResult(writeTarget.tableName(), writeResult);
      metrics.flushDuration(
          writeTarget.tableName(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
      LOG.debug(
          "Iceberg writer for table {} subtask {} attempt {} flushed {} data files and {} delete files",
          writeTarget.tableName(),
          subTaskId,
          attemptId,
          writeResult.dataFiles().length,
          writeResult.deleteFiles().length);

      result.add(new DynamicWriteResult(writeTarget, writeResult));
    }

    writers.clear();

    return result;
  }

  private static List<Integer> getEqualityFields(Table table, List<Integer> equalityFieldIds) {
    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      return equalityFieldIds;
    }
    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
    if (identifierFieldIds != null && !identifierFieldIds.isEmpty()) {
      return Lists.newArrayList(identifierFieldIds);
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  DynamicWriterMetrics getMetrics() {
    return metrics;
  }
}
