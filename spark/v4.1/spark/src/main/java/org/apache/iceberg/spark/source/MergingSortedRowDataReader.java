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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PartitionReader} that reads multiple sorted files and merges them into a single sorted
 * stream.
 *
 * <p>This reader is used when {@code preserve-data-ordering} is enabled and the task group contains
 * multiple files that all have the same sort order. It creates one {@link RowDataReader} per file
 * and uses {@link MergingPartitionReader} to perform a k-way merge.
 */
class MergingSortedRowDataReader implements PartitionReader<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSortedRowDataReader.class);

  private final MergingPartitionReader<InternalRow> mergingReader;
  private final List<RowDataReader> fileReaders;

  MergingSortedRowDataReader(SparkInputPartition partition, int reportableSortOrderId) {
    Table table = partition.table();
    ScanTaskGroup<FileScanTask> taskGroup = partition.taskGroup();
    Schema tableSchema = SnapshotUtil.schemaFor(table, partition.branch());
    Schema expectedSchema = partition.expectedSchema();

    Preconditions.checkArgument(
        reportableSortOrderId > 0, "Invalid sort order ID: %s", reportableSortOrderId);
    Preconditions.checkArgument(
        taskGroup.tasks().size() > 1,
        "Merging reader requires multiple files, got %s",
        taskGroup.tasks().size());

    LOG.info(
        "Creating merging reader for {} files with sort order ID {} in table {}",
        taskGroup.tasks().size(),
        reportableSortOrderId,
        table.name());

    SortOrder sortOrder = table.sortOrders().get(reportableSortOrderId);
    Preconditions.checkNotNull(
        sortOrder,
        "Cannot find sort order with ID %s in table %s",
        reportableSortOrderId,
        table.name());

    this.fileReaders =
        taskGroup.tasks().stream()
            .map(
                task -> {
                  ScanTaskGroup<FileScanTask> singleTaskGroup =
                      new BaseScanTaskGroup<>(java.util.Collections.singletonList(task));

                  return new RowDataReader(
                      table,
                      singleTaskGroup,
                      tableSchema,
                      expectedSchema,
                      partition.isCaseSensitive(),
                      partition.cacheDeleteFilesOnExecutors());
                })
            .collect(Collectors.toList());

    List<PartitionReader<InternalRow>> readers =
        fileReaders.stream()
            .map(reader -> (PartitionReader<InternalRow>) reader)
            .collect(Collectors.toList());

    StructType sparkSchema = SparkSchemaUtil.convert(expectedSchema);
    this.mergingReader =
        new MergingPartitionReader<>(readers, sortOrder, sparkSchema, expectedSchema);
  }

  @Override
  public boolean next() throws IOException {
    return mergingReader.next();
  }

  @Override
  public InternalRow get() {
    return mergingReader.get();
  }

  @Override
  public void close() throws IOException {
    mergingReader.close();
  }

  public CustomTaskMetric[] currentMetricsValues() {
    long totalSplits = fileReaders.size();

    long totalDeletes =
        fileReaders.stream()
            .flatMap(reader -> Arrays.stream(reader.currentMetricsValues()))
            .filter(
                metric -> metric instanceof org.apache.iceberg.spark.source.metrics.TaskNumDeletes)
            .mapToLong(CustomTaskMetric::value)
            .sum();

    return new CustomTaskMetric[] {
      new org.apache.iceberg.spark.source.metrics.TaskNumSplits(totalSplits),
      new org.apache.iceberg.spark.source.metrics.TaskNumDeletes(totalDeletes)
    };
  }
}
