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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SortedMerge;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PartitionReader} that reads multiple sorted files and merges them into a single sorted
 * stream using a k-way heap merge ({@link SortedMerge}).
 *
 * <p>This reader is used when {@code preserve-data-ordering} is enabled and the task group contains
 * multiple files that all have the same sort order.
 *
 * <p>Sort key columns absent from the requested projection are temporarily added to the read schema
 * so that {@link SortOrderComparators} can access them during the merge. The extra columns are
 * stripped from each row before it is returned to Spark.
 */
class MergingSortedRowDataReader implements PartitionReader<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSortedRowDataReader.class);

  private final CloseableGroup resources;
  private final CloseableIterator<InternalRow> mergedIterator;
  private final List<RowDataReader> fileReaders;
  // non-null only when sort key columns were added to the read schema beyond what Spark projected
  private final int[] outputPositions;
  private final DataType[] outputDataTypes;
  private final Object[] outputValues; // reused per row to avoid per-row allocation
  private InternalRow current;

  MergingSortedRowDataReader(SparkInputPartition partition) {
    Table table = partition.table();
    ScanTaskGroup<FileScanTask> taskGroup = partition.taskGroup();
    Schema projection = partition.projection();
    SortOrder sortOrder = table.sortOrder();

    int numFiles = taskGroup.tasks().size();

    Preconditions.checkState(
        sortOrder.isSorted(), "Cannot create merging reader for unsorted table %s", table.name());
    Preconditions.checkState(
        numFiles > 1, "Merging reader requires multiple files, got %s", numFiles);

    LOG.info(
        "Creating merging reader for {} files with sort order {} in table {}",
        numFiles,
        sortOrder.orderId(),
        table.name());

    // Augment the projected schema with any sort key columns Spark did not request so that
    // SortOrderComparators can access every sort key field during the merge.
    Schema mergeReadSchema = mergeReadSchema(projection, sortOrder, table);
    this.outputPositions = buildOutputPositions(projection, mergeReadSchema);
    this.outputDataTypes = buildOutputDataTypes(projection, outputPositions);
    this.outputValues = outputPositions != null ? new Object[outputPositions.length] : null;

    this.resources = new CloseableGroup();
    this.fileReaders =
        taskGroup.tasks().stream()
            .map(
                task ->
                    new RowDataReader(
                        table,
                        partition.io(),
                        new BaseScanTaskGroup<>(Collections.singletonList(task)),
                        mergeReadSchema,
                        partition.isCaseSensitive(),
                        partition.cacheDeleteFilesOnExecutors()))
            .toList();
    // Wrap each reader as a CloseableIterable and feed into SortedMerge.
    List<CloseableIterable<InternalRow>> fileIterables =
        fileReaders.stream().map(this::readerToIterable).toList();
    SortedMerge<InternalRow> sortedMerge =
        new SortedMerge<>(buildComparator(mergeReadSchema, sortOrder), fileIterables);
    resources.addCloseable(sortedMerge);
    this.mergedIterator = sortedMerge.iterator();
  }

  /**
   * Adapts a {@link RowDataReader} to a {@link CloseableIterable} for use with {@link SortedMerge}.
   * Each row is copied before it enters the priority queue because Spark's Parquet/ORC readers
   * reuse {@link InternalRow} instances for performance.
   */
  private CloseableIterable<InternalRow> readerToIterable(RowDataReader reader) {
    return CloseableIterable.withNoopClose(
        () ->
            new CloseableIterator<>() {
              private boolean advanced = false;
              private boolean hasNext = false;

              @Override
              public boolean hasNext() {
                if (!advanced) {
                  try {
                    hasNext = reader.next();
                    advanced = true;
                  } catch (IOException e) {
                    throw new UncheckedIOException("Failed to advance reader", e);
                  }
                }
                return hasNext;
              }

              @Override
              public InternalRow next() {
                if (!advanced) {
                  hasNext();
                }
                advanced = false;
                return reader.get().copy();
              }

              @Override
              public void close() throws IOException {
                reader.close();
              }
            });
  }

  @Override
  public boolean next() throws IOException {
    if (!mergedIterator.hasNext()) {
      return false;
    }

    InternalRow merged = mergedIterator.next();
    if (outputPositions == null) {
      this.current = merged;
    } else {
      // Strip the extra sort key columns that were added for comparison purposes.
      for (int i = 0; i < outputPositions.length; i++) {
        outputValues[i] = merged.get(outputPositions[i], outputDataTypes[i]);
      }
      this.current = new GenericInternalRow(outputValues);
    }

    return true;
  }

  @Override
  public InternalRow get() {
    return current;
  }

  @Override
  public void close() throws IOException {
    resources.close();
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    long totalDeletes =
        fileReaders.stream()
            .flatMap(reader -> Arrays.stream(reader.currentMetricsValues()))
            .filter(metric -> metric instanceof TaskNumDeletes)
            .mapToLong(CustomTaskMetric::value)
            .sum();
    return new CustomTaskMetric[] {
      new TaskNumSplits(fileReaders.size()), new TaskNumDeletes(totalDeletes)
    };
  }

  /**
   * Builds a comparator for merging {@link InternalRow}s by the given sort order. Uses {@link
   * SortOrderComparators} which handles all transform types (identity, bucket, truncate), ASC/DESC
   * directions, and null ordering. The two {@link InternalRowWrapper} instances are allocated once
   * and reused — {@code wrap()} just updates an internal reference.
   */
  private static Comparator<InternalRow> buildComparator(
      Schema mergeReadSchema, SortOrder sortOrder) {
    StructType sparkSchema = SparkSchemaUtil.convert(mergeReadSchema);
    Comparator<StructLike> keyComparator =
        SortOrderComparators.forSchema(mergeReadSchema, sortOrder);
    InternalRowWrapper left = new InternalRowWrapper(sparkSchema, mergeReadSchema.asStruct());
    InternalRowWrapper right = new InternalRowWrapper(sparkSchema, mergeReadSchema.asStruct());
    return (r1, r2) -> keyComparator.compare(left.wrap(r1), right.wrap(r2));
  }

  /**
   * Returns the Spark {@link DataType}s for each column in {@code projection}, or {@code null} when
   * {@code outputPositions} is {@code null} (no extra columns were added, no projection needed).
   */
  private static DataType[] buildOutputDataTypes(Schema projection, int[] outputPositions) {
    if (outputPositions == null) {
      return null;
    }
    StructType sparkSchema = SparkSchemaUtil.convert(projection);
    DataType[] dataTypes = new DataType[sparkSchema.fields().length];
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      dataTypes[i] = sparkSchema.fields()[i].dataType();
    }
    return dataTypes;
  }

  /**
   * Returns the schema to use when reading each file. This is the requested {@code projection}
   * augmented with any sort key columns that are not already present, so the merge comparator can
   * access every sort key field regardless of what Spark projected.
   */
  private static Schema mergeReadSchema(Schema projection, SortOrder sortOrder, Table table) {
    Schema tableSchema = table.schema();
    List<Types.NestedField> missingFields = Lists.newArrayList();

    for (SortField sortField : sortOrder.fields()) {
      int fieldId = sortField.sourceId();
      if (projection.findField(fieldId) == null) {
        Types.NestedField tableField = tableSchema.findField(fieldId);
        if (tableField != null) {
          missingFields.add(tableField);
        }
      }
    }

    if (missingFields.isEmpty()) {
      return projection;
    }

    return TypeUtil.join(projection, new Schema(missingFields));
  }

  /**
   * Returns an array mapping each output column (in {@code projection} order) to its position in
   * {@code mergeSchema}, or {@code null} if the two schemas are identical (no extra columns were
   * added and no projection is needed).
   */
  private static int[] buildOutputPositions(Schema projection, Schema mergeSchema) {
    if (projection.columns().size() == mergeSchema.columns().size()) {
      return null;
    }

    List<Types.NestedField> mergeColumns = mergeSchema.columns();
    int[] positions = new int[projection.columns().size()];

    for (int i = 0; i < projection.columns().size(); i++) {
      int fieldId = projection.columns().get(i).fieldId();
      boolean found = false;
      for (int j = 0; j < mergeColumns.size(); j++) {
        if (mergeColumns.get(j).fieldId() == fieldId) {
          positions[i] = j;
          found = true;
          break;
        }
      }
      Preconditions.checkState(
          found, "Projection field id=%d not found in merge read schema — this is a bug", fieldId);
    }

    return positions;
  }
}
