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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.SortedMerge;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

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

  private record TaggedRow(InternalRow row, String filePath, long start, long length) {}

  private final CloseableGroup resources;
  private final CloseableIterator<TaggedRow> mergedIterator;
  private final List<RowDataReader> fileReaders;
  // non-null only when sort key columns were added to the read schema beyond what Spark projected
  private final ProjectingInternalRow projectingRow;
  private InternalRow current;

  MergingSortedRowDataReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.io(),
        partition.taskGroup(),
        partition.projection(),
        partition.isCaseSensitive(),
        partition.cacheDeleteFilesOnExecutors());
  }

  MergingSortedRowDataReader(
      Table table,
      org.apache.iceberg.io.FileIO io,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema projection,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    SortOrder sortOrder = table.sortOrder();

    int numFiles = taskGroup.tasks().size();

    Preconditions.checkState(
        sortOrder.isSorted(), "Cannot create merging reader for unsorted table %s", table.name());
    Preconditions.checkState(
        numFiles > 1, "Merging reader requires multiple files, got %s", numFiles);

    int expectedOrderId = sortOrder.orderId();
    Preconditions.checkState(
        taskGroup.tasks().stream().allMatch(task -> task.file().sortOrderId() == expectedOrderId),
        "Not all files in task group have the expected sort order %s",
        expectedOrderId);

    LOG.info(
        "Creating merging reader for {} files with sort order {} in table {}",
        numFiles,
        sortOrder.orderId(),
        table.name());

    // Augment the projected schema with any sort key columns Spark did not request so that
    // SortOrderComparators can access every sort key field during the merge.
    Schema mergeReadSchema = mergeReadSchema(projection, sortOrder, table);
    this.projectingRow = buildProjectingRow(projection, mergeReadSchema);

    this.resources = new CloseableGroup();
    List<FileScanTask> tasks = Lists.newArrayList(taskGroup.tasks());
    this.fileReaders =
        tasks.stream()
            .map(
                task ->
                    new RowDataReader(
                        table,
                        io,
                        new BaseScanTaskGroup<>(ImmutableList.of(task)),
                        mergeReadSchema,
                        caseSensitive,
                        cacheDeleteFilesOnExecutors))
            .toList();
    fileReaders.forEach(resources::addCloseable);
    // Wrap each reader as a CloseableIterable and feed into SortedMerge.
    List<CloseableIterable<TaggedRow>> fileIterables = Lists.newArrayListWithCapacity(tasks.size());
    for (int i = 0; i < tasks.size(); i++) {
      fileIterables.add(readerToIterable(fileReaders.get(i), tasks.get(i)));
    }
    Comparator<InternalRow> rowComparator = buildComparator(mergeReadSchema, sortOrder);
    SortedMerge<TaggedRow> sortedMerge =
        new SortedMerge<>((a, b) -> rowComparator.compare(a.row(), b.row()), fileIterables);
    resources.addCloseable(sortedMerge);
    boolean threw = true;
    try {
      this.mergedIterator = sortedMerge.iterator();
      threw = false;
    } finally {
      if (threw) {
        Exceptions.close(resources, true);
      }
    }
  }

  /**
   * Adapts a {@link RowDataReader} to a {@link CloseableIterable} for use with {@link SortedMerge}.
   * Each row is copied and tagged with its source file metadata before it enters the priority queue
   * because Spark's Parquet/ORC readers reuse {@link InternalRow} instances for performance.
   */
  private CloseableIterable<TaggedRow> readerToIterable(RowDataReader reader, FileScanTask task) {
    String filePath = task.file().location();
    long start = task.start();
    long length = task.length();
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
              public TaggedRow next() {
                if (!advanced) {
                  hasNext();
                }
                advanced = false;
                return new TaggedRow(reader.get().copy(), filePath, start, length);
              }

              @Override
              public void close() {
                // No-op. The RowDataReaders are owned by the enclosing CloseableGroup
                // (resources) and closed exactly once from close(). SortedMerge cannot be the
                // sole owner because it filters out empty iterators (for example a file whose
                // rows are all removed by deletes), so those readers would never be closed
                // through the merge. Closing here as well would double-close the readers that
                // SortedMerge does drain.
              }
            });
  }

  @Override
  public boolean next() throws IOException {
    if (!mergedIterator.hasNext()) {
      return false;
    }

    TaggedRow tagged = mergedIterator.next();
    InputFileBlockHolder.set(tagged.filePath(), tagged.start(), tagged.length());

    InternalRow merged = tagged.row();
    if (projectingRow == null) {
      this.current = merged;
    } else {
      projectingRow.project(merged);
      this.current = projectingRow;
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
   * Returns a {@link ProjectingInternalRow} that remaps columns from the wider merge schema back to
   * the requested projection, or {@code null} if no extra columns were added.
   */
  private static ProjectingInternalRow buildProjectingRow(Schema projection, Schema mergeSchema) {
    if (projection.columns().size() == mergeSchema.columns().size()) {
      return null;
    }

    List<Types.NestedField> mergeColumns = mergeSchema.columns();
    List<Object> positions = Lists.newArrayListWithCapacity(projection.columns().size());

    for (int i = 0; i < projection.columns().size(); i++) {
      int fieldId = projection.columns().get(i).fieldId();
      boolean found = false;
      for (int j = 0; j < mergeColumns.size(); j++) {
        if (mergeColumns.get(j).fieldId() == fieldId) {
          positions.add(j);
          found = true;
          break;
        }
      }
      Preconditions.checkState(
          found, "Projection field id=%s not found in merge read schema — this is a bug", fieldId);
    }

    StructType sparkSchema = SparkSchemaUtil.convert(projection);
    return new ProjectingInternalRow(sparkSchema, JavaConverters.asScala(positions).toIndexedSeq());
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
      Preconditions.checkState(
          tableSchema.columns().stream().anyMatch(col -> col.fieldId() == fieldId),
          "Merging reader does not support sort keys on nested fields (field id %s in table %s)",
          fieldId,
          table.name());
      if (projection.findField(fieldId) == null
          && missingFields.stream().noneMatch(f -> f.fieldId() == fieldId)) {
        Types.NestedField tableField = tableSchema.findField(fieldId);
        Preconditions.checkState(
            tableField != null,
            "Cannot find sort field id %s in schema of table %s",
            fieldId,
            table.name());
        missingFields.add(tableField);
      }
    }

    if (missingFields.isEmpty()) {
      return projection;
    }

    return TypeUtil.join(projection, new Schema(missingFields));
  }
}
