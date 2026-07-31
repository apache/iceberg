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
import java.util.Objects;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Accessors;
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
import org.apache.iceberg.io.FileIO;
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
 * <p>Every file in the task group must be written with the table's current sort order. Sort keys on
 * nested fields are not supported.
 */
class MergingSortedRowDataReader implements PartitionReader<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSortedRowDataReader.class);

  private final CloseableGroup resources;
  private final CloseableIterator<TaggedRow> mergedIterator;
  private final List<RowDataReader> fileReaders;
  // non-null only when sort key columns were added to the read schema beyond what Spark projected
  private final ProjectingInternalRow projectingRow;
  private InternalRow current;
  private FileBlock currentBlock;

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
      FileIO io,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema projection,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    SortOrder sortOrder = table.sortOrder();
    int numFiles = taskGroup.tasks().size();

    Preconditions.checkArgument(
        sortOrder.isSorted(), "Cannot create merging reader for unsorted table %s", table.name());
    Preconditions.checkArgument(
        numFiles > 1, "Merging reader requires multiple files, got %s", numFiles);

    int expectedOrderId = sortOrder.orderId();
    Preconditions.checkArgument(
        taskGroup.tasks().stream()
            .allMatch(task -> Objects.equals(task.file().sortOrderId(), expectedOrderId)),
        "Not all files in task group have the expected sort order %s",
        expectedOrderId);

    LOG.debug(
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
   *
   * <p>Rows are copied on the way into the heap. {@link SortedMerge} advances an iterator before
   * returning the value it just polled, so an uncopied row would be overwritten by the next read
   * from the same file since Spark's Parquet and ORC readers reuse {@link InternalRow} containers.
   * At most one row per file is held at a time, so the copy is bounded by the number of files.
   */
  private CloseableIterable<TaggedRow> readerToIterable(RowDataReader reader, FileScanTask task) {
    FileBlock block = new FileBlock(task.file().location(), task.start(), task.length());
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
                return new TaggedRow(reader.get().copy(), block);
              }

              @Override
              public void close() {
                // Readers are owned by the enclosing CloseableGroup, not by the merge. SortedMerge
                // drops iterators that are empty on the first hasNext() without closing them, so a
                // file whose rows are all deleted would otherwise leak. Closing here too would
                // double-close every reader the merge does drain.
              }
            });
  }

  @Override
  public boolean next() throws IOException {
    if (!mergedIterator.hasNext()) {
      return false;
    }

    TaggedRow tagged = mergedIterator.next();
    // all rows from one task share a FileBlock instance, so identity is enough to detect a switch
    // and avoid re-allocating the block holder entry on every row
    if (tagged.block() != currentBlock) {
      FileBlock block = tagged.block();
      InputFileBlockHolder.set(block.filePath(), block.start(), block.length());
      this.currentBlock = block;
    }

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
   * Builds a comparator for merging {@link InternalRow}s by the given sort order. Each side wraps
   * its row in its own reusable {@link InternalRowWrapper} so the two arguments stay distinct.
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

    List<Object> positions = Lists.newArrayListWithCapacity(projection.columns().size());
    for (Types.NestedField column : projection.columns()) {
      Accessor<StructLike> accessor = mergeSchema.accessorForField(column.fieldId());
      Preconditions.checkArgument(
          accessor != null,
          "Cannot find projected field id %s in merge read schema",
          column.fieldId());
      positions.add(Accessors.toPosition(accessor));
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
      Types.NestedField tableField = tableSchema.findField(fieldId);
      Preconditions.checkArgument(
          tableField != null,
          "Cannot find sort field id %s in schema of table %s",
          fieldId,
          table.name());
      Preconditions.checkArgument(
          TypeUtil.ancestorFields(tableSchema, fieldId).isEmpty(),
          "Merging reader does not support sort keys on nested fields (field id %s in table %s)",
          fieldId,
          table.name());

      if (projection.findField(fieldId) == null
          && missingFields.stream().noneMatch(f -> f.fieldId() == fieldId)) {
        missingFields.add(tableField);
      }
    }

    if (missingFields.isEmpty()) {
      return projection;
    }

    return TypeUtil.join(projection, new Schema(missingFields));
  }

  private record FileBlock(String filePath, long start, long length) {}

  private record TaggedRow(InternalRow row, FileBlock block) {}
}
