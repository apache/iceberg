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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.SortedMerge;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Range;

/**
 * A {@link PartitionReader} that reads multiple sorted files and merges them into a single sorted
 * stream using a k-way heap merge ({@link SortedMerge}).
 *
 * <p>Every file in the task group must have the same sort order. Sort keys on nested fields are not
 * supported.
 */
class MergingSortedRowDataReader implements PartitionReader<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSortedRowDataReader.class);

  private final CloseableGroup resources;
  private final CloseableIterator<TaggedRow> mergedIterator;
  private final List<RowDataReader> fileReaders;
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
    List<FileScanTask> tasks = Lists.newArrayList(taskGroup.tasks());
    int numFiles = tasks.size();

    Preconditions.checkArgument(
        numFiles > 1, "Merging reader requires multiple files, got %s", numFiles);

    Integer expectedOrderId = tasks.get(0).file().sortOrderId();
    Preconditions.checkArgument(
        expectedOrderId != null && expectedOrderId != SortOrder.unsorted().orderId(),
        "Merging reader requires sorted files, got sort order %s",
        expectedOrderId);
    Preconditions.checkArgument(
        tasks.stream().allMatch(task -> Objects.equals(task.file().sortOrderId(), expectedOrderId)),
        "Not all files in task group have the expected sort order %s",
        expectedOrderId);

    SortOrder sortOrder = table.sortOrders().get(expectedOrderId);
    Preconditions.checkArgument(
        sortOrder != null, "Cannot find sort order %s in table %s", expectedOrderId, table.name());

    LOG.debug(
        "Creating merging reader for {} files with sort order {} in table {}",
        numFiles,
        expectedOrderId,
        table.name());

    // Augment the projected schema with any sort key columns Spark did not request so that
    // SortOrderComparators can access every sort key field during the merge.
    Schema mergeReadSchema = mergeReadSchema(projection, sortOrder, table);
    this.projectingRow = buildProjectingRow(projection, mergeReadSchema);
    UnsafeProjection deepCopyProjection =
        UnsafeProjection.create(SparkSchemaUtil.convert(mergeReadSchema));

    this.resources = new CloseableGroup();
    // The group holds one reader per file plus the merge. Avoid leaking resources when close()
    // failure on one resource is called.
    resources.setSuppressCloseFailure(true);
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

    List<CloseableIterable<TaggedRow>> fileIterables = Lists.newArrayListWithCapacity(tasks.size());
    for (int i = 0; i < tasks.size(); i++) {
      fileIterables.add(
          new TaggedRowIterable(fileReaders.get(i), tasks.get(i), deepCopyProjection));
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
   * A {@link CloseableIterable} over one file's rows, each tagged with the {@link FileBlock} it was
   * read from so the merged stream can report the correct source file. {@code close()} is a no-op:
   * the readers are owned by the enclosing {@link CloseableGroup} (see the constructor), not by the
   * merge.
   */
  private static class TaggedRowIterable implements CloseableIterable<TaggedRow> {
    private final RowDataReader reader;
    private final UnsafeProjection deepCopyProjection;
    private final FileBlock block;

    private TaggedRowIterable(
        RowDataReader reader, FileScanTask task, UnsafeProjection deepCopyProjection) {
      this.reader = reader;
      this.deepCopyProjection = deepCopyProjection;
      this.block = new FileBlock(task.file().location(), task.start(), task.length());
    }

    @Override
    public CloseableIterator<TaggedRow> iterator() {
      return new TaggedRowIterator(reader, deepCopyProjection, block);
    }

    @Override
    public void close() {
      // No-op. See TaggedRowIterator#close.
    }
  }

  /**
   * Adapts a {@link RowDataReader} to an iterator of {@link TaggedRow}. {@code hasNext()} advances
   * the reader and caches the result so {@code next()} returns the current row without advancing it
   * again.
   *
   * <p>Rows are deep-copied into a self-contained {@code UnsafeRow} before entering the heap.
   * {@link SortedMerge} advances an iterator before returning the value it just polled, so an
   * uncopied row would be overwritten by the next read from the same file since Spark's Parquet and
   * ORC readers reuse {@link InternalRow} containers.
   */
  private static class TaggedRowIterator implements CloseableIterator<TaggedRow> {
    private final RowDataReader reader;
    private final UnsafeProjection deepCopyProjection;
    private final FileBlock block;
    private boolean advanced = false;
    private boolean hasNext = false;

    private TaggedRowIterator(
        RowDataReader reader, UnsafeProjection deepCopyProjection, FileBlock block) {
      this.reader = reader;
      this.deepCopyProjection = deepCopyProjection;
      this.block = block;
    }

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
      InternalRow deepCopy = deepCopyProjection.apply(reader.get()).copy();
      return new TaggedRow(deepCopy, block);
    }

    @Override
    public void close() {
      // Readers are owned by the enclosing CloseableGroup, not by the merge. SortedMerge drops
      // iterators that are empty on the first hasNext() without closing them, so a file whose rows
      // are all deleted would otherwise leak. Closing here too would double-close every reader the
      // merge does drain.
    }
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
    projectingRow.project(merged);
    this.current = projectingRow;

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
   * the requested projection. The remap is the identity when no extra columns were added.
   */
  private static ProjectingInternalRow buildProjectingRow(Schema projection, Schema mergeSchema) {
    int numColumns = projection.columns().size();
    Preconditions.checkArgument(
        mergeSchema.columns().subList(0, numColumns).equals(projection.columns()),
        "Projection must be a prefix of the merge read schema");
    StructType sparkSchema = SparkSchemaUtil.convert(projection);
    return new ProjectingInternalRow(sparkSchema, new Range.Exclusive(0, numColumns, 1));
  }

  /**
   * Returns the schema to use when reading each file. This is the requested {@code projection}
   * augmented with any sort key columns that are not already present, so the merge comparator can
   * access every sort key field regardless of what Spark projected.
   */
  private static Schema mergeReadSchema(Schema projection, SortOrder sortOrder, Table table) {
    Schema tableSchema = table.schema();
    validateSortKeys(sortOrder, tableSchema, table.name());

    List<Types.NestedField> missingFields = Lists.newArrayList();
    for (SortField sortField : sortOrder.fields()) {
      int fieldId = sortField.sourceId();
      if (projection.findField(fieldId) != null
          || missingFields.stream().anyMatch(f -> f.fieldId() == fieldId)) {
        continue;
      }

      // A missing field can only be added to the read schema as a top-level column, so a nested
      // sort key is only supported when it is already part of the requested projection.
      Preconditions.checkArgument(
          tableSchema.asStruct().field(fieldId) != null,
          "Merging reader does not support sort keys on nested fields (field id %s in table %s)",
          fieldId,
          table.name());
      missingFields.add(tableSchema.findField(fieldId));
    }

    if (missingFields.isEmpty()) {
      return projection;
    }

    return TypeUtil.join(projection, new Schema(missingFields));
  }

  /** Validates that every sort key exists in the table schema and is not UUID-typed. */
  private static void validateSortKeys(SortOrder sortOrder, Schema tableSchema, String tableName) {
    for (SortField sortField : sortOrder.fields()) {
      int fieldId = sortField.sourceId();
      Types.NestedField tableField = tableSchema.findField(fieldId);
      Preconditions.checkArgument(
          tableField != null,
          "Cannot find sort field id %s in schema of table %s",
          fieldId,
          tableName);

      // Iceberg orders UUIDs by their bit pattern while Spark orders them lexicographically,
      // https://github.com/apache/iceberg/issues/14216
      Type resultType = sortField.transform().getResultType(tableField.type());
      Preconditions.checkArgument(
          resultType.typeId() != Type.TypeID.UUID,
          "Merging reader does not support UUID-typed sort keys (field id %s in table %s)",
          fieldId,
          tableName);
    }
  }

  private record FileBlock(String filePath, long start, long length) {}

  private record TaggedRow(InternalRow row, FileBlock block) {}
}
