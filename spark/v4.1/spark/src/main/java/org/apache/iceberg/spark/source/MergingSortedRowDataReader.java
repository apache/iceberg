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
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
 * Returns the rows of a task group's files as one stream in the files' shared sort order.
 *
 * <p>All files must have the same sort order. Sort orders with a UUID key, a floating point key
 * followed by another key, or a nested key that is not projected are rejected. All files are open
 * at once and each loads its own deletes.
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
    int numTasks = tasks.size();

    Preconditions.checkArgument(
        numTasks > 1, "Merging reader requires at least two tasks, got %s", numTasks);

    Integer expectedOrderId = tasks.get(0).file().sortOrderId();
    Preconditions.checkArgument(
        expectedOrderId != null && expectedOrderId != SortOrder.unsorted().orderId(),
        "Merging reader requires sorted files, got sort order %s",
        expectedOrderId);
    for (FileScanTask task : tasks) {
      Preconditions.checkArgument(
          Objects.equals(task.file().sortOrderId(), expectedOrderId),
          "Cannot merge files with different sort orders: %s has sort order %s, expected %s",
          task.file().location(),
          task.file().sortOrderId(),
          expectedOrderId);
    }

    SortOrder sortOrder = table.sortOrders().get(expectedOrderId);
    Preconditions.checkArgument(
        sortOrder != null, "Cannot find sort order %s in table %s", expectedOrderId, table.name());

    LOG.debug(
        "Creating merging reader for {} tasks with sort order {} in table {}",
        numTasks,
        expectedOrderId,
        table.name());

    Schema mergeReadSchema = mergeReadSchema(projection, sortOrder, table);
    this.projectingRow = buildProjectingRow(projection);
    UnsafeProjection deepCopyProjection =
        UnsafeProjection.create(SparkSchemaUtil.convert(mergeReadSchema));

    this.resources = new CloseableGroup();
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
    public void close() {}
  }

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
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      advanced = false;
      // the next row from this file is read before this one is returned, and readers reuse row
      // containers, including the structs inside arrays and maps, so a shallow copy is not enough
      InternalRow deepCopy = deepCopyProjection.apply(reader.get()).copy();
      return new TaggedRow(deepCopy, block);
    }

    @Override
    public void close() {
      // The enclosing CloseableGroup owns the reader. The merge cannot, as it never sees readers
      // that were not reached when construction fails, or a reader it polled whose next read threw.
    }
  }

  @Override
  public boolean next() throws IOException {
    if (!mergedIterator.hasNext()) {
      return false;
    }

    TaggedRow tagged = mergedIterator.next();
    // rows from one task share a FileBlock instance
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
    long totalDeletes = 0L;
    for (RowDataReader reader : fileReaders) {
      totalDeletes += reader.counter().get();
    }

    return new CustomTaskMetric[] {
      new TaskNumSplits(fileReaders.size()), new TaskNumDeletes(totalDeletes)
    };
  }

  private static Comparator<InternalRow> buildComparator(
      Schema mergeReadSchema, SortOrder sortOrder) {
    StructType sparkSchema = SparkSchemaUtil.convert(mergeReadSchema);
    Comparator<StructLike> keyComparator =
        SortOrderComparators.forSchema(mergeReadSchema, sortOrder);
    // each side needs its own wrapper so the two rows being compared stay distinct
    InternalRowWrapper left = new InternalRowWrapper(sparkSchema, mergeReadSchema.asStruct());
    InternalRowWrapper right = new InternalRowWrapper(sparkSchema, mergeReadSchema.asStruct());
    return (r1, r2) -> keyComparator.compare(left.wrap(r1), right.wrap(r2));
  }

  private static ProjectingInternalRow buildProjectingRow(Schema projection) {
    // the merge read schema starts with the projection
    int numColumns = projection.columns().size();
    StructType sparkSchema = SparkSchemaUtil.convert(projection);
    return new ProjectingInternalRow(sparkSchema, new Range.Exclusive(0, numColumns, 1));
  }

  /**
   * Returns the requested {@code projection} with any sort key columns it lacks appended after it,
   * so the merge comparator can read every sort key.
   */
  private static Schema mergeReadSchema(Schema projection, SortOrder sortOrder, Table table) {
    Schema tableSchema = table.schema();
    validateSortKeys(sortOrder, projection, tableSchema, table.name());

    Set<Integer> missingIds = Sets.newHashSet();
    for (SortField sortField : sortOrder.fields()) {
      int fieldId = sortField.sourceId();
      if (projection.findField(fieldId) == null) {
        // an unprojected nested key would have to be added as a top-level column
        Preconditions.checkArgument(
            tableSchema.asStruct().field(fieldId) != null,
            "Cannot merge on nested sort key %s: it must be included in the projection",
            tableSchema.findColumnName(fieldId));
        missingIds.add(fieldId);
      }
    }

    if (missingIds.isEmpty()) {
      return projection;
    }

    return TypeUtil.join(projection, TypeUtil.select(tableSchema, missingIds));
  }

  private static void validateSortKeys(
      SortOrder sortOrder, Schema projection, Schema tableSchema, String tableName) {
    List<SortField> sortFields = sortOrder.fields();
    for (int i = 0; i < sortFields.size(); i++) {
      SortField sortField = sortFields.get(i);
      int fieldId = sortField.sourceId();
      Types.NestedField field = projection.findField(fieldId);
      if (field == null) {
        field = tableSchema.findField(fieldId);
      }

      Preconditions.checkArgument(
          field != null,
          "Cannot find sort field id %s in the projection or the schema of table %s",
          fieldId,
          tableName);

      // Iceberg orders UUIDs by bit pattern and Spark orders them as strings:
      // https://github.com/apache/iceberg/issues/14216
      Type.TypeID resultType = sortField.transform().getResultType(field.type()).typeId();
      Preconditions.checkArgument(
          resultType != Type.TypeID.UUID,
          "Cannot merge on UUID sort key %s: Iceberg and Spark order UUIDs differently",
          field.name());

      // Iceberg orders -0.0 before 0.0 and Spark treats them as equal, so the next sort key breaks
      // the tie differently
      boolean floatingPoint = resultType == Type.TypeID.FLOAT || resultType == Type.TypeID.DOUBLE;
      Preconditions.checkArgument(
          !floatingPoint || i == sortFields.size() - 1,
          "Cannot merge on floating point sort key %s followed by other sort keys: "
              + "Iceberg and Spark order -0.0 and 0.0 differently",
          field.name());
    }
  }

  private record FileBlock(String filePath, long start, long length) {}

  private record TaggedRow(InternalRow row, FileBlock block) {}
}
