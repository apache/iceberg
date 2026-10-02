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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * Resolves deleted keys by scanning data files. No state is kept between calls.
 *
 * <p>The candidate files are planned with the filters of {@link KeyFilters}: the partition of the
 * equality delete files is pinned through its transform and the keys are matched with {@code IN}
 * lists while they fit the comparison budget the table's data file count allows, with value ranges
 * split at the largest gaps of the leading key column above that. Only the key columns and the row
 * position of the candidate files are read, in parallel, and a row is matched by comparing its key
 * with the deleted set. A cluster of hot keys and a few scattered keys therefore cost the files
 * that hold them; keys spread evenly over a large table still match most files of the partition.
 *
 * <p>Data files that carry position delete files instead of deletion vectors are rejected, because
 * a deletion vector replaces all position deletes of its data file.
 */
class ScanKeyPositionResolver implements KeyPositionResolver {

  private final Table table;
  private final DeleteLoader deleteLoader;

  ScanKeyPositionResolver(Table table) {
    this.table = table;
    this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
  }

  @Override
  public Resolution resolve(
      Snapshot base,
      Schema keySchema,
      StructLikeSet keys,
      PartitionSpec deleteSpec,
      StructLike deletePartition)
      throws IOException {
    // the plan filter carries the partition pin and the key filter; only the key filter is passed
    // to the file readers, whose row group filters evaluate column references,
    // not partition transforms
    Expression keyFilter =
        KeyFilters.keyFilter(keySchema, keys, KeyFilters.comparisonsPerFile(base));
    Expression planFilter =
        Expressions.and(
            KeyFilters.partitionFilter(table.schema(), deleteSpec, deletePartition), keyFilter);

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> planned =
        table
            .newScan()
            .useSnapshot(base.snapshotId())
            .filter(planFilter)
            .ignoreResiduals()
            .planFiles()) {
      tasks = Lists.newArrayList(planned);
    }

    Map<String, FileMatches> matches = Maps.newConcurrentMap();
    Tasks.foreach(tasks)
        .executeWith(ThreadPools.getWorkerPool())
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(task -> match(task, keySchema, keys, keyFilter, matches), IOException.class);

    return new ScanResolution(matches.values(), tasks.size());
  }

  /** Reads the key columns and row positions of one data file and records the matching rows. */
  private void match(
      FileScanTask task,
      Schema keySchema,
      StructLikeSet keys,
      Expression keyFilter,
      Map<String, FileMatches> matches)
      throws IOException {
    DataFile file = task.file();

    List<DeleteFile> dvs = Lists.newArrayList();
    for (DeleteFile delete : task.deletes()) {
      if (ContentFileUtil.isDV(delete)) {
        dvs.add(delete);
      } else if (delete.content() == FileContent.POSITION_DELETES) {
        throw new IllegalStateException(
            String.format(
                "Data file %s has position delete file %s, rewrite position deletes into deletion "
                    + "vectors before enabling equality delete conversion",
                file.location(), delete.location()));
      }
      // an attached equality delete stays in the table and is still applied by readers
    }

    PositionDeleteIndex existing =
        dvs.isEmpty() ? null : deleteLoader.loadPositionDeletes(dvs, file.location());

    Schema readSchema =
        new Schema(
            ImmutableList.<Types.NestedField>builder()
                .addAll(keySchema.columns())
                .add(MetadataColumns.ROW_POSITION)
                .build());
    Accessor<StructLike> posAccessor =
        readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    StructProjection keyProjection = StructProjection.create(readSchema, keySchema);
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keySchema.asStruct());

    InputFile input = table.io().newInputFile(file);
    ReadBuilder<Record, Schema> reader =
        FormatModelRegistry.readBuilder(file.format(), Record.class, input);
    Positions positions = new Positions();
    try (CloseableIterable<Record> records =
        reader.project(readSchema).filter(keyFilter).reuseContainers().build()) {
      for (Record record : records) {
        long pos = (long) posAccessor.get(record);
        if (existing != null && existing.isDeleted(pos)) {
          continue;
        }

        if (keys.contains(keyWrapper.wrap(keyProjection.wrap(record)))) {
          positions.add(pos);
        }
      }
    }

    if (!positions.isEmpty()) {
      matches.put(
          file.location(),
          new ScanFileMatches(file.location(), task.spec(), file.partition(), existing, positions));
    }
  }

  private static class ScanResolution implements Resolution {
    private final Collection<FileMatches> matches;
    private final int scannedDataFiles;

    ScanResolution(Collection<FileMatches> matches, int scannedDataFiles) {
      this.matches = matches;
      this.scannedDataFiles = scannedDataFiles;
    }

    @Override
    public Collection<FileMatches> matches() {
      return matches;
    }

    @Override
    public int scannedDataFiles() {
      return scannedDataFiles;
    }
  }

  private static class ScanFileMatches implements FileMatches {
    private final String path;
    private final PartitionSpec spec;
    private final StructLike partition;
    private final PositionDeleteIndex existingDeletes;
    private final Positions positions;

    ScanFileMatches(
        String path,
        PartitionSpec spec,
        StructLike partition,
        PositionDeleteIndex existingDeletes,
        Positions positions) {
      this.path = path;
      this.spec = spec;
      this.partition = partition;
      this.existingDeletes = existingDeletes;
      this.positions = positions;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public PartitionSpec spec() {
      return spec;
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public PositionDeleteIndex existingDeletes() {
      return existingDeletes;
    }

    @Override
    public void forEachPosition(LongConsumer consumer) {
      positions.forEach(consumer);
    }
  }

  /** Growable list of row positions without boxing. */
  private static class Positions {
    private long[] values = new long[64];
    private int size = 0;

    void add(long value) {
      if (size == values.length) {
        long[] grown = new long[values.length * 2];
        System.arraycopy(values, 0, grown, 0, size);
        this.values = grown;
      }

      values[size] = value;
      size++;
    }

    boolean isEmpty() {
      return size == 0;
    }

    void forEach(LongConsumer consumer) {
      for (int i = 0; i < size; i++) {
        consumer.accept(values[i]);
      }
    }
  }
}
