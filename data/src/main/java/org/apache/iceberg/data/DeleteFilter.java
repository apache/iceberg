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
package org.apache.iceberg.data;

import com.github.benmanes.caffeine.cache.Cache;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeleteFilter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteFilter.class);
  private static final long DEFAULT_SET_FILTER_THRESHOLD = 100_000L;
  private static final Schema POS_DELETE_SCHEMA =
      new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

  private final long setFilterThreshold;
  private final String filePath;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final Schema requiredSchema;
  private final Accessor<StructLike> posAccessor;
  private final boolean hasIsDeletedColumn;
  private final int isDeletedColumnPosition;
  private final DeleteCounter counter;

  private PositionDeleteIndex deleteRowPositions = null;
  private List<Predicate<T>> isInDeleteSets = null;
  private Predicate<T> eqDeleteRows = null;

  protected DeleteFilter(
      String filePath,
      List<DeleteFile> deletes,
      Schema tableSchema,
      Schema requestedSchema,
      DeleteCounter counter) {
    this.setFilterThreshold = DEFAULT_SET_FILTER_THRESHOLD;
    this.filePath = filePath;
    this.counter = counter;

    ImmutableList.Builder<DeleteFile> posDeleteBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> eqDeleteBuilder = ImmutableList.builder();
    for (DeleteFile delete : deletes) {
      switch (delete.content()) {
        case POSITION_DELETES:
          LOG.debug("Adding position delete file {} to filter", delete.path());
          posDeleteBuilder.add(delete);
          break;
        case EQUALITY_DELETES:
          LOG.debug("Adding equality delete file {} to filter", delete.path());
          eqDeleteBuilder.add(delete);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown delete file content: " + delete.content());
      }
    }

    this.posDeletes = posDeleteBuilder.build();
    this.eqDeletes = eqDeleteBuilder.build();
    this.requiredSchema = fileProjection(tableSchema, requestedSchema, posDeletes, eqDeletes);
    this.posAccessor = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    this.hasIsDeletedColumn =
        requiredSchema.findField(MetadataColumns.IS_DELETED.fieldId()) != null;
    this.isDeletedColumnPosition = requiredSchema.columns().indexOf(MetadataColumns.IS_DELETED);
  }

  protected DeleteFilter(
      String filePath, List<DeleteFile> deletes, Schema tableSchema, Schema requestedSchema) {
    this(filePath, deletes, tableSchema, requestedSchema, new DeleteCounter());
  }

  protected int columnIsDeletedPosition() {
    return isDeletedColumnPosition;
  }

  public Schema requiredSchema() {
    return requiredSchema;
  }

  public boolean hasPosDeletes() {
    return !posDeletes.isEmpty();
  }

  public boolean hasEqDeletes() {
    return !eqDeletes.isEmpty();
  }

  public void incrementDeleteCount() {
    counter.increment();
  }

  Accessor<StructLike> posAccessor() {
    return posAccessor;
  }

  protected abstract StructLike asStructLike(T record);

  protected abstract InputFile getInputFile(String location);

  protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

  public CloseableIterable<T> filter(
      CloseableIterable<T> records, Map<String, PositionDeleteIndex> posIndexMap) {
    Optional<PositionDeleteIndex> posIndex =
        Optional.ofNullable(posIndexMap).map(cache -> cache.get(filePath));

    boolean skipPosDeletes = posIndexMap != null && !posIndex.isPresent();
    return applyEqDeletes(!skipPosDeletes ? applyPosDeletes(records, posIndex) : records);
  }

  private List<Predicate<T>> applyEqDeletes() {
    if (isInDeleteSets != null) {
      return isInDeleteSets;
    }

    isInDeleteSets = Lists.newArrayList();
    if (eqDeletes.isEmpty()) {
      return isInDeleteSets;
    }

    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
      filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
        filesByDeleteIds.asMap().entrySet()) {
      Set<Integer> ids = entry.getKey();
      Iterable<DeleteFile> deletes = entry.getValue();

      Schema deleteSchema = TypeUtil.select(requiredSchema, ids);
      InternalRecordWrapper wrapper = new InternalRecordWrapper(deleteSchema.asStruct());

      // a projection to select and reorder fields of the file schema to match the delete rows
      StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

      Iterable<CloseableIterable<Record>> deleteRecords =
          Iterables.transform(deletes, delete -> openDeletes(delete, deleteSchema));

      // copy the delete records because they will be held in a set
      CloseableIterable<Record> records =
          CloseableIterable.transform(CloseableIterable.concat(deleteRecords), Record::copy);

      StructLikeSet deleteSet =
          Deletes.toEqualitySet(
              CloseableIterable.transform(records, wrapper::copyFor), deleteSchema.asStruct());

      Predicate<T> isInDeleteSet =
          record -> deleteSet.contains(projectRow.wrap(asStructLike(record)));
      isInDeleteSets.add(isInDeleteSet);
    }

    return isInDeleteSets;
  }

  public CloseableIterable<T> findEqualityDeleteRows(CloseableIterable<T> records) {
    // Predicate to test whether a row has been deleted by equality deletions.
    Predicate<T> deletedRows = applyEqDeletes().stream().reduce(Predicate::or).orElse(t -> false);

    return CloseableIterable.filter(records, deletedRows);
  }

  private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    Predicate<T> isEqDeleted = applyEqDeletes().stream().reduce(Predicate::or).orElse(t -> false);

    return createDeleteIterable(records, isEqDeleted);
  }

  protected void markRowDeleted(T item) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement markRowDeleted");
  }

  public Predicate<T> eqDeletedRowFilter() {
    if (eqDeleteRows == null) {
      eqDeleteRows =
          applyEqDeletes().stream().map(Predicate::negate).reduce(Predicate::and).orElse(t -> true);
    }
    return eqDeleteRows;
  }

  public PositionDeleteIndex deletedRowPositions() {
    if (posDeletes.isEmpty()) {
      return null;
    }

    if (deleteRowPositions == null) {
      List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);
      deleteRowPositions = Deletes.toPositionIndex(filePath, deletes);
    }
    return deleteRowPositions;
  }

  private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    return applyPosDeletes(records, Optional.empty());
  }

  private CloseableIterable<T> applyPosDeletes(
      CloseableIterable<T> records, Optional<PositionDeleteIndex> posIndex) {
    if (posDeletes.isEmpty()) {
      return records;
    }
    List<CloseableIterable<Record>> deletes = Lists.transform(posDeletes, this::openPosDeletes);

    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeletes.stream().mapToLong(DeleteFile::recordCount).sum() < setFilterThreshold) {
      PositionDeleteIndex pi = posIndex.orElseGet(() -> Deletes.toPositionIndex(filePath, deletes));
      Predicate<T> isDeleted = record -> pi.isDeleted(pos(record));
      return createDeleteIterable(records, isDeleted);
    }

    return hasIsDeletedColumn
        ? Deletes.streamingMarker(
            records, this::pos, Deletes.deletePositions(filePath, deletes), this::markRowDeleted)
        : Deletes.streamingFilter(
            records, this::pos, Deletes.deletePositions(filePath, deletes), counter);
  }

  private CloseableIterable<T> createDeleteIterable(
      CloseableIterable<T> records, Predicate<T> isDeleted) {
    return hasIsDeletedColumn
        ? Deletes.markDeleted(records, isDeleted, this::markRowDeleted)
        : Deletes.filterDeleted(records, isDeleted, counter);
  }

  private CloseableIterable<Record> openPosDeletesNoFilter(DeleteFile deleteFile) {
    return openDeletes(deleteFile, POS_DELETE_SCHEMA, null);
  }

  private CloseableIterable<Record> openPosDeletes(DeleteFile deleteFile) {
    return openDeletes(deleteFile, POS_DELETE_SCHEMA);
  }

  private CloseableIterable<Record> openDeletes(DeleteFile deleteFile, Schema deleteSchema) {
    return openDeletes(deleteFile, deleteSchema, filePath);
  }

  private CloseableIterable<Record> openDeletes(
      DeleteFile deleteFile, Schema deleteSchema, String pathFilter) {
    LOG.trace("Opening delete file {}", deleteFile.path());
    InputFile input = getInputFile(deleteFile.path().toString());
    switch (deleteFile.format()) {
      case AVRO:
        return Avro.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(DataReader::create)
            .build();

      case PARQUET:
        return Parquet.read(input)
            .project(deleteSchema)
            .reuseContainers()
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
            .filter(deletesFilter(deleteFile, pathFilter))
            .build();

      case ORC:
        // Reusing containers is automatic for ORC. No need to set 'reuseContainers' here.
        return ORC.read(input)
            .project(deleteSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(deleteSchema, fileSchema))
            .filter(deletesFilter(deleteFile, pathFilter))
            .build();
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot read deletes, %s is not a supported format: %s",
                deleteFile.format().name(), deleteFile.path()));
    }
  }

  private Expression deletesFilter(DeleteFile deleteFile, String pathFilter) {
    if (pathFilter != null && FileContent.POSITION_DELETES == deleteFile.content()) {
      return Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), pathFilter);
    }
    return null;
  }

  private static Schema fileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes) {
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

    Set<Integer> requiredIds = Sets.newLinkedHashSet();
    if (!posDeletes.isEmpty()) {
      requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    for (DeleteFile eqDelete : eqDeletes) {
      requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    Set<Integer> missingIds =
        Sets.newLinkedHashSet(
            Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
      return requestedSchema;
    }

    // TODO: support adding nested columns. this will currently fail when finding nested columns to
    // add
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
      if (fieldId == MetadataColumns.ROW_POSITION.fieldId()
          || fieldId == MetadataColumns.IS_DELETED.fieldId()) {
        continue; // add _pos and _deleted at the end
      }

      Types.NestedField field = tableSchema.asStruct().field(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find required field for ID %s", fieldId);

      columns.add(field);
    }

    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
      columns.add(MetadataColumns.ROW_POSITION);
    }

    if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
      columns.add(MetadataColumns.IS_DELETED);
    }

    return new Schema(columns);
  }

  public Map<String, PositionDeleteIndex> createPosIndexMap(
      Map<String, PositionDeleteIndex> posIndexMap, Iterable<FileScanTask> fileTasks) {
    return createPosIndexMap(posIndexMap, fileTasks, DEFAULT_SET_FILTER_THRESHOLD);
  }

  public Map<String, PositionDeleteIndex> createPosIndexMap(
      Map<String, PositionDeleteIndex> posIndexMap,
      Iterable<FileScanTask> fileTasks,
      long filterThreshold) {
    Preconditions.checkNotNull(posIndexMap, "posIndexMap cannot be null");

    if (!posIndexMap.isEmpty()) {
      return posIndexMap;
    }

    List<DeleteFile> posDeleteFiles = distinctPosDeletes(fileTasks);
    if (posDeleteFiles.isEmpty()) {
      return ImmutableMap.of();
    }

    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeleteFiles.stream().mapToLong(DeleteFile::recordCount).sum() < filterThreshold) {
      CharSequenceSet filePaths =
          CharSequenceSet.of(Iterables.transform(fileTasks, task -> task.file().path()));
      List<CloseableIterable<Record>> deletes =
          Lists.transform(posDeleteFiles, this::openPosDeletesNoFilter);
      posIndexMap.putAll(Deletes.toPositionIndexMap(filePaths, deletes));
      return posIndexMap;
    }
    return null;
  }

  public Map<String, PositionDeleteIndex> createPosIndexMap(
      Cache<CharSequence, Map<String, PositionDeleteIndex>> posIndexCache,
      Iterable<FileScanTask> fileTasks) {
    return createPosIndexMap(posIndexCache, fileTasks, DEFAULT_SET_FILTER_THRESHOLD);
  }

  public Map<String, PositionDeleteIndex> createPosIndexMap(
      Cache<CharSequence, Map<String, PositionDeleteIndex>> posIndexCache,
      Iterable<FileScanTask> fileTasks,
      long filterThreshold) {
    Preconditions.checkNotNull(posIndexCache, "posIndexCache cannot be null");

    List<DeleteFile> posDeleteFiles = distinctPosDeletes(fileTasks);
    if (posDeleteFiles.isEmpty()) {
      return ImmutableMap.of();
    }

    // if there are fewer deletes than a reasonable number to keep in memory, use a set
    if (posDeleteFiles.stream().mapToLong(DeleteFile::recordCount).sum() < filterThreshold) {
      CharSequenceSet filePaths =
          CharSequenceSet.of(Iterables.transform(fileTasks, task -> task.file().path()));

      Map<String, PositionDeleteIndex> posIndexMap = Maps.newConcurrentMap();
      // open all of the delete files in parallel, use index to avoid reordering
      Tasks.Task<DeleteFile, RuntimeException> task =
          deleteFile ->
              Maps.filterKeys(
                      posIndexCache.get(
                          deleteFile.path(),
                          func -> {
                            LOG.debug("Cache miss: {}", deleteFile.path());
                            Instant start = Instant.now();
                            Map<String, PositionDeleteIndex> res =
                                Deletes.toPositionIndexMap(openPosDeletesNoFilter(deleteFile));
                            LOG.debug(
                                "Cache load: {}; Time taken: {} ms;",
                                deleteFile.path(),
                                Duration.between(start, Instant.now()).toMillis());
                            return res;
                          }),
                      filePaths::contains)
                  .forEach((key, value) -> posIndexMap.merge(key, value, PositionDeleteIndex::or));
      Tasks.foreach(posDeleteFiles)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(ThreadPools.getDeleteWorkerPool())
          .run(task);
      LOG.debug("Cache size: {}", posIndexCache.estimatedSize());
      return posIndexMap;
    }
    return null;
  }

  private static List<DeleteFile> distinctPosDeletes(Iterable<FileScanTask> fileTasks) {
    CharSequenceSet deleteSet = CharSequenceSet.empty(Comparators.filePath());

    return FluentIterable.from(fileTasks)
        .transformAndConcat(FileScanTask::deletes)
        .filter(deleteFile -> FileContent.POSITION_DELETES == deleteFile.content())
        .filter(deleteFile -> deleteSet.add(deleteFile.path()))
        .toList();
  }
}
