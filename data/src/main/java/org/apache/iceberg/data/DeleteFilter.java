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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeleteFilter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteFilter.class);

  private final String filePath;
  private final DeleteFile dv;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final Schema requiredSchema;
  private final Accessor<StructLike> posAccessor;
  private final boolean hasIsDeletedColumn;
  private final int isDeletedColumnPosition;
  private final DeleteCounter counter;

  private volatile DeleteLoader deleteLoader = null;
  private PositionDeleteIndex deleteRowPositions = null;
  private List<Predicate<T>> isInDeleteSets = null;
  private Predicate<T> eqDeleteRows = null;

  protected DeleteFilter(
      String filePath,
      List<DeleteFile> deletes,
      Schema tableSchema,
      Schema requestedSchema,
      DeleteCounter counter) {
    this.filePath = filePath;
    this.counter = counter;

    DeleteFile foundDV = null;
    List<DeleteFile> foundPosDeletes = Lists.newArrayList();
    List<DeleteFile> foundEqDeletes = Lists.newArrayList();

    for (DeleteFile delete : deletes) {
      switch (delete.content()) {
        case POSITION_DELETES:
          if (ContentFileUtil.isDV(delete)) {
            Preconditions.checkArgument(foundDV == null, "Multiple DVs for %s", filePath);
            LOG.debug("Adding DV {} to filter", ContentFileUtil.dvDesc(delete));
            foundDV = delete;
          } else {
            LOG.debug("Adding position delete file {} to filter", delete.path());
            foundPosDeletes.add(delete);
          }
          break;
        case EQUALITY_DELETES:
          LOG.debug("Adding equality delete file {} to filter", delete.path());
          foundEqDeletes.add(delete);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown delete file content: " + delete.content());
      }
    }

    Preconditions.checkArgument(
        foundDV == null || foundPosDeletes.isEmpty(),
        "Can't have both DV and position delete files for %s",
        filePath);

    this.dv = foundDV;
    this.posDeletes = Collections.unmodifiableList(foundPosDeletes);
    this.eqDeletes = Collections.unmodifiableList(foundEqDeletes);
    this.requiredSchema = fileProjection(tableSchema, requestedSchema, dv, posDeletes, eqDeletes);
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
    return dv != null || !posDeletes.isEmpty();
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

  protected InputFile loadInputFile(DeleteFile deleteFile) {
    return getInputFile(deleteFile.path().toString());
  }

  protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
  }

  protected DeleteLoader newDeleteLoader() {
    return new BaseDeleteLoader(this::loadInputFile);
  }

  private DeleteLoader deleteLoader() {
    if (deleteLoader == null) {
      synchronized (this) {
        if (deleteLoader == null) {
          this.deleteLoader = newDeleteLoader();
        }
      }
    }

    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
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

      // a projection to select and reorder fields of the file schema to match the delete rows
      StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

      StructLikeSet deleteSet = deleteLoader().loadEqualityDeletes(deletes, deleteSchema);
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
    if (deleteRowPositions == null && !posDeletes.isEmpty()) {
      this.deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
    } else if (deleteRowPositions == null && dv != null) {
      this.deleteRowPositions = deleteLoader().loadDV(dv);
    }

    return deleteRowPositions;
  }

  private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (dv == null && posDeletes.isEmpty()) {
      return records;
    }

    PositionDeleteIndex positionIndex = deletedRowPositions();
    Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));
    return createDeleteIterable(records, isDeleted);
  }

  private CloseableIterable<T> createDeleteIterable(
      CloseableIterable<T> records, Predicate<T> isDeleted) {
    return hasIsDeletedColumn
        ? Deletes.markDeleted(records, isDeleted, this::markRowDeleted)
        : Deletes.filterDeleted(records, isDeleted, counter);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Schema fileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      DeleteFile dv,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes) {
    if (dv == null && posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

    Set<Integer> requiredIds = Sets.newLinkedHashSet();
    if (dv != null || !posDeletes.isEmpty()) {
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
}
