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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DeleteFilter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteFilter.class);

  private final String filePath;
  private final List<DeleteFile> posDeletes;
  private final List<DeleteFile> eqDeletes;
  private final Schema requiredSchema;
  private final Schema expectedSchema;
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
      Schema expectedSchema,
      DeleteCounter counter,
      boolean needRowPosCol) {
    this.filePath = filePath;
    this.counter = counter;
    this.expectedSchema = expectedSchema;

    ImmutableList.Builder<DeleteFile> posDeleteBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> eqDeleteBuilder = ImmutableList.builder();
    for (DeleteFile delete : deletes) {
      switch (delete.content()) {
        case POSITION_DELETES:
          LOG.debug("Adding position delete file {} to filter", delete.location());
          posDeleteBuilder.add(delete);
          break;
        case EQUALITY_DELETES:
          LOG.debug("Adding equality delete file {} to filter", delete.location());
          eqDeleteBuilder.add(delete);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown delete file content: " + delete.content());
      }
    }

    this.posDeletes = posDeleteBuilder.build();
    this.eqDeletes = eqDeleteBuilder.build();
    this.requiredSchema =
        fileProjection(tableSchema, expectedSchema, posDeletes, eqDeletes, needRowPosCol);
    this.posAccessor = requiredSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
    this.hasIsDeletedColumn =
        requiredSchema.findField(MetadataColumns.IS_DELETED.fieldId()) != null;
    this.isDeletedColumnPosition = requiredSchema.columns().indexOf(MetadataColumns.IS_DELETED);
  }

  protected DeleteFilter(
      String filePath,
      List<DeleteFile> deletes,
      Schema tableSchema,
      Schema requestedSchema,
      DeleteCounter counter) {
    this(filePath, deletes, tableSchema, requestedSchema, counter, true);
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

  public Schema expectedSchema() {
    return expectedSchema;
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

  protected InputFile loadInputFile(DeleteFile deleteFile) {
    return getInputFile(deleteFile.location());
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
    }

    return deleteRowPositions;
  }

  private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
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

  private static Schema fileProjection(
      Schema tableSchema,
      Schema requestedSchema,
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes,
      boolean needRowPosCol) {
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

    Set<Integer> requiredIds = Sets.newLinkedHashSet();
    if (needRowPosCol && !posDeletes.isEmpty()) {
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

    // Merge requested schema fields with missing IDs (excluding metadata columns)
    Set<Integer> allRequiredIds = mergeFieldIds(requestedSchema, missingIds);

    // Convert field IDs to full column names to preserve nested field paths
    Set<String> columnNames = getColumnNamesForFieldIds(tableSchema, allRequiredIds);

    // Use Schema.select() to create a projection that preserves the full nested structure
    Schema projection = tableSchema.select(columnNames);

    // Add metadata columns if needed
    List<Types.NestedField> columns = Lists.newArrayList(projection.columns());
    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
      columns.add(MetadataColumns.ROW_POSITION);
    }

    if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
      columns.add(MetadataColumns.IS_DELETED);
    }

    return new Schema(columns);
  }

  /**
   * Merges field IDs from requested schema with missing IDs, excluding metadata columns.
   *
   * @param requestedSchema the schema requested by the user
   * @param missingIds the set of field IDs missing from the requested schema
   * @return combined set of field IDs (metadata columns excluded)
   */
  private static Set<Integer> mergeFieldIds(Schema requestedSchema, Set<Integer> missingIds) {
    Set<Integer> allRequiredIds = Sets.newLinkedHashSet();
    allRequiredIds.addAll(TypeUtil.getProjectedIds(requestedSchema));

    // Add missing field IDs, excluding metadata columns (they'll be added at the end)
    for (int fieldId : missingIds) {
      if (fieldId != MetadataColumns.ROW_POSITION.fieldId()
          && fieldId != MetadataColumns.IS_DELETED.fieldId()) {
        allRequiredIds.add(fieldId);
      }
    }

    return allRequiredIds;
  }

  /**
   * Converts field IDs to full column names (e.g., "structData.nestedField").
   *
   * @param tableSchema the table schema to look up field names
   * @param fieldIds the set of field IDs to convert
   * @return a set of full column names preserving nested field paths
   */
  private static Set<String> getColumnNamesForFieldIds(Schema tableSchema, Set<Integer> fieldIds) {
    Set<String> columnNames = Sets.newLinkedHashSet();
    for (int fieldId : fieldIds) {
      String columnName = tableSchema.findColumnName(fieldId);
      Preconditions.checkArgument(
          columnName != null, "Cannot find required field for ID %s", fieldId);
      columnNames.add(columnName);
    }
    return columnNames;
  }
}
