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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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

  private static final Map<Integer, Types.NestedField> METADATA_COLUMNS_BY_ID =
      MetadataColumns.metadataColumns().stream()
          .collect(ImmutableMap.toImmutableMap(Types.NestedField::fieldId, field -> field));

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

  @VisibleForTesting
  static Schema fileProjection(
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

    // Separate missing IDs into data and metadata columns
    Set<Integer> missingDataIds = Sets.newLinkedHashSet();
    Set<Integer> missingMetadataIds = Sets.newLinkedHashSet();
    for (int fieldId : missingIds) {
      if (MetadataColumns.isMetadataColumn(fieldId)) {
        missingMetadataIds.add(fieldId);
      } else {
        missingDataIds.add(fieldId);
      }
    }

    // Build column names for all requested fields plus missing data fields
    // We need to use Schema.select() on ALL fields together to properly handle
    // cases where multiple nested fields come from the same parent struct
    List<String> allColumnNames = Lists.newArrayList();

    for (Types.NestedField field : requestedSchema.columns()) {
      String columnName = requestedSchema.findColumnName(field.fieldId());
      Preconditions.checkArgument(
          columnName != null, "Cannot find column name for field ID %s", field.fieldId());
      allColumnNames.add(columnName);
    }

    for (int fieldId : missingDataIds) {
      String columnName = tableSchema.findColumnName(fieldId);
      Preconditions.checkArgument(
          columnName != null, "Cannot find column name for field ID %s", fieldId);
      allColumnNames.add(columnName);
    }

    // Use Schema.select to get properly nested structure
    // Note: this returns fields in table schema order, which may differ from requested order
    Schema selected = tableSchema.select(allColumnNames);

    // Rebuild columns in requested schema order to maintain column ordering
    return rebuildSchemaInRequestedOrder(requestedSchema, selected, missingMetadataIds);
  }

  private static Schema rebuildSchemaInRequestedOrder(
      Schema requestedSchema, Schema selected, Set<Integer> missingMetadataIds) {
    List<Types.NestedField> columns = Lists.newArrayList();
    Set<Integer> addedFieldIds = Sets.newHashSet();

    // First, add all columns from requested schema (preserves order)
    for (Types.NestedField requestedField : requestedSchema.columns()) {
      // Metadata columns don't exist in tableSchema/selected, use the requested field directly
      if (MetadataColumns.isMetadataColumn(requestedField.fieldId())) {
        columns.add(requestedField);
        addedFieldIds.add(requestedField.fieldId());
      } else {
        Types.NestedField selectedField = selected.findField(requestedField.fieldId());
        Preconditions.checkArgument(
            selectedField != null,
            "Cannot find requested field %s in selected schema",
            requestedField.fieldId());
        columns.add(selectedField);
        addedFieldIds.add(selectedField.fieldId());
      }
    }

    // Then add any new top-level fields needed for nested equality delete columns
    for (Types.NestedField selectedField : selected.columns()) {
      if (!addedFieldIds.contains(selectedField.fieldId())) {
        columns.add(selectedField);
        addedFieldIds.add(selectedField.fieldId());
      }
    }

    // Finally, add metadata columns (they don't exist in tableSchema)
    for (int fieldId : missingMetadataIds) {
      if (!addedFieldIds.contains(fieldId)) {
        Types.NestedField metadataColumn = METADATA_COLUMNS_BY_ID.get(fieldId);
        Preconditions.checkArgument(
            metadataColumn != null, "Cannot find metadata column for ID %s", fieldId);
        columns.add(metadataColumn);
      }
    }

    return new Schema(columns);
  }
}
