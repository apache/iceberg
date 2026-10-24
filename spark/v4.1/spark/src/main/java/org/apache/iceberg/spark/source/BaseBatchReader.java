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
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.VortexBatchReadConf;
import org.apache.iceberg.spark.data.vectorized.ColumnVectorWithFilter;
import org.apache.iceberg.spark.data.vectorized.ColumnarBatchUtil;
import org.apache.iceberg.spark.data.vectorized.DeletedColumnVector;
import org.apache.iceberg.spark.data.vectorized.UpdatableDeletedColumnVector;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

abstract class BaseBatchReader<T extends ScanTask> extends BaseReader<ColumnarBatch, T> {
  private final ParquetBatchReadConf parquetConf;
  private final OrcBatchReadConf orcConf;
  private final VortexBatchReadConf vortexConf;

  BaseBatchReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<T> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive,
      ParquetBatchReadConf parquetConf,
      OrcBatchReadConf orcConf,
      VortexBatchReadConf vortexConf,
      boolean cacheDeleteFilesOnExecutors) {
    super(table, fileIO, taskGroup, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);
    this.parquetConf = parquetConf;
    this.orcConf = orcConf;
    this.vortexConf = vortexConf;
  }

  protected CloseableIterable<ColumnarBatch> newBatchIterable(
      InputFile inputFile,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      @Nonnull SparkDeleteFilter deleteFilter) {
    ReadBuilder<ColumnarBatch, ?> readBuilder =
        FormatModelRegistry.readBuilder(format, ColumnarBatch.class, inputFile);

    if (parquetConf != null) {
      readBuilder = readBuilder.recordsPerBatch(parquetConf.batchSize());
    } else if (orcConf != null) {
      readBuilder = readBuilder.recordsPerBatch(orcConf.batchSize());
    } else if (vortexConf != null) {
      readBuilder = readBuilder.recordsPerBatch(vortexConf.batchSize());
    }

    if (readBuilder.supportsPositionDeletes()) {
      // Vortex applies deletes (and residual filters) natively in the scan, so the post-scan
      // BatchDeleteFilter (which derives positions from a contiguous _pos column and is unsound
      // once
      // rows are filtered out during the scan) is bypassed. Deleted rows are normally dropped via a
      // pushed position bitmap; but when the _deleted metadata column is projected they must
      // instead
      // be retained and flagged, so nothing is pushed and rows are marked from their _pos.
      boolean markDeletes =
          deleteFilter.requiredSchema().findField(MetadataColumns.IS_DELETED.fieldId()) != null
              && (deleteFilter.hasPosDeletes() || deleteFilter.hasEqDeletes());
      if (markDeletes) {
        return newDeleteMarkingBatchIterable(
            inputFile, readBuilder, start, length, residual, idToConstant, deleteFilter);
      }
      return newPushdownBatchIterable(
          inputFile, readBuilder, start, length, residual, idToConstant, deleteFilter);
    }

    CloseableIterable<ColumnarBatch> iterable =
        readBuilder
            .project(deleteFilter.requiredSchema())
            .idToConstant(idToConstant)
            .split(start, length)
            .filter(residual)
            .caseSensitive(caseSensitive())
            // Spark eagerly consumes the batches. So the underlying memory allocated could be
            // reused without worrying about subsequent reads clobbering over each other. This
            // improves read performance as every batch read doesn't have to pay the cost of
            // allocating memory.
            .reuseContainers()
            .withNameMapping(nameMapping())
            .build();

    return CloseableIterable.transform(iterable, new BatchDeleteFilter(deleteFilter)::filterBatch);
  }

  // Drops deleted rows by turning every delete into file-relative positions and pushing them into
  // the scan so they are never materialized: position deletes directly, and equality deletes via a
  // pre-scan that resolves the matching rows to positions. Only the expected columns are projected.
  private CloseableIterable<ColumnarBatch> newPushdownBatchIterable(
      InputFile inputFile,
      ReadBuilder<ColumnarBatch, ?> readBuilder,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    PositionDeleteIndex deletePositions =
        pushableDeletePositions(inputFile, start, length, residual, idToConstant, deleteFilter);
    if (deletePositions.isNotEmpty()) {
      readBuilder.positionDeletes(deletePositions);
    }

    return readBuilder
        .project(deleteFilter.expectedSchema())
        .idToConstant(idToConstant)
        .split(start, length)
        .filter(residual)
        .caseSensitive(caseSensitive())
        .reuseContainers()
        .withNameMapping(nameMapping())
        .build();
  }

  // Retains all rows and flags deleted ones in the _deleted column instead of dropping them (for
  // CDC-style reads that project _deleted). Nothing is pushed into the scan; rows are marked using
  // their actual _pos, which stays correct even when a residual filter makes positions
  // non-contiguous within a batch.
  private CloseableIterable<ColumnarBatch> newDeleteMarkingBatchIterable(
      InputFile inputFile,
      ReadBuilder<ColumnarBatch, ?> readBuilder,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    PositionDeleteIndex deletePositions =
        pushableDeletePositions(inputFile, start, length, residual, idToConstant, deleteFilter);

    Schema expectedSchema = deleteFilter.expectedSchema();
    Schema scanSchema = expectedSchema;
    if (expectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()) == null) {
      // _pos is needed to look up the delete state of each row; append it when not already
      // projected
      // and trim it back off the emitted batch.
      List<Types.NestedField> columns = Lists.newArrayList(expectedSchema.columns());
      columns.add(MetadataColumns.ROW_POSITION);
      scanSchema = new Schema(columns);
    }
    int rowPositionIndex = scanSchema.columns().indexOf(MetadataColumns.ROW_POSITION);
    int isDeletedIndex = scanSchema.columns().indexOf(MetadataColumns.IS_DELETED);
    int outputColumnCount = expectedSchema.columns().size();

    CloseableIterable<ColumnarBatch> iterable =
        readBuilder
            .project(scanSchema)
            .idToConstant(idToConstant)
            .split(start, length)
            .filter(residual)
            .caseSensitive(caseSensitive())
            .reuseContainers()
            .withNameMapping(nameMapping())
            .build();

    return CloseableIterable.transform(
        iterable,
        batch ->
            markDeletedRows(
                batch, deletePositions, rowPositionIndex, isDeletedIndex, outputColumnCount));
  }

  private static ColumnarBatch markDeletedRows(
      ColumnarBatch batch,
      PositionDeleteIndex deletePositions,
      int rowPositionIndex,
      int isDeletedIndex,
      int outputColumnCount) {
    int numRows = batch.numRows();
    ColumnVector rowPositions = batch.column(rowPositionIndex);
    boolean[] isDeleted = new boolean[numRows];
    for (int row = 0; row < numRows; row++) {
      isDeleted[row] = deletePositions.isDeleted(rowPositions.getLong(row));
    }

    DeletedColumnVector deletedColumn = new DeletedColumnVector(Types.BooleanType.get());
    deletedColumn.setValue(isDeleted);

    // Emit only the expected columns (_pos is dropped when it was appended just for marking) with
    // the constant _deleted column replaced by the computed flags.
    ColumnVector[] vectors = new ColumnVector[outputColumnCount];
    for (int i = 0; i < outputColumnCount; i++) {
      vectors[i] = i == isDeletedIndex ? deletedColumn : batch.column(i);
    }

    ColumnarBatch output = new ColumnarBatch(vectors);
    output.setNumRows(numRows);
    return output;
  }

  // Collects all rows to drop as file-relative positions: the position deletes for the file, plus
  // the positions of rows matching equality deletes (resolved by a row-oriented pre-scan).
  private PositionDeleteIndex pushableDeletePositions(
      InputFile inputFile,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    PositionDeleteIndex positions = Deletes.toPositionIndex(CloseableIterable.empty());

    PositionDeleteIndex posDeletes = deleteFilter.deletedRowPositions();
    if (posDeletes != null) {
      posDeletes.forEach(positions::delete);
    }

    if (deleteFilter.hasEqDeletes()) {
      addEqualityDeletePositions(
          positions, inputFile, start, length, residual, idToConstant, deleteFilter);
    }

    return positions;
  }

  // Pre-scans the data file as rows projecting the equality-delete columns and _pos, evaluates the
  // equality-delete predicate, and records the file position of every matching row. This lets
  // equality deletes ride the same native position pushdown as position deletes.
  private void addEqualityDeletePositions(
      PositionDeleteIndex positions,
      InputFile inputFile,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    Schema requiredSchema = deleteFilter.requiredSchema();
    Schema scanSchema = requiredSchema;
    if (requiredSchema.findField(MetadataColumns.ROW_POSITION.fieldId()) == null) {
      // The equality-delete predicate binds against requiredSchema; appending _pos at the end keeps
      // those columns aligned while exposing the position for each matching row.
      List<Types.NestedField> columns = Lists.newArrayList(requiredSchema.columns());
      columns.add(MetadataColumns.ROW_POSITION);
      scanSchema = new Schema(columns);
    }
    int rowPositionIndex = scanSchema.columns().indexOf(MetadataColumns.ROW_POSITION);

    ReadBuilder<InternalRow, ?> rowReadBuilder =
        FormatModelRegistry.readBuilder(FileFormat.VORTEX, InternalRow.class, inputFile);
    try (CloseableIterable<InternalRow> rows =
            rowReadBuilder
                .project(scanSchema)
                .idToConstant(idToConstant)
                .split(start, length)
                .filter(residual)
                .caseSensitive(caseSensitive())
                .withNameMapping(nameMapping())
                .build();
        CloseableIterable<InternalRow> equalityDeleted =
            deleteFilter.findEqualityDeleteRows(rows)) {
      for (InternalRow row : equalityDeleted) {
        positions.delete(row.getLong(rowPositionIndex));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to resolve equality-delete positions for Vortex", e);
    }
  }

  @VisibleForTesting
  static class BatchDeleteFilter {
    private final DeleteFilter<InternalRow> deletes;
    private boolean hasIsDeletedColumn;
    private int rowPositionColumnIndex = -1;

    BatchDeleteFilter(DeleteFilter<InternalRow> deletes) {
      this.deletes = deletes;

      Schema schema = deletes.requiredSchema();
      for (int i = 0; i < schema.columns().size(); i++) {
        if (schema.columns().get(i).fieldId() == MetadataColumns.ROW_POSITION.fieldId()) {
          this.rowPositionColumnIndex = i;
        } else if (schema.columns().get(i).fieldId() == MetadataColumns.IS_DELETED.fieldId()) {
          this.hasIsDeletedColumn = true;
        }
      }
    }

    ColumnarBatch filterBatch(ColumnarBatch batch) {
      if (!needDeletes()) {
        return batch;
      }

      ColumnVector[] vectors = new ColumnVector[batch.numCols()];
      for (int i = 0; i < batch.numCols(); i++) {
        vectors[i] = batch.column(i);
      }

      int numLiveRows = batch.numRows();
      long rowStartPosInBatch =
          rowPositionColumnIndex == -1 ? -1 : vectors[rowPositionColumnIndex].getLong(0);

      if (hasIsDeletedColumn) {
        boolean[] isDeleted =
            ColumnarBatchUtil.buildIsDeleted(vectors, deletes, rowStartPosInBatch, numLiveRows);
        for (ColumnVector vector : vectors) {
          if (vector instanceof UpdatableDeletedColumnVector) {
            ((UpdatableDeletedColumnVector) vector).setValue(isDeleted);
          }
        }
      } else {
        Pair<int[], Integer> pair =
            ColumnarBatchUtil.buildRowIdMapping(vectors, deletes, rowStartPosInBatch, numLiveRows);
        if (pair != null) {
          int[] rowIdMapping = pair.first();
          numLiveRows = pair.second();
          for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new ColumnVectorWithFilter(vectors[i], rowIdMapping);
          }
        }
      }

      if (deletes != null && deletes.hasEqDeletes()) {
        vectors = ColumnarBatchUtil.removeExtraColumns(deletes, vectors);
      }

      ColumnarBatch output = new ColumnarBatch(vectors);
      output.setNumRows(numLiveRows);
      return output;
    }

    private boolean needDeletes() {
      return hasIsDeletedColumn
          || (deletes != null && (deletes.hasEqDeletes() || deletes.hasPosDeletes()));
    }
  }
}
