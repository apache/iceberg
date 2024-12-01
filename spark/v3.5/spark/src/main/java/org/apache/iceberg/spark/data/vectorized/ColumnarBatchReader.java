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
package org.apache.iceberg.spark.data.vectorized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.arrow.vectorized.BaseBatchReader;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.DeletedVectorReader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized
 * read path. The {@link ColumnarBatch} returned is created by passing in the Arrow vectors
 * populated via delegated read calls to {@linkplain VectorizedArrowReader VectorReader(s)}.
 */
public class ColumnarBatchReader extends BaseBatchReader<ColumnarBatch> {
  private final boolean hasIsDeletedColumn;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;

  public ColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof DeletedVectorReader);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    super.setRowGroupInfo(pageStore, metaData, rowPosition);
    this.rowStartPosInBatch = rowPosition;
  }

  public void setDeleteFilter(DeleteFilter<InternalRow> deleteFilter) {
    this.deletes = deleteFilter;
  }

  @Override
  public final ColumnarBatch read(ColumnarBatch reuse, int numRowsToRead) {
    if (reuse == null) {
      closeVectors();
    }

    ColumnarBatch columnarBatch = new ColumnBatchLoader(numRowsToRead).loadDataToColumnBatch();
    rowStartPosInBatch += numRowsToRead;
    return columnarBatch;
  }

  /**
   * Calculates the number of extra columns that are necessary for processing equality delete
   * filters but are not part of the final output. For instance, if the table schema includes C1,
   * C2, C3, C4, C5 and the query is 'SELECT C5 FROM table', and there are equality delete filters
   * on C3 and C4, the required schema for processing would be C5, C3, C4. These extra columns (C3,
   * C4) are needed to determine the rows to delete based on the filter criteria. After identifying
   * the deletable rows, these extra column values are no longer needed to be returned to Spark.
   *
   * @param deleteFilter The delete filter applied for equality delete.
   * @return the number of extra columns that are read but not included in the final query result.
   */
  private int numOfExtraColumns(DeleteFilter<InternalRow> deleteFilter) {
    if (deleteFilter != null && deleteFilter.hasEqDeletes()) {
      List<Types.NestedField> requiredColumns = deleteFilter.requiredSchema().columns();
      List<Types.NestedField> expectedColumns = deleteFilter.expectedSchema().columns();
      return requiredColumns.size() - expectedColumns.size();
    }

    return 0;
  }

  private class ColumnBatchLoader {
    private final int numRowsToRead;
    // the rowId mapping to skip deleted rows for all column vectors inside a batch, it is null when
    // there is no deletes
    private int[] rowIdMapping;
    // the array to indicate if a row is deleted or not, it is null when there is no "_deleted"
    // metadata column
    private boolean[] isDeleted;

    ColumnBatchLoader(int numRowsToRead) {
      Preconditions.checkArgument(
          numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
      this.numRowsToRead = numRowsToRead;
      if (hasIsDeletedColumn) {
        isDeleted = new boolean[numRowsToRead];
      }
    }

    ColumnarBatch loadDataToColumnBatch() {
      int numRowsUndeleted = initRowIdMapping();

      ColumnVector[] arrowColumnVectors = readDataToColumnVectors();

      ColumnarBatch newColumnarBatch = new ColumnarBatch(arrowColumnVectors);
      newColumnarBatch.setNumRows(numRowsUndeleted);

      if (hasEqDeletes()) {
        applyEqDelete(newColumnarBatch);
        newColumnarBatch = removeExtraColumns(arrowColumnVectors, newColumnarBatch);
      }

      if (hasIsDeletedColumn && rowIdMapping != null) {
        // reset the row id mapping array, so that it doesn't filter out the deleted rows
        for (int i = 0; i < numRowsToRead; i++) {
          rowIdMapping[i] = i;
        }
        newColumnarBatch.setNumRows(numRowsToRead);
      }

      return newColumnarBatch;
    }

    ColumnVector[] readDataToColumnVectors() {
      ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

      ColumnVectorBuilder columnVectorBuilder = new ColumnVectorBuilder();
      for (int i = 0; i < readers.length; i += 1) {
        vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
        int numRowsInVector = vectorHolders[i].numValues();
        Preconditions.checkState(
            numRowsInVector == numRowsToRead,
            "Number of rows in the vector %s didn't match expected %s ",
            numRowsInVector,
            numRowsToRead);

        arrowColumnVectors[i] =
            columnVectorBuilder
                .withDeletedRows(rowIdMapping, isDeleted)
                .build(vectorHolders[i], numRowsInVector);
      }
      return arrowColumnVectors;
    }

    boolean hasEqDeletes() {
      return deletes != null && deletes.hasEqDeletes();
    }

    int initRowIdMapping() {
      Pair<int[], Integer> posDeleteRowIdMapping = posDelRowIdMapping();
      if (posDeleteRowIdMapping != null) {
        rowIdMapping = posDeleteRowIdMapping.first();
        return posDeleteRowIdMapping.second();
      } else {
        rowIdMapping = initEqDeleteRowIdMapping();
        return numRowsToRead;
      }
    }

    Pair<int[], Integer> posDelRowIdMapping() {
      if (deletes != null && deletes.hasPosDeletes()) {
        return buildPosDelRowIdMapping(deletes.deletedRowPositions());
      } else {
        return null;
      }
    }

    /**
     * Build a row id mapping inside a batch, which skips deleted rows. Here is an example of how we
     * delete 2 rows in a batch with 8 rows in total. [0,1,2,3,4,5,6,7] -- Original status of the
     * row id mapping array [F,F,F,F,F,F,F,F] -- Original status of the isDeleted array Position
     * delete 2, 6 [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
     * [F,F,T,F,F,F,T,F] -- After applying position deletes
     *
     * @param deletedRowPositions a set of deleted row positions
     * @return the mapping array and the new num of rows in a batch, null if no row is deleted
     */
    Pair<int[], Integer> buildPosDelRowIdMapping(PositionDeleteIndex deletedRowPositions) {
      if (deletedRowPositions == null) {
        return null;
      }

      int[] posDelRowIdMapping = new int[numRowsToRead];
      int originalRowId = 0;
      int currentRowId = 0;
      while (originalRowId < numRowsToRead) {
        if (!deletedRowPositions.isDeleted(originalRowId + rowStartPosInBatch)) {
          posDelRowIdMapping[currentRowId] = originalRowId;
          currentRowId++;
        } else {
          if (hasIsDeletedColumn) {
            isDeleted[originalRowId] = true;
          }

          deletes.incrementDeleteCount();
        }
        originalRowId++;
      }

      if (currentRowId == numRowsToRead) {
        // there is no delete in this batch
        return null;
      } else {
        return Pair.of(posDelRowIdMapping, currentRowId);
      }
    }

    int[] initEqDeleteRowIdMapping() {
      int[] eqDeleteRowIdMapping = null;
      if (hasEqDeletes()) {
        eqDeleteRowIdMapping = new int[numRowsToRead];
        for (int i = 0; i < numRowsToRead; i++) {
          eqDeleteRowIdMapping[i] = i;
        }
      }

      return eqDeleteRowIdMapping;
    }

    /**
     * Filter out the equality deleted rows. Here is an example, [0,1,2,3,4,5,6,7] -- Original
     * status of the row id mapping array [F,F,F,F,F,F,F,F] -- Original status of the isDeleted
     * array Position delete 2, 6 [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num
     * records to 6] [F,F,T,F,F,F,T,F] -- After applying position deletes Equality delete 1 <= x <=
     * 3 [0,4,5,7,-,-,-,-] -- After applying equality deletes [Set Num records to 4]
     * [F,T,T,T,F,F,T,F] -- After applying equality deletes
     *
     * @param columnarBatch the {@link ColumnarBatch} to apply the equality delete
     */
    void applyEqDelete(ColumnarBatch columnarBatch) {
      Iterator<InternalRow> it = columnarBatch.rowIterator();
      int rowId = 0;
      int currentRowId = 0;
      while (it.hasNext()) {
        InternalRow row = it.next();
        if (deletes.eqDeletedRowFilter().test(row)) {
          // the row is NOT deleted
          // skip deleted rows by pointing to the next undeleted row Id
          rowIdMapping[currentRowId] = rowIdMapping[rowId];
          currentRowId++;
        } else {
          if (hasIsDeletedColumn) {
            isDeleted[rowIdMapping[rowId]] = true;
          }

          deletes.incrementDeleteCount();
        }

        rowId++;
      }

      columnarBatch.setNumRows(currentRowId);
    }

    ColumnarBatch removeExtraColumns(
        ColumnVector[] arrowColumnVectors, ColumnarBatch columnarBatch) {
      int numOfExtraColumns = numOfExtraColumns(deletes);
      if (numOfExtraColumns > 0) {
        int newLength = arrowColumnVectors.length - numOfExtraColumns;
        // In DeleteFilter.fileProjection, the columns for missingIds (the columns required
        // for equality delete or ROW_POSITION) are appended to the end of the expectedSchema.
        // Therefore, these extra columns can be removed from the end of arrowColumnVectors.
        ColumnVector[] newColumns = Arrays.copyOf(arrowColumnVectors, newLength);
        return new ColumnarBatch(newColumns, columnarBatch.numRows());
      } else {
        return columnarBatch;
      }
    }
  }
}
