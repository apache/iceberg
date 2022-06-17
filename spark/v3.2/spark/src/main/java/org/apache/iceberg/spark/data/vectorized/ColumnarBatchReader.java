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
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized read path. The
 * {@link ColumnarBatch} returned is created by passing in the Arrow vectors populated via delegated read calls to
 * {@linkplain VectorizedArrowReader VectorReader(s)}.
 */
public class ColumnarBatchReader extends BaseBatchReader<ColumnarBatch> {
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;
  private final boolean hasIsDeletedColumn;

  public ColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
    this.hasIsDeletedColumn = hasDeletedVectorReader(readers);
  }

  private boolean hasDeletedVectorReader(List<VectorizedReader<?>> readers) {
    for (VectorizedReader reader : readers) {
      if (reader instanceof VectorizedArrowReader.DeletedVectorReader) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void setRowGroupInfo(PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData,
                              long rowPosition) {
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

    ColumnBatchLoader batchLoader = new ColumnBatchLoader(numRowsToRead);
    rowStartPosInBatch += numRowsToRead;
    return batchLoader.columnarBatch;
  }

  private class ColumnBatchLoader {
    private ColumnarBatch columnarBatch;
    private final int numRowsToRead;
    // the rowId mapping to skip deleted rows for all column vectors inside a batch, it is null when there is no deletes
    private int[] rowIdMapping;
    // the array to indicate if a row is deleted or not, it is null when there is no "_deleted" metadata column
    private boolean[] isDeleted;

    ColumnBatchLoader(int numRowsToRead) {
      Preconditions.checkArgument(numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
      this.numRowsToRead = numRowsToRead;
      this.columnarBatch = loadDataToColumnBatch();
    }

    ColumnarBatch loadDataToColumnBatch() {
      int numRowsUndeleted = initRowIdMapping();

      ColumnVector[] arrowColumnVectors = readDataToColumnVectors();

      ColumnarBatch newColumnarBatch = new ColumnarBatch(arrowColumnVectors);
      newColumnarBatch.setNumRows(numRowsUndeleted);

      if (hasEqDeletes()) {
        numRowsUndeleted = applyEqDelete(newColumnarBatch);
      }

      if (hasIsDeletedColumn) {
        rowIdMappingToIsDeleted(numRowsUndeleted);
        newColumnarBatch.setNumRows(numRowsToRead);
      }

      return newColumnarBatch;
    }

    ColumnVector[] readDataToColumnVectors() {
      ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

      if (hasIsDeletedColumn) {
        isDeleted = new boolean[numRowsToRead];
      }

      for (int i = 0; i < readers.length; i += 1) {
        vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
        int numRowsInVector = vectorHolders[i].numValues();
        Preconditions.checkState(
            numRowsInVector == numRowsToRead,
            "Number of rows in the vector %s didn't match expected %s ", numRowsInVector,
            numRowsToRead);

        arrowColumnVectors[i] = new ColumnVectorBuilder(vectorHolders[i], numRowsInVector)
            .withDeletedRows(rowIdMapping, isDeleted)
            .build();
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
     * Build a row id mapping inside a batch, which skips deleted rows. Here is an example of how we delete 2 rows in a
     * batch with 8 rows in total.
     * [0,1,2,3,4,5,6,7] -- Original status of the row id mapping array
     * Position delete 2, 6
     * [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
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
     * Filter out the equality deleted rows. Here is an example,
     * [0,1,2,3,4,5,6,7] -- Original status of the row id mapping array
     * Position delete 2, 6
     * [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
     * Equality delete 1 <= x <= 3
     * [0,4,5,7,-,-,-,-] -- After applying equality deletes [Set Num records to 4]
     *
     * @return the number of undeleted rows in a batch after applying equality deletes
     */
    int applyEqDelete(ColumnarBatch newColumnarBatch) {
      Iterator<InternalRow> it = newColumnarBatch.rowIterator();
      int rowId = 0;
      int currentRowId = 0;
      while (it.hasNext()) {
        InternalRow row = it.next();
        if (deletes.eqDeletedRowFilter().test(row)) {
          // the row is NOT deleted
          // skip deleted rows by pointing to the next undeleted row Id
          rowIdMapping[currentRowId] = rowIdMapping[rowId];
          currentRowId++;
        }

        rowId++;
      }

      newColumnarBatch.setNumRows(currentRowId);
      return currentRowId;
    }

    /**
     * Convert the row id mapping array to the isDeleted array.
     *
     * @param numRowsInRowIdMapping the num of rows in the row id mapping array
     */
    void rowIdMappingToIsDeleted(int numRowsInRowIdMapping) {
      // isDeleted array is filled with "false" by default. We keep that to indicate no row is deleted
      // when there is no deletes(rowIdMapping == null).
      if (isDeleted == null || rowIdMapping == null) {
        return;
      }

      Arrays.fill(isDeleted, true);

      for (int i = 0; i < numRowsInRowIdMapping; i++) {
        isDeleted[rowIdMapping[i]] = false;
      }

      // reset the row id mapping array, so that it doesn't filter out the deleted rows
      for (int i = 0; i < numRowsToRead; i++) {
        rowIdMapping[i] = i;
      }
    }
  }
}
