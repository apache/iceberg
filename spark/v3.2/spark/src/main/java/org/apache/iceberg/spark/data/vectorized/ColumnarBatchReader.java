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
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized
 * read path. The {@link ColumnarBatch} returned is created by passing in the Arrow vectors
 * populated via delegated read calls to {@linkplain VectorizedArrowReader VectorReader(s)}.
 */
public class ColumnarBatchReader extends BaseBatchReader<ColumnarBatch> {
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;

  public ColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
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

    ColumnBatchLoader batchLoader = new ColumnBatchLoader(numRowsToRead);
    rowStartPosInBatch += numRowsToRead;
    return batchLoader.columnarBatch;
  }

  private class ColumnBatchLoader {
    private int[]
        rowIdMapping; // the rowId mapping to skip deleted rows for all column vectors inside a
    // batch
    private int numRows;
    private ColumnarBatch columnarBatch;

    ColumnBatchLoader(int numRowsToRead) {
      initRowIdMapping(numRowsToRead);
      loadDataToColumnBatch(numRowsToRead);
    }

    ColumnarBatch loadDataToColumnBatch(int numRowsToRead) {
      Preconditions.checkArgument(
          numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
      ColumnVector[] arrowColumnVectors = readDataToColumnVectors(numRowsToRead);

      columnarBatch = new ColumnarBatch(arrowColumnVectors);
      columnarBatch.setNumRows(numRows);

      if (hasEqDeletes()) {
        applyEqDelete();
      }
      return columnarBatch;
    }

    ColumnVector[] readDataToColumnVectors(int numRowsToRead) {
      ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

      for (int i = 0; i < readers.length; i += 1) {
        vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
        int numRowsInVector = vectorHolders[i].numValues();
        Preconditions.checkState(
            numRowsInVector == numRowsToRead,
            "Number of rows in the vector %s didn't match expected %s ",
            numRowsInVector,
            numRowsToRead);

        arrowColumnVectors[i] =
            hasDeletes()
                ? ColumnVectorWithFilter.forHolder(vectorHolders[i], rowIdMapping, numRows)
                : IcebergArrowColumnVector.forHolder(vectorHolders[i], numRowsInVector);
      }
      return arrowColumnVectors;
    }

    boolean hasDeletes() {
      return rowIdMapping != null;
    }

    boolean hasEqDeletes() {
      return deletes != null && deletes.hasEqDeletes();
    }

    void initRowIdMapping(int numRowsToRead) {
      Pair<int[], Integer> posDeleteRowIdMapping = posDelRowIdMapping(numRowsToRead);
      if (posDeleteRowIdMapping != null) {
        rowIdMapping = posDeleteRowIdMapping.first();
        numRows = posDeleteRowIdMapping.second();
      } else {
        numRows = numRowsToRead;
        rowIdMapping = initEqDeleteRowIdMapping(numRowsToRead);
      }
    }

    Pair<int[], Integer> posDelRowIdMapping(int numRowsToRead) {
      if (deletes != null && deletes.hasPosDeletes()) {
        return buildPosDelRowIdMapping(deletes.deletedRowPositions(), numRowsToRead);
      } else {
        return null;
      }
    }

    /**
     * Build a row id mapping inside a batch, which skips deleted rows. Here is an example of how we
     * delete 2 rows in a batch with 8 rows in total. [0,1,2,3,4,5,6,7] -- Original status of the
     * row id mapping array Position delete 2, 6 [0,1,3,4,5,7,-,-] -- After applying position
     * deletes [Set Num records to 6]
     *
     * @param deletedRowPositions a set of deleted row positions
     * @param numRowsToRead the num of rows
     * @return the mapping array and the new num of rows in a batch, null if no row is deleted
     */
    Pair<int[], Integer> buildPosDelRowIdMapping(
        PositionDeleteIndex deletedRowPositions, int numRowsToRead) {
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

    int[] initEqDeleteRowIdMapping(int numRowsToRead) {
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
     * status of the row id mapping array Position delete 2, 6 [0,1,3,4,5,7,-,-] -- After applying
     * position deletes [Set Num records to 6] Equality delete 1 <= x <= 3 [0,4,5,7,-,-,-,-] --
     * After applying equality deletes [Set Num records to 4]
     */
    void applyEqDelete() {
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
        }

        rowId++;
      }

      columnarBatch.setNumRows(currentRowId);
    }
  }
}
