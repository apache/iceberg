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
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class ColumnBatchWithRowMapping {
  private final DeleteFilter<InternalRow> deletes;
  private int[] rowIdMapping; // the rowId mapping to skip deleted rows for all column vectors inside a batch
  private int numRows;
  private ColumnarBatch batch;

  ColumnBatchWithRowMapping(DeleteFilter<InternalRow> deletes, int numRowsToRead, long rowStartPosInBatch) {
    this.deletes = deletes;
    initRowIdMapping(numRowsToRead, rowStartPosInBatch);
  }

  ColumnarBatch createColumnBatch(ColumnVector[] columns) {
    batch = new ColumnarBatch(columns);
    batch.setNumRows(numRows);

    if (hasEqDeletes()) {
      applyEqDelete();
    }
    return batch;
  }

  boolean hasDeletes() {
    return rowIdMapping != null;
  }

  private boolean hasEqDeletes() {
    return deletes != null && deletes.hasEqDeletes();
  }

  int[] rowIdMapping() {
    return rowIdMapping;
  }

  int numRows() {
    return numRows;
  }

  private void initRowIdMapping(int numRowsToRead, long rowStartPosInBatch) {
    Pair<int[], Integer> posDeleteRowIdMapping = posDelRowIdMapping(numRowsToRead, rowStartPosInBatch);
    if (posDeleteRowIdMapping != null) {
      rowIdMapping = posDeleteRowIdMapping.first();
      numRows = posDeleteRowIdMapping.second();
    } else {
      numRows = numRowsToRead;
      rowIdMapping = initEqDeleteRowIdMapping(numRowsToRead);
    }
  }

  private Pair<int[], Integer> posDelRowIdMapping(int numRowsToRead, long rowStartPosInBatch) {
    if (deletes != null && deletes.hasPosDeletes()) {
      return buildPosDelRowIdMapping(deletes.deletedRowPositions(), numRowsToRead, rowStartPosInBatch);
    } else {
      return null;
    }
  }

  /**
   * Build a row id mapping inside a batch, which skips delete rows. For example, if the 1st and 3rd rows are deleted in
   * a batch with 5 rows, the mapping would be {0->1, 1->3, 2->4}, and the new num of rows is 3.
   *
   * @param deletedRowPositions a set of deleted row positions
   * @param numRowsToRead             the num of rows
   * @return the mapping array and the new num of rows in a batch, null if no row is deleted
   */
  private Pair<int[], Integer> buildPosDelRowIdMapping(PositionDeleteIndex deletedRowPositions, int numRowsToRead,
                                                       long rowStartPosInBatch) {
    if (deletedRowPositions == null) {
      return null;
    }

    int[] posDelRowIdMapping = new int[numRowsToRead];
    int originalRowId = 0;
    int currentRowId = 0;
    while (originalRowId < numRowsToRead) {
      if (!deletedRowPositions.deleted(originalRowId + rowStartPosInBatch)) {
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

  private int[] initEqDeleteRowIdMapping(int numRowsToRead) {
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
   * [0,1,2,3,4,5,6,7] -- Original
   * POS Delete 2, 6
   * [0,1,3,4,5,7,-,-] -- After Apply Pos Deletes [Set Num records to 6]
   * Equality delete 1 <= x <= 3
   * [0,4,5,7,-,-,-,-] -- After Apply Eq Deletes [Set Num records to 4]
   */
  private void applyEqDelete() {
    Iterator<InternalRow> it = batch.rowIterator();
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

    batch.setNumRows(currentRowId);
  }
}
