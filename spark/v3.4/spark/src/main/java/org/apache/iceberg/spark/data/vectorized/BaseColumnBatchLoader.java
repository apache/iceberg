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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseColumnBatchLoader {
  protected final int numRowsToRead;
  // the rowId mapping to skip deleted rows for all column vectors inside a batch, it is null when
  // there is no deletes
  protected int[] rowIdMapping;
  // the array to indicate if a row is deleted or not, it is null when there is no "_deleted"
  // metadata column
  protected boolean[] isDeleted;
  private final boolean hasIsDeletedColumn;
  private final DeleteFilter<InternalRow> deletes;
  private final long rowStartPosInBatch;

  protected BaseColumnBatchLoader(
      int numRowsToRead,
      boolean hasIsDeletedColumn,
      DeleteFilter<InternalRow> deletes,
      long rowStartPosInBatch) {
    Preconditions.checkArgument(
        numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
    this.numRowsToRead = numRowsToRead;
    this.hasIsDeletedColumn = hasIsDeletedColumn;
    this.deletes = deletes;
    this.rowStartPosInBatch = rowStartPosInBatch;
    if (hasIsDeletedColumn) {
      isDeleted = new boolean[numRowsToRead];
    }
  }

  public ColumnarBatch loadDataToColumnBatch() {
    int numRowsUndeleted = initRowIdMapping();

    ColumnVector[] arrowColumnVectors = readDataToColumnVectors();

    ColumnarBatch newColumnarBatch = new ColumnarBatch(arrowColumnVectors);
    newColumnarBatch.setNumRows(numRowsUndeleted);

    if (hasEqDeletes()) {
      applyEqDelete(newColumnarBatch);
    }

    if (hasIsDeletedColumn && rowIdMapping != null) {
      // reset the row id mapping array, so that it doesn't filter out the deleted rows
      for (int i = 0; i < numRowsToRead; i++) {
        rowIdMapping[i] = i;
      }
      newColumnarBatch.setNumRows(numRowsToRead);
    }

    if (hasIsDeletedColumn) {
      readDeletedColumnIfNecessary(arrowColumnVectors);
    }

    return newColumnarBatch;
  }

  protected abstract ColumnVector[] readDataToColumnVectors();

  protected abstract void readDeletedColumnIfNecessary(ColumnVector[] arrowColumnVectors);

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
   * delete 2 rows in a batch with 8 rows in total. [0,1,2,3,4,5,6,7] -- Original status of the row
   * id mapping array [F,F,F,F,F,F,F,F] -- Original status of the isDeleted array Position delete 2,
   * 6 [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6] [F,F,T,F,F,F,T,F]
   * -- After applying position deletes
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
   * Filter out the equality deleted rows. Here is an example, [0,1,2,3,4,5,6,7] -- Original status
   * of the row id mapping array [F,F,F,F,F,F,F,F] -- Original status of the isDeleted array
   * Position delete 2, 6 [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to
   * 6] [F,F,T,F,F,F,T,F] -- After applying position deletes Equality delete 1 <= x <= 3
   * [0,4,5,7,-,-,-,-] -- After applying equality deletes [Set Num records to 4] [F,T,T,T,F,F,T,F]
   * -- After applying equality deletes
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
}
