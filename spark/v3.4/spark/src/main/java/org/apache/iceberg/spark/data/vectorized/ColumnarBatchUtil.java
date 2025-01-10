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

public class ColumnarBatchUtil {

  private ColumnarBatchUtil() {}

  /**
   * Applies deletes to a given {@link ColumnarBatch}. This method calculates rowIdMapping based on
   * deletes and then update the rowIdMapping on each of the ColumnVector in the ColumnarBatch.
   *
   * @param columnarBatch The columnar batch to which the deletes are to be applied.
   * @param deletes The delete filter containing delete information.
   * @param isDeleted An array indicating whether the row in the batch is deleted.
   * @param numRowsToRead The number of rows to be read and processed from the columnar batch.
   * @param rowStartPosInBatch The starting position of the row in the batch.
   * @param hasIsDeletedColumn Indicates whether the columnar batch includes _deleted column.
   */
  public static void applyDeletesToColumnarBatch(
      ColumnarBatch columnarBatch,
      DeleteFilter<InternalRow> deletes,
      boolean[] isDeleted,
      int numRowsToRead,
      long rowStartPosInBatch,
      boolean hasIsDeletedColumn) {
    Pair<int[], Integer> pair =
        initRowIdMapping(deletes, numRowsToRead, rowStartPosInBatch, isDeleted, hasIsDeletedColumn);
    setRowIdMappingOnColumnarBatch(columnarBatch, pair.first());
    columnarBatch.setNumRows(pair.second());
    if (hasEqDeletes(deletes)) {
      applyEqDelete(columnarBatch, deletes, pair.first(), isDeleted, hasIsDeletedColumn);
    }
  }

  /**
   * This method resets the row id mapping array, so that it doesn't filter out the deleted rows
   *
   * @param columnarBatch The columnar batch to which the deletes are to be applied.
   * @param numRowsToRead The number of rows to be read and processed from the columnar batch.
   */
  public static void resetRowIdMapping(ColumnarBatch columnarBatch, int numRowsToRead) {
    int[] rowIdMapping = new int[numRowsToRead];
    for (int i = 0; i < numRowsToRead; i++) {
      rowIdMapping[i] = i;
    }

    setRowIdMappingOnColumnarBatch(columnarBatch, rowIdMapping);
    columnarBatch.setNumRows(numRowsToRead);
  }

  private static boolean hasEqDeletes(DeleteFilter<InternalRow> deletes) {
    return deletes != null && deletes.hasEqDeletes();
  }

  private static Pair<int[], Integer> initRowIdMapping(
      DeleteFilter<InternalRow> deletes,
      int numRowsToRead,
      long rowStartPosInBatch,
      boolean[] isDeleted,
      boolean hasIsDeletedColumn) {
    Pair<int[], Integer> posDeleteRowIdMapping =
        posDelRowIdMapping(
            deletes, numRowsToRead, rowStartPosInBatch, isDeleted, hasIsDeletedColumn);
    if (posDeleteRowIdMapping != null) {
      return posDeleteRowIdMapping;
    } else {
      int[] rowIdMapping = initEqDeleteRowIdMapping(numRowsToRead);
      return Pair.of(rowIdMapping, numRowsToRead);
    }
  }

  static Pair<int[], Integer> posDelRowIdMapping(
      DeleteFilter<InternalRow> deletes,
      int numRowsToRead,
      long rowStartPosInBatch,
      boolean[] isDeleted,
      boolean hasIsDeletedColumn) {
    if (deletes != null && deletes.hasPosDeletes()) {
      return buildPosDelRowIdMapping(
          deletes, numRowsToRead, rowStartPosInBatch, isDeleted, hasIsDeletedColumn);
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
   * @param deletes The delete filter containing delete information.
   * @param isDeleted An array indicating whether the row in the batch is deleted.
   * @param numRowsToRead The number of rows to be read and processed from the columnar batch.
   * @param rowStartPosInBatch The starting position of the row in the batch.
   * @param hasIsDeletedColumn Indicates whether the columnar batch includes _deleted column.
   * @return the mapping array and the new num of rows in a batch, null if no row is deleted
   */
  private static Pair<int[], Integer> buildPosDelRowIdMapping(
      DeleteFilter<InternalRow> deletes,
      int numRowsToRead,
      long rowStartPosInBatch,
      boolean[] isDeleted,
      boolean hasIsDeletedColumn) {
    PositionDeleteIndex deletedRowPositions = deletes.deletedRowPositions();
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

  private static int[] initEqDeleteRowIdMapping(int numRowsToRead) {
    int[] eqDeleteRowIdMapping = new int[numRowsToRead];
    for (int i = 0; i < numRowsToRead; i++) {
      eqDeleteRowIdMapping[i] = i;
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
   * @param deletes The delete filter containing delete information.
   * @param rowIdMapping the rowId mapping to skip deleted rows for all column vectors inside a
   *     batch
   * @param isDeleted An array indicating whether the row in the batch is deleted.
   * @param hasIsDeletedColumn Indicates whether the columnar batch includes _deleted column.
   */
  private static void applyEqDelete(
      ColumnarBatch columnarBatch,
      DeleteFilter<InternalRow> deletes,
      int[] rowIdMapping,
      boolean[] isDeleted,
      boolean hasIsDeletedColumn) {
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

  private static void setRowIdMappingOnColumnarBatch(
      ColumnarBatch columnarBatch, int[] rowIdMapping) {
    for (int i = 0; i < columnarBatch.numCols(); i++) {
      ColumnVector vector = columnarBatch.column(i);
      if (vector instanceof ColumnVectorWithFilter) {
        ((ColumnVectorWithFilter) vector).setRowIdMapping(rowIdMapping);
      } else if (vector instanceof CometVector) {
        ((CometVector) vector).setRowIdMapping(rowIdMapping);
      }
    }
  }
}
