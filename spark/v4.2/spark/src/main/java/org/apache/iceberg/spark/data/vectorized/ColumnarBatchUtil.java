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
import java.util.function.Predicate;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatchRow;

public class ColumnarBatchUtil {

  private ColumnarBatchUtil() {}

  /**
   * Builds a row ID mapping inside a batch to skip deleted rows.
   *
   * <pre>
   * Initial state
   * Data values: [v0, v1, v2, v3, v4, v5, v6, v7]
   * Row ID mapping: [0, 1, 2, 3, 4, 5, 6, 7]
   *
   * Apply position deletes
   * Position deletes: 2, 6
   * Row ID mapping: [0, 1, 3, 4, 5, 7, -, -] (6 live records)
   *
   * Apply equality deletes
   * Equality deletes: v1, v2, v3
   * Row ID mapping: [0, 4, 5, 7, -, -, -, -] (4 live records)
   * </pre>
   *
   * @param columnVectors the array of column vectors for the batch
   * @param deletes the delete filter containing delete information
   * @param rowStartPosInBatch the starting position of the row in the batch
   * @param batchSize the size of the batch
   * @return the mapping array and the number of live rows, or {@code null} if nothing is deleted
   */
  public static Pair<int[], Integer> buildRowIdMapping(
      ColumnVector[] columnVectors,
      DeleteFilter<InternalRow> deletes,
      long rowStartPosInBatch,
      int batchSize) {
    if (deletes == null) {
      return null;
    }

    PositionDeleteIndex deletedPositions = deletes.deletedRowPositions();
    Predicate<InternalRow> eqDeleteFilter = deletes.eqDeletedRowFilter();
    ColumnarBatchRow row = new ColumnarBatchRow(columnVectors);
    int[] rowIdMapping = new int[batchSize];
    int liveRowId = 0;

    for (int rowId = 0; rowId < batchSize; rowId++) {
      long pos = rowStartPosInBatch + rowId;
      row.rowId = rowId;
      if (isDeleted(pos, row, deletedPositions, eqDeleteFilter)) {
        deletes.incrementDeleteCount();
      } else {
        rowIdMapping[liveRowId] = rowId;
        liveRowId++;
      }
    }

    return liveRowId == batchSize ? null : Pair.of(rowIdMapping, liveRowId);
  }

  /**
   * Builds a boolean array to indicate if a row is deleted or not.
   *
   * <pre>
   * Initial state
   * Data values: [v0, v1, v2, v3, v4, v5, v6, v7]
   * Is deleted array: [F, F, F, F, F, F, F, F]
   *
   * Apply position deletes
   * Position deletes: 2, 6
   * Is deleted array: [F, F, T, F, F, F, T, F] (6 live records)
   *
   * Apply equality deletes
   * Equality deletes: v1, v2, v3
   * Is deleted array: [F, T, T, T, F, F, T, F] (4 live records)
   * </pre>
   *
   * @param columnVectors the array of column vectors for the batch.
   * @param deletes the delete filter containing information about which rows should be deleted.
   * @param rowStartPosInBatch the starting position of the row in the batch, used to calculate the
   *     absolute position of the rows in the context of the entire dataset.
   * @param batchSize the number of rows in the current batch.
   * @return an array of boolean values to indicate if a row is deleted or not
   */
  public static boolean[] buildIsDeleted(
      ColumnVector[] columnVectors,
      DeleteFilter<InternalRow> deletes,
      long rowStartPosInBatch,
      int batchSize) {
    boolean[] isDeleted = new boolean[batchSize];

    if (deletes == null) {
      return isDeleted;
    }

    PositionDeleteIndex deletedPositions = deletes.deletedRowPositions();
    Predicate<InternalRow> eqDeleteFilter = deletes.eqDeletedRowFilter();
    ColumnarBatchRow row = new ColumnarBatchRow(columnVectors);

    for (int rowId = 0; rowId < batchSize; rowId++) {
      long pos = rowStartPosInBatch + rowId;
      row.rowId = rowId;
      if (isDeleted(pos, row, deletedPositions, eqDeleteFilter)) {
        deletes.incrementDeleteCount();
        isDeleted[rowId] = true;
      }
    }

    return isDeleted;
  }

  private static boolean isDeleted(
      long pos,
      InternalRow row,
      PositionDeleteIndex deletedPositions,
      Predicate<InternalRow> eqDeleteFilter) {
    // use separate if statements to reduce the chance of speculative execution for equality tests
    if (deletedPositions != null && deletedPositions.isDeleted(pos)) {
      return true;
    }

    if (eqDeleteFilter != null && !eqDeleteFilter.test(row)) {
      return true;
    }

    return false;
  }

  /**
   * Removes extra column vectors added for processing equality delete filters that are not part of
   * the final query output.
   *
   * <p>During query execution, additional columns may be included in the schema to evaluate
   * equality delete filters. For example, if the table schema contains columns C1, C2, C3, C4, and
   * C5, and the query is 'SELECT C5 FROM table'. While equality delete filters are applied on C3
   * and C4, the processing schema includes C5, C3, and C4. These extra columns (C3 and C4) are
   * needed to identify rows to delete but are not included in the final result.
   *
   * <p>This method removes the extra column vectors from the end of column vectors array, ensuring
   * only the expected column vectors remain.
   *
   * @param deletes the delete filter containing delete information.
   * @param columnVectors the array of column vectors representing query result data
   * @return a new column vectors array with extra column vectors removed, or the original column
   *     vectors array if no extra column vectors are found
   */
  public static ColumnVector[] removeExtraColumns(
      DeleteFilter<InternalRow> deletes, ColumnVector[] columnVectors) {
    int expectedColumnSize = deletes.expectedSchema().columns().size();
    if (columnVectors.length > expectedColumnSize) {
      return Arrays.copyOf(columnVectors, expectedColumnSize);
    } else {
      return columnVectors;
    }
  }
}
