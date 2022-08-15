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
    Preconditions.checkArgument(
        numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

    if (reuse == null) {
      closeVectors();
    }

    Pair<int[], Integer> rowIdMapping = rowIdMapping(numRowsToRead);

    for (int i = 0; i < readers.length; i += 1) {
      vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
      int numRowsInVector = vectorHolders[i].numValues();
      Preconditions.checkState(
          numRowsInVector == numRowsToRead,
          "Number of rows in the vector %s didn't match expected %s ",
          numRowsInVector,
          numRowsToRead);

      if (rowIdMapping == null) {
        arrowColumnVectors[i] =
            IcebergArrowColumnVector.forHolder(vectorHolders[i], numRowsInVector);
      } else {
        int[] rowIdMap = rowIdMapping.first();
        Integer numRows = rowIdMapping.second();
        arrowColumnVectors[i] =
            ColumnVectorWithFilter.forHolder(vectorHolders[i], rowIdMap, numRows);
      }
    }

    rowStartPosInBatch += numRowsToRead;
    ColumnarBatch batch = new ColumnarBatch(arrowColumnVectors);

    if (rowIdMapping == null) {
      batch.setNumRows(numRowsToRead);
    } else {
      Integer numRows = rowIdMapping.second();
      batch.setNumRows(numRows);
    }
    return batch;
  }

  private Pair<int[], Integer> rowIdMapping(int numRows) {
    if (deletes != null && deletes.hasPosDeletes()) {
      return buildRowIdMapping(deletes.deletedRowPositions(), numRows);
    } else {
      return null;
    }
  }

  /**
   * Build a row id mapping inside a batch, which skips delete rows. For example, if the 1st and 3rd
   * rows are deleted in a batch with 5 rows, the mapping would be {0->1, 1->3, 2->4}, and the new
   * num of rows is 3.
   *
   * @param deletedRowPositions a set of deleted row positions
   * @param numRows the num of rows
   * @return the mapping array and the new num of rows in a batch, null if no row is deleted
   */
  private Pair<int[], Integer> buildRowIdMapping(
      PositionDeleteIndex deletedRowPositions, int numRows) {
    if (deletedRowPositions == null) {
      return null;
    }

    int[] rowIdMapping = new int[numRows];
    int originalRowId = 0;
    int currentRowId = 0;
    while (originalRowId < numRows) {
      if (!deletedRowPositions.isDeleted(originalRowId + rowStartPosInBatch)) {
        rowIdMapping[currentRowId] = originalRowId;
        currentRowId++;
      }
      originalRowId++;
    }

    if (currentRowId == numRows) {
      // there is no delete in this batch
      return null;
    } else {
      return Pair.of(rowIdMapping, currentRowId);
    }
  }
}
