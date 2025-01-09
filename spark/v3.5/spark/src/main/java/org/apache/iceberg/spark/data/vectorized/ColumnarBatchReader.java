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
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.DeletedVectorReader;
import org.apache.iceberg.data.DeleteFilter;
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
    setRowGroupInfo(pageStore, metaData);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    super.setRowGroupInfo(pageStore, metaData);
    this.rowStartPosInBatch =
        pageStore
            .getRowIndexOffset()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "PageReadStore does not contain row index offset"));
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

  private class ColumnBatchLoader {
    private int numRowsToRead;
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
      ColumnVector[] arrowColumnVectors = readDataToColumnVectors();
      if (!hasIsDeletedColumn) {
        Pair<int[], Integer> pair =
            ColumnarBatchUtil.buildRowIdMapping(
                arrowColumnVectors, deletes, rowStartPosInBatch, numRowsToRead);
        if (pair != null) {
          rowIdMapping = pair.first();
          numRowsToRead = pair.second();
          for (int i = 0; i < arrowColumnVectors.length; i++) {
            ColumnVector vector = arrowColumnVectors[i];
            if (vector instanceof IcebergArrowColumnVector) {
              arrowColumnVectors[i] =
                  new ColumnVectorWithFilter(
                      ((IcebergArrowColumnVector) vector).vector(), rowIdMapping);
            }
          }
        }
      } else {
        isDeleted =
            ColumnarBatchUtil.buildIsDeleted(
                arrowColumnVectors, deletes, rowStartPosInBatch, numRowsToRead);
        for (int i = 0; i < arrowColumnVectors.length; i++) {
          ColumnVector vector = arrowColumnVectors[i];
          if (vector instanceof DeletedColumnVector) {
            ((DeletedColumnVector) vector).setIsDeleted(isDeleted);
          }
        }
      }

      ColumnarBatch newColumnarBatch = new ColumnarBatch(arrowColumnVectors);
      newColumnarBatch.setNumRows(numRowsToRead);

      if (deletes != null && deletes.hasEqDeletes()) {
        return ColumnarBatchUtil.removeExtraColumns(deletes, arrowColumnVectors, newColumnarBatch);
      } else {
        return newColumnarBatch;
      }
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

        arrowColumnVectors[i] = columnVectorBuilder.build(vectorHolders[i], numRowsInVector);
      }
      return arrowColumnVectors;
    }
  }
}
