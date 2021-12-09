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
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  public ColumnarBatchReader(List<VectorizedReader<?>> readers) {
    super(readers);
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
    Preconditions.checkArgument(numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];

    if (reuse == null) {
      closeVectors();
    }

    ColumnBatchWithRowMapping batch = new ColumnBatchWithRowMapping(deletes, numRowsToRead, rowStartPosInBatch);

    for (int i = 0; i < readers.length; i += 1) {
      vectorHolders[i] = readers[i].read(vectorHolders[i], numRowsToRead);
      int numRowsInVector = vectorHolders[i].numValues();
      Preconditions.checkState(
          numRowsInVector == numRowsToRead,
          "Number of rows in the vector %s didn't match expected %s ", numRowsInVector,
          numRowsToRead);

      arrowColumnVectors[i] = batch.hasDeletes() ?
          ColumnVectorWithFilter.forHolder(vectorHolders[i], batch.rowIdMapping(), batch.numRows()) :
          IcebergArrowColumnVector.forHolder(vectorHolders[i], numRowsInVector);
    }

    rowStartPosInBatch += numRowsToRead;

    return batch.createColumnBatch(arrowColumnVectors);
  }
}
