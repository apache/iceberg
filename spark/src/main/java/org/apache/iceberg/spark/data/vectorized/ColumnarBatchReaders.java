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

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * {@link VectorizedReader} that returns Spark's {@link ColumnarBatch} to support Spark's vectorized read path. The
 * {@link ColumnarBatch} returned is created by passing in the Arrow vectors populated via delegated read calls to
 * {@linkplain VectorizedArrowReader VectorReader(s)}.
 */
public class ColumnarBatchReaders implements VectorizedReader<ColumnarBatch> {
  private final VectorizedArrowReader[] readers;

  public ColumnarBatchReaders(List<VectorizedReader> readers) {
    this.readers = (VectorizedArrowReader[]) Array.newInstance(
        VectorizedArrowReader.class, readers.size());
    int idx = 0;
    for (VectorizedReader reader : readers) {
      this.readers[idx] = (VectorizedArrowReader) reader;
      idx++;
    }
  }

  @Override
  public final void setRowGroupInfo(PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    for (int i = 0; i < readers.length; i += 1) {
      if (readers[i] != null) {
        readers[i].setRowGroupInfo(pageStore, metaData);
      }
    }
  }

  @Override
  public void reuseContainers(boolean reuse) {
    for (VectorizedReader reader : readers) {
      reader.reuseContainers(reuse);
    }
  }

  @Override
  public final ColumnarBatch read(int numRowsToRead) {
    Preconditions.checkArgument(numRowsToRead > 0, "Invalid value: " + numRowsToRead);
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];
    int prevNum = 0;
    for (int i = 0; i < readers.length; i += 1) {
      VectorHolder holder = readers[i].read(numRowsToRead);
      int numRowsInVector = holder.numValues();
      Preconditions.checkState(
          numRowsInVector == numRowsToRead,
          "Number of rows in the vector " + numRowsInVector + " didn't match expected " +
              numRowsToRead);
      if (prevNum > 0) {
        // assert that all the vectors in the batch have the same number of rows
        Preconditions.checkState(numRowsInVector == prevNum, "Number of rows in arrow vectors didn't match " +
            "for " + readers[i - 1] + " and " + readers[i]);
      } else {
        prevNum = numRowsInVector;
      }
      arrowColumnVectors[i] = IcebergArrowColumnVector.forHolder(holder, numRowsInVector);
    }
    ColumnarBatch batch = new ColumnarBatch(arrowColumnVectors);
    batch.setNumRows(numRowsToRead);
    return batch;
  }

  @Override
  public void close() {
    for (VectorizedReader reader : readers) {
      reader.close();
    }
  }
}
