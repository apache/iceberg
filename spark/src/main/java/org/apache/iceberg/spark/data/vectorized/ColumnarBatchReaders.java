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
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader;
import org.apache.iceberg.parquet.vectorized.VectorizedReader;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
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
  private final int batchSize;
  private boolean reuseContainers;

  public ColumnarBatchReaders(
      List<VectorizedReader> readers,
      int bSize) {
    this.readers = (VectorizedArrowReader[]) Array.newInstance(
        VectorizedArrowReader.class, readers.size());
    int idx = 0;
    for (VectorizedReader reader : readers) {
      this.readers[idx] = (VectorizedArrowReader) reader;
      idx++;
    }
    this.batchSize = bSize;
  }

  @Override
  public final void setRowGroupInfo(
      PageReadStore pageStore,
      DictionaryPageReadStore dictionaryPageReadStore,
      Map<ColumnPath, Boolean> columnDictEncoded) {
    for (int i = 0; i < readers.length; i += 1) {
      if (readers[i] != null) {
        readers[i].setRowGroupInfo(pageStore, dictionaryPageReadStore, columnDictEncoded);
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
  public final ColumnarBatch read() {
    ColumnVector[] arrowColumnVectors = new ColumnVector[readers.length];
    int numRows = 0;
    for (int i = 0; i < readers.length; i += 1) {
      VectorHolder holder = readers[i].read();
      FieldVector vector = holder.getVector();
      if (vector == null) {
        arrowColumnVectors[i] = new NullValuesColumnVector(batchSize);
      } else {
        arrowColumnVectors[i] = new IcebergArrowColumnVector(holder);
        numRows = vector.getValueCount();
      }
    }
    ColumnarBatch batch = new ColumnarBatch(arrowColumnVectors);
    batch.setNumRows(numRows);
    return batch;
  }

}
