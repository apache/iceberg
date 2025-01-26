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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.comet.parquet.AbstractColumnReader;
import org.apache.comet.parquet.BatchReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
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
 * populated via delegated read calls to {@link CometColumnReader VectorReader(s)}.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
class CometColumnarBatchReader implements VectorizedReader<ColumnarBatch> {

  private final CometColumnReader[] readers;
  private final boolean hasIsDeletedColumn;
  private DeleteFilter<InternalRow> deletes = null;
  private long rowStartPosInBatch = 0;
  // The delegated batch reader on Comet side
  private final BatchReader delegate;

  CometColumnarBatchReader(List<VectorizedReader<?>> readers, Schema schema) {
    this.readers =
        readers.stream().map(CometColumnReader.class::cast).toArray(CometColumnReader[]::new);
    this.hasIsDeletedColumn =
        readers.stream().anyMatch(reader -> reader instanceof CometDeleteColumnReader);

    AbstractColumnReader[] abstractColumnReaders = new AbstractColumnReader[readers.size()];
    delegate = new BatchReader(abstractColumnReaders);
    delegate.setSparkSchema(SparkSchemaUtil.convert(schema));
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    setRowGroupInfo(pageStore, metaData);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData) {
    for (int i = 0; i < readers.length; i++) {
      try {
        if (!(readers[i] instanceof CometConstantColumnReader)
            && !(readers[i] instanceof CometPositionColumnReader)
            && !(readers[i] instanceof CometDeleteColumnReader)) {
          readers[i].reset();
          readers[i].setPageReader(pageStore.getPageReader(readers[i].getDescriptor()));
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to setRowGroupInfo for Comet vectorization", e);
      }
    }

    for (int i = 0; i < readers.length; i++) {
      delegate.getColumnReaders()[i] = this.readers[i].getDelegate();
    }

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
    ColumnarBatch columnarBatch = new ColumnBatchLoader(numRowsToRead).loadDataToColumnBatch();
    rowStartPosInBatch += numRowsToRead;
    return columnarBatch;
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (CometColumnReader reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }

  @Override
  public void close() {
    for (CometColumnReader reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private class ColumnBatchLoader {
    private final int batchSize;

    ColumnBatchLoader(int numRowsToRead) {
      Preconditions.checkArgument(
          numRowsToRead > 0, "Invalid number of rows to read: %s", numRowsToRead);
      this.batchSize = numRowsToRead;
    }

    ColumnarBatch loadDataToColumnBatch() {
      ColumnVector[] arrowColumnVectors = readDataToColumnVectors();
      int numLiveRows = batchSize;
      if (hasIsDeletedColumn) {
        boolean[] isDeleted =
            ColumnarBatchUtil.buildIsDeleted(
                arrowColumnVectors, deletes, rowStartPosInBatch, batchSize);
        readDeletedColumn(arrowColumnVectors, isDeleted);
      } else {
        Pair<int[], Integer> pair =
            ColumnarBatchUtil.buildRowIdMapping(
                arrowColumnVectors, deletes, rowStartPosInBatch, batchSize);
        if (pair != null) {
          int[] rowIdMapping = pair.first();
          numLiveRows = pair.second();
          for (int i = 0; i < arrowColumnVectors.length; i++) {
            ((CometVector) arrowColumnVectors[i]).setRowIdMapping(rowIdMapping);
          }
        }
      }

      if (deletes != null && deletes.hasEqDeletes()) {
        arrowColumnVectors = ColumnarBatchUtil.removeExtraColumns(deletes, arrowColumnVectors);
      }

      ColumnarBatch newColumnarBatch = new ColumnarBatch(arrowColumnVectors);
      newColumnarBatch.setNumRows(numLiveRows);
      return newColumnarBatch;
    }

    ColumnVector[] readDataToColumnVectors() {
      CometVector[] columnVectors = new CometVector[readers.length];
      // Fetch rows for all readers in the delegate
      delegate.nextBatch(batchSize);
      for (int i = 0; i < readers.length; i++) {
        columnVectors[i] = readers[i].getVector();
        columnVectors[i].resetRowIdMapping();
        org.apache.comet.vector.CometVector vector = readers[i].getDelegate().currentBatch();
        columnVectors[i].setDelegate(vector);
      }

      return columnVectors;
    }

    void readDeletedColumn(ColumnVector[] columnVectors, boolean[] isDeleted) {
      for (int i = 0; i < readers.length; i++) {
        if (readers[i] instanceof CometDeleteColumnReader) {
          CometDeleteColumnReader deleteColumnReader = new CometDeleteColumnReader<>(isDeleted);
          deleteColumnReader.setBatchSize(batchSize);
          deleteColumnReader.read(deleteColumnReader.getVector(), batchSize);
          columnVectors[i] = deleteColumnReader.getVector();
        }
      }
    }
  }
}
