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

package org.apache.iceberg.arrow.vectorized.parquet;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;

/**
 * Vectorized version of the ColumnIterator that reads column values in data pages of a column in a row group in a
 * batched fashion.
 */
public class VectorizedColumnIterator {

  private final ColumnDescriptor desc;
  private final VectorizedPageIterator vectorizedPageIterator;

  // state reset for each row group
  private PageReader columnPageReader = null;
  private long totalValuesCount = 0L;
  private long valuesRead = 0L;
  private long advanceNextPageCount = 0L;
  private final int batchSize;

  public VectorizedColumnIterator(ColumnDescriptor desc, String writerVersion, int batchSize,
                                  boolean setArrowValidityVector) {
    Preconditions.checkArgument(desc.getMaxRepetitionLevel() == 0,
        "Only non-nested columns are supported for vectorized reads");
    this.desc = desc;
    this.batchSize = batchSize;
    this.vectorizedPageIterator = new VectorizedPageIterator(desc, writerVersion, setArrowValidityVector);
  }

  public Dictionary setRowGroupInfo(PageReadStore store, boolean allPagesDictEncoded) {
    this.columnPageReader = store.getPageReader(desc);
    this.totalValuesCount = columnPageReader.getTotalValueCount();
    this.valuesRead = 0L;
    this.advanceNextPageCount = 0L;
    this.vectorizedPageIterator.reset();
    Dictionary dict = ParquetUtil.readDictionary(desc, this.columnPageReader);
    this.vectorizedPageIterator.setDictionaryForColumn(dict, allPagesDictEncoded);
    advance();
    return dict;
  }

  private void advance() {
    if (valuesRead >= advanceNextPageCount) {
      // A parquet page may be empty i.e. contains no values
      while (!vectorizedPageIterator.hasNext()) {
        DataPage page = columnPageReader.readPage();
        if (page != null) {
          vectorizedPageIterator.setPage(page);
          this.advanceNextPageCount += vectorizedPageIterator.currentPageCount();
        } else {
          return;
        }
      }
    }
  }

  public boolean hasNext() {
    return valuesRead < totalValuesCount;
  }

  public void nextBatchIntegers(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchIntegers(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchDictionaryIds(IntVector vector, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchDictionaryIds(vector, batchSize - rowsReadSoFar,
          rowsReadSoFar, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      vector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchLongs(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchLongs(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFloats(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchFloats(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchDoubles(FieldVector fieldVector, int typeWidth, NullabilityHolder holder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchDoubles(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, typeWidth, holder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchIntLongBackedDecimal(
      FieldVector fieldVector,
      int typeWidth,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchIntLongBackedDecimal(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFixedLengthDecimal(
      FieldVector fieldVector,
      int typeWidth,
      NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchFixedLengthDecimal(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchVarWidthType(FieldVector fieldVector, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchVarWidthType(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchFixedWidthBinary(FieldVector fieldVector, int typeWidth, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch =
          vectorizedPageIterator.nextBatchFixedWidthBinary(fieldVector, batchSize - rowsReadSoFar,
              rowsReadSoFar, typeWidth, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

  public void nextBatchBoolean(FieldVector fieldVector, NullabilityHolder nullabilityHolder) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = vectorizedPageIterator.nextBatchBoolean(fieldVector, batchSize - rowsReadSoFar,
          rowsReadSoFar, nullabilityHolder);
      rowsReadSoFar += rowsInThisBatch;
      this.valuesRead += rowsInThisBatch;
      fieldVector.setValueCount(rowsReadSoFar);
    }
  }

}
