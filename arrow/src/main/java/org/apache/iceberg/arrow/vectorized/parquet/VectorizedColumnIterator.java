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

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.ParquetDecodingException;

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

  public VectorizedColumnIterator(ColumnDescriptor desc, String writerVersion, int batchSize) {
    this.desc = desc;
    this.batchSize = batchSize;
    this.vectorizedPageIterator = new VectorizedPageIterator(desc, writerVersion, batchSize);
  }

  public Dictionary setRowGroupInfo(PageReadStore store, boolean allPagesDictEncoded) {
    this.columnPageReader = store.getPageReader(desc);
    this.totalValuesCount = columnPageReader.getTotalValueCount();
    this.valuesRead = 0L;
    this.advanceNextPageCount = 0L;
    this.vectorizedPageIterator.reset();
    Dictionary dict = readDictionaryForColumn(store);
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   */
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   */
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   */
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   */
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   */
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

  /**
   * Method for reading a batch of decimals backed by INT32 and INT64 parquet data types.
   */
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

  /**
   * Method for reading a batch of decimals backed by fixed length byte array parquet data type.
   */
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

  /**
   * Method for reading a batch of variable width data type (ENUM, JSON, UTF8, BSON).
   */
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

  /**
   * Method for reading batches of fixed width binary type (e.g. BYTE[7]).
   */
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

  /**
   * Method for reading batches of booleans.
   */
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

  private Dictionary readDictionaryForColumn(
      PageReadStore pageReadStore) {
    DictionaryPage dictionaryPage = pageReadStore.getPageReader(desc).readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        return dictionaryPage.getEncoding().initDictionary(desc, dictionaryPage);
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + desc, e);
      }
    }
    return null;
  }
}
