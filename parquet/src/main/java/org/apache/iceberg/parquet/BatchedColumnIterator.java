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

package org.apache.iceberg.parquet;

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.ParquetDecodingException;

public class BatchedColumnIterator {

  private final ColumnDescriptor desc;
  private final BatchedPageIterator batchedPageIterator;

  // state reset for each row group
  private PageReader pageSource = null;
  private long triplesCount = 0L;
  private long triplesRead = 0L;
  private long advanceNextPageCount = 0L;
  private final int batchSize;

  public BatchedColumnIterator(ColumnDescriptor desc, String writerVersion, int batchSize) {
    this.desc = desc;
    this.batchSize = batchSize;
    this.batchedPageIterator = new BatchedPageIterator(desc, writerVersion, batchSize);
  }

  public void setPageSource(PageReadStore store) {
    this.pageSource = store.getPageReader(desc);
    this.triplesCount = pageSource.getTotalValueCount();
    this.triplesRead = 0L;
    this.advanceNextPageCount = 0L;
    this.batchedPageIterator.reset();
    this.batchedPageIterator.setDictionary(readDictionary(desc, pageSource));
    advance();
  }

  private void advance() {
    if (triplesRead >= advanceNextPageCount) {
      // A parquet page could be empty (no values) -> confirm with Ryan, anjali
      while (!batchedPageIterator.hasNext()) {
        DataPage page = pageSource.readPage();
        if (page != null) {
          batchedPageIterator.setPage(page);
          this.advanceNextPageCount += batchedPageIterator.currentPageCount();
        } else {
          return;
        }
      }
    }
  }

  public boolean hasNext() {
    return triplesRead < triplesCount;
  }

  public void nextBatch(FieldVector fieldVector) {
    int rowsReadSoFar = 0;
    while (rowsReadSoFar < batchSize && hasNext()) {
      advance();
      int rowsInThisBatch = batchedPageIterator.nextBatch(fieldVector, batchSize - rowsReadSoFar);
      rowsReadSoFar += rowsInThisBatch;
      this.triplesRead += rowsInThisBatch;
    }
  }



  private static Dictionary readDictionary(ColumnDescriptor desc, PageReader pageSource) {
    DictionaryPage dictionaryPage = pageSource.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        return dictionaryPage.getEncoding().initDictionary(desc, dictionaryPage);
//        if (converter.hasDictionarySupport()) {
//          converter.setDictionary(dictionary);
//        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + desc, e);
      }
    }
    return null;
  }
}
