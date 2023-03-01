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

import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseColumnIterator {
  protected final ColumnDescriptor desc;
  protected final int definitionLevel;

  // state reset for each row group
  protected PageReader pageSource = null;
  protected long triplesCount = 0L;
  protected long triplesRead = 0L;
  protected long advanceNextPageCount = 0L;
  protected Dictionary dictionary;

  // state for page skipping
  protected boolean synchronizing = false;
  protected PrimitiveIterator.OfLong rowIndexes;
  protected long targetRowIndex;
  protected long currentRowIndex;
  protected int skipValues;

  protected BaseColumnIterator(ColumnDescriptor descriptor) {
    this.desc = descriptor;
    this.definitionLevel = desc.getMaxDefinitionLevel() - 1;
  }

  public void setPageSource(PageReader source, Optional<RowRanges> rowRanges) {
    this.pageSource = source;
    this.triplesCount = source.getTotalValueCount();
    this.triplesRead = 0L;
    this.advanceNextPageCount = 0L;
    if (rowRanges.isPresent()) {
      this.synchronizing = true;
      this.rowIndexes = rowRanges.get().iterator();
      this.targetRowIndex = Long.MIN_VALUE;
    } else {
      this.synchronizing = false;
    }

    BasePageIterator pageIterator = pageIterator();
    pageIterator.reset();
    dictionary = ParquetUtil.readDictionary(desc, pageSource);
    pageIterator.setDictionary(dictionary);
    advance();
    skip();
  }

  protected abstract BasePageIterator pageIterator();

  protected void skip() {
    throw new UnsupportedOperationException();
  }

  protected void advance() {
    if (triplesRead >= advanceNextPageCount) {
      BasePageIterator pageIterator = pageIterator();
      while (!pageIterator.hasNext()) {
        DataPage page = pageSource.readPage();
        if (page != null) {
          pageIterator.setPage(page);
          this.advanceNextPageCount += pageIterator.currentPageCount();

          if (synchronizing) {
            long firstRowIndex =
                page.getFirstRowIndex()
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                "Missing page first row index for synchronizing values"));
            this.skipValues = 0;
            this.currentRowIndex = firstRowIndex - 1;
          }
        } else {
          return;
        }
      }
    }
  }

  public boolean hasNext() {
    return triplesRead < triplesCount;
  }
}
