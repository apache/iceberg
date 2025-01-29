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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseColumnIterator {
  protected final ColumnDescriptor desc;

  // state reset for each row group
  protected PageReader pageSource = null;
  protected long triplesCount = 0L;
  protected long triplesRead = 0L;
  protected long advanceNextPageCount = 0L;
  protected Dictionary dictionary;

  protected BaseColumnIterator(ColumnDescriptor descriptor) {
    this.desc = descriptor;
  }

  public void setPageSource(PageReader source) {
    this.pageSource = source;
    this.triplesCount = source.getTotalValueCount();
    this.triplesRead = 0L;
    this.advanceNextPageCount = 0L;
    BasePageIterator pageIterator = pageIterator();
    pageIterator.reset();
    dictionary = ParquetUtil.readDictionary(desc, pageSource);
    pageIterator.setDictionary(dictionary);
    advance();
  }

  protected abstract BasePageIterator pageIterator();

  protected void advance() {
    if (triplesRead >= advanceNextPageCount) {
      BasePageIterator pageIterator = pageIterator();
      while (!pageIterator.hasNext()) {
        DataPage page = pageSource.readPage();
        if (page != null) {
          pageIterator.setPage(page);
          this.advanceNextPageCount += pageIterator.currentPageCount();
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
