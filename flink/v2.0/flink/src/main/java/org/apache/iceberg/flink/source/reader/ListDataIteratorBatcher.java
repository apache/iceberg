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
package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * FlinkRecordReaderFunction essentially cloned objects already. So there is no need to use array
 * pool to clone objects. Simply create a new ArrayList for each batch.
 */
class ListDataIteratorBatcher<T> implements DataIteratorBatcher<T> {

  private final int batchSize;

  ListDataIteratorBatcher(ReadableConfig config) {
    this.batchSize = config.get(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT);
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> batch(
      String splitId, DataIterator<T> dataIterator) {
    return new ListBatchIterator(splitId, dataIterator);
  }

  private class ListBatchIterator
      implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> {

    private final String splitId;
    private final DataIterator<T> inputIterator;

    ListBatchIterator(String splitId, DataIterator<T> inputIterator) {
      this.splitId = splitId;
      this.inputIterator = inputIterator;
    }

    @Override
    public boolean hasNext() {
      return inputIterator.hasNext();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<T>> next() {
      if (!inputIterator.hasNext()) {
        throw new NoSuchElementException();
      }

      final List<T> batch = Lists.newArrayListWithCapacity(batchSize);
      int recordCount = 0;
      while (inputIterator.hasNext() && recordCount < batchSize) {
        T nextRecord = inputIterator.next();
        batch.add(nextRecord);
        recordCount++;
        if (!inputIterator.currentFileHasNext()) {
          // break early so that records have the same fileOffset.
          break;
        }
      }

      return ListBatchRecords.forRecords(
          splitId, batch, inputIterator.fileOffset(), inputIterator.recordOffset() - recordCount);
    }

    @Override
    public void close() throws IOException {
      if (inputIterator != null) {
        inputIterator.close();
      }
    }
  }
}
