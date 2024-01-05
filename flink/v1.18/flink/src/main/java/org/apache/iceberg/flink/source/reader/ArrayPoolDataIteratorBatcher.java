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
import java.util.NoSuchElementException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** This implementation stores record batch in array from recyclable pool */
class ArrayPoolDataIteratorBatcher<T> implements DataIteratorBatcher<T> {
  private final int batchSize;
  private final int handoverQueueSize;
  private final RecordFactory<T> recordFactory;

  private transient Pool<T[]> pool;

  ArrayPoolDataIteratorBatcher(ReadableConfig config, RecordFactory<T> recordFactory) {
    this.batchSize = config.get(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT);
    this.handoverQueueSize = config.get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
    this.recordFactory = recordFactory;
  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> batch(
      String splitId, DataIterator<T> inputIterator) {
    Preconditions.checkArgument(inputIterator != null, "Input data iterator can't be null");
    // lazily create pool as it is not serializable
    if (pool == null) {
      this.pool = createPoolOfBatches(handoverQueueSize);
    }
    return new ArrayPoolBatchIterator(splitId, inputIterator, pool);
  }

  private Pool<T[]> createPoolOfBatches(int numBatches) {
    Pool<T[]> poolOfBatches = new Pool<>(numBatches);
    for (int batchId = 0; batchId < numBatches; batchId++) {
      T[] batch = recordFactory.createBatch(batchSize);
      poolOfBatches.add(batch);
    }

    return poolOfBatches;
  }

  private class ArrayPoolBatchIterator
      implements CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> {

    private final String splitId;
    private final DataIterator<T> inputIterator;
    private final Pool<T[]> pool;

    ArrayPoolBatchIterator(String splitId, DataIterator<T> inputIterator, Pool<T[]> pool) {
      this.splitId = splitId;
      this.inputIterator = inputIterator;
      this.pool = pool;
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

      T[] batch = getCachedEntry();
      int recordCount = 0;
      while (inputIterator.hasNext() && recordCount < batchSize) {
        // The record produced by inputIterator can be reused like for the RowData case.
        // inputIterator.next() can't be called again until the copy is made
        // since the record is not consumed immediately.
        T nextRecord = inputIterator.next();
        recordFactory.clone(nextRecord, batch, recordCount);
        recordCount++;
        if (!inputIterator.currentFileHasNext()) {
          // break early so that records in the ArrayResultIterator
          // have the same fileOffset.
          break;
        }
      }

      return ArrayBatchRecords.forRecords(
          splitId,
          pool.recycler(),
          batch,
          recordCount,
          inputIterator.fileOffset(),
          inputIterator.recordOffset() - recordCount);
    }

    @Override
    public void close() throws IOException {
      inputIterator.close();
    }

    private T[] getCachedEntry() {
      try {
        return pool.pollEntry();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for array pool entry", e);
      }
    }
  }
}
