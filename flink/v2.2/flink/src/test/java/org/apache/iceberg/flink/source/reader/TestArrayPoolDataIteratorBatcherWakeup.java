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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests that {@link ArrayPoolDataIteratorBatcher}'s iterator reacts correctly to {@code wakeUp()},
 * verifying the fix for issue #16426 (fetcher thread leak during cancellation). A real {@link
 * PoolWithWakeup} is injected in a controlled state so no file I/O is needed.
 */
class TestArrayPoolDataIteratorBatcherWakeup {

  private static Configuration config() {
    Configuration config = new Configuration();
    config.set(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 2);
    config.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 2);
    return config;
  }

  /**
   * When the pool is exhausted, {@code next()} blocks. {@code wakeUp()} must unblock it and yield
   * an empty batch so the fetcher thread can return control (and exit on shutdown) instead of
   * leaking.
   */
  @Test
  @Timeout(10)
  @SuppressWarnings("unchecked")
  void testWakeUpUnblocksExhaustedPoll() throws Exception {
    ArrayPoolDataIteratorBatcher<RowData> batcher =
        new ArrayPoolDataIteratorBatcher<>(
            config(), new RowDataRecordFactory(TestFixtures.ROW_TYPE));
    // Empty pool simulates exhaustion (watermark alignment / shutdown).
    batcher.setPoolForTesting(new PoolWithWakeup<>(2));

    DataIterator<RowData> mockIterator = mock(DataIterator.class);
    when(mockIterator.hasNext()).thenReturn(true);

    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> iterator =
        batcher.batch("test-split", mockIterator);
    assertThat(iterator).isInstanceOf(WakeableIterator.class);

    CountDownLatch threadStarted = new CountDownLatch(1);
    AtomicReference<RecordsWithSplitIds<RecordAndPosition<RowData>>> result =
        new AtomicReference<>();
    Thread fetchThread =
        new Thread(
            () -> {
              threadStarted.countDown();
              result.set(iterator.next());
            });
    fetchThread.start();

    try {
      assertThat(threadStarted.await(2, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(200);
      assertThat(fetchThread.isAlive()).as("Thread should be blocked on exhausted pool").isTrue();

      // This is what Flink does during shutdown.
      ((WakeableIterator<?>) iterator).wakeUp();

      fetchThread.join(2000);
      assertThat(fetchThread.isAlive()).as("Thread should exit after wakeUp").isFalse();

      // A woken read returns an empty batch (no records, no finished splits) so fetch() stays
      // reentrant.
      RecordsWithSplitIds<RecordAndPosition<RowData>> batch = result.get();
      assertThat(batch).isNotNull();
      assertThat(batch.nextSplit()).isNull();
      assertThat(batch.finishedSplits()).isEmpty();
    } finally {
      if (fetchThread.isAlive()) {
        ((WakeableIterator<?>) iterator).wakeUp();
        fetchThread.join(1000);
      }
    }
  }

  /**
   * After a wakeup the iterator must be reentrant: a subsequent {@code next()} blocks again while
   * the pool stays exhausted (this is the watermark-alignment pause).
   */
  @Test
  @Timeout(10)
  @SuppressWarnings("unchecked")
  void testReentrantAfterWakeUp() throws Exception {
    ArrayPoolDataIteratorBatcher<RowData> batcher =
        new ArrayPoolDataIteratorBatcher<>(
            config(), new RowDataRecordFactory(TestFixtures.ROW_TYPE));
    batcher.setPoolForTesting(new PoolWithWakeup<>(2));

    DataIterator<RowData> mockIterator = mock(DataIterator.class);
    when(mockIterator.hasNext()).thenReturn(true);

    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> iterator =
        batcher.batch("test-split", mockIterator);

    // First call: wake it up, get an empty batch.
    Thread t1 = new Thread(iterator::next);
    t1.start();
    Thread.sleep(200);
    ((WakeableIterator<?>) iterator).wakeUp();
    t1.join(2000);
    assertThat(t1.isAlive()).isFalse();

    // Second call must block again (the wakeup was consumed), proving the pause still holds.
    Thread t2 = new Thread(iterator::next);
    t2.start();
    Thread.sleep(300);
    assertThat(t2.isAlive()).as("Second next() should block again after the wakeup").isTrue();

    ((WakeableIterator<?>) iterator).wakeUp();
    t2.join(2000);
    assertThat(t2.isAlive()).isFalse();
  }

  /** Normal operation: when the pool has an entry, {@code next()} returns a real batch. */
  @Test
  @Timeout(10)
  @SuppressWarnings("unchecked")
  void testNormalReadWhenEntryAvailable() {
    ArrayPoolDataIteratorBatcher<RowData> batcher =
        new ArrayPoolDataIteratorBatcher<>(
            config(), new RowDataRecordFactory(TestFixtures.ROW_TYPE));
    PoolWithWakeup<RowData[]> pool = new PoolWithWakeup<>(2);
    pool.add(new RowData[2]);
    batcher.setPoolForTesting(pool);

    DataIterator<RowData> mockIterator = mock(DataIterator.class);
    // true for next()'s guard check, false for the fill loop so we don't exercise record cloning.
    when(mockIterator.hasNext()).thenReturn(true, false);
    when(mockIterator.fileOffset()).thenReturn(0);
    when(mockIterator.recordOffset()).thenReturn(0L);

    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> iterator =
        batcher.batch("test-split", mockIterator);

    RecordsWithSplitIds<RecordAndPosition<RowData>> batch = iterator.next();
    assertThat(batch).isNotNull();
    // A real batch carries the split id.
    assertThat(batch.nextSplit()).isEqualTo("test-split");
  }
}
