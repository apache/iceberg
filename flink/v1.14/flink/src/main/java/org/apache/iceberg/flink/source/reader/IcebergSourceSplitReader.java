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
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.metrics.MetricsContext.Counter;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergSourceSplitReader<T> implements SplitReader<RecordAndPosition<T>, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final ReaderFunction<T> openSplitFunction;
  private final int indexOfSubtask;
  private final Queue<IcebergSourceSplit> splits;

  private final Counter<Long> assignedSplits;
  private final Counter<Long> assignedBytes;
  private final Counter<Long> finishedSplits;
  private final Counter<Long> finishedBytes;
  private final Counter<Long> splitReaderFetchCalls;

  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> currentReader;
  private IcebergSourceSplit currentSplit;
  private String currentSplitId;

  IcebergSourceSplitReader(ReaderFunction<T> openSplitFunction,
                           SourceReaderContext context,
                           ReaderMetricsContext metrics) {
    this.openSplitFunction = openSplitFunction;
    this.indexOfSubtask = context.getIndexOfSubtask();
    this.splits = new ArrayDeque<>();

    this.assignedSplits = metrics.counter(ReaderMetricsContext.ASSIGNED_SPLITS, Long.class, Unit.COUNT);
    this.assignedBytes = metrics.counter(ReaderMetricsContext.ASSIGNED_BYTES, Long.class, Unit.COUNT);
    this.finishedSplits = metrics.counter(ReaderMetricsContext.FINISHED_SPLITS, Long.class, Unit.COUNT);
    this.finishedBytes = metrics.counter(ReaderMetricsContext.FINISHED_BYTES, Long.class, Unit.COUNT);
    this.splitReaderFetchCalls = metrics.counter(ReaderMetricsContext.SPLIT_READER_FETCH_CALLS, Long.class, Unit.COUNT);
  }

  @Override
  public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws IOException {
    splitReaderFetchCalls.increment();
    checkSplitOrStartNext();

    if (currentReader.hasNext()) {
      // Because Iterator#next() doesn't support checked exception,
      // we need to wrap and unwrap the checked IOException with UncheckedIOException
      try {
        return currentReader.next();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    } else {
      return finishSplit();
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<IcebergSourceSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(String.format(
          "Unsupported split change: %s", splitsChange.getClass()));
    }

    LOG.info("Add {} splits to reader", splitsChange.splits().size());
    splits.addAll(splitsChange.splits());
    assignedSplits.increment(Long.valueOf(splitsChange.splits().size()));
    assignedBytes.increment(calculateBytes(splitsChange));
  }

  @Override
  public void wakeUp() {
  }

  @Override
  public void close() throws Exception {
    currentSplitId = null;
    if (currentReader != null) {
      currentReader.close();
    }
  }

  private long calculateBytes(IcebergSourceSplit split) {
    return split.task().files().stream()
        .map(fileScanTask -> fileScanTask.length())
        .reduce(0L, Long::sum);
  }

  private long calculateBytes(SplitsChange<IcebergSourceSplit> splitsChanges) {
    return splitsChanges.splits().stream()
        .map(split -> calculateBytes(split))
        .reduce(0L, Long::sum);
  }

  private void checkSplitOrStartNext() throws IOException {
    if (currentReader != null) {
      return;
    }

    IcebergSourceSplit nextSplit = splits.poll();
    if (nextSplit == null) {
      // throws IOException when current split is done and there is no more splits available.
      // It will be propagated by the caller of FetchTask#run(). That will cause
      // SplitFetcher to exit. When new split is assigned, a new SplitFetcher
      // will be created to handle it. This behavior is a little odd.
      // Right now, we are copying the same behavior from Flink file source.
      // We can work with Flink community and potentially improve this behavior.
      throw new IOException("No split remaining");
    }

    currentSplit = nextSplit;
    currentSplitId = nextSplit.splitId();
    currentReader = openSplitFunction.apply(currentSplit);
  }

  private ArrayBatchRecords<T> finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }

    ArrayBatchRecords<T> finishRecords = ArrayBatchRecords.finishedSplit(currentSplitId);
    LOG.info("Split reader {} finished split: {}", indexOfSubtask, currentSplitId);
    finishedSplits.increment(1L);
    finishedBytes.increment(calculateBytes(currentSplit));
    currentSplitId = null;
    return finishRecords;
  }
}
