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
import java.util.Collections;
import java.util.Queue;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergSourceSplitReader<T> implements SplitReader<RecordAndPosition<T>, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final ReaderFunction<T> openSplitFunction;
  private final int indexOfSubtask;
  private final Queue<IcebergSourceSplit> splits;

  private final Counter assignedSplits;
  private final Counter assignedBytes;
  private final Counter finishedSplits;
  private final Counter finishedBytes;
  private final Counter splitReaderFetchCalls;

  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> currentReader;
  private IcebergSourceSplit currentSplit;
  private String currentSplitId;

  IcebergSourceSplitReader(ReaderFunction<T> openSplitFunction, SourceReaderContext context) {
    this.openSplitFunction = openSplitFunction;
    this.indexOfSubtask = context.getIndexOfSubtask();
    this.splits = new ArrayDeque<>();

    MetricGroup metrics = context.metricGroup();
    this.assignedSplits = metrics.counter("assignedSplits");
    this.assignedBytes = metrics.counter("assignedBytes");
    this.finishedSplits = metrics.counter("finishedSplits");
    this.finishedBytes = metrics.counter("finishedBytes");
    this.splitReaderFetchCalls = metrics.counter("splitReaderFetchCalls");
  }

  @Override
  public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws IOException {
    splitReaderFetchCalls.inc();
    if (currentReader == null) {
      IcebergSourceSplit nextSplit = splits.poll();
      if (nextSplit != null) {
        currentSplit = nextSplit;
        currentSplitId = nextSplit.splitId();
        currentReader = openSplitFunction.apply(currentSplit);
      } else {
        // return an empty result, which will lead to split fetch to be idle.
        // SplitFetcherManager will then close idle fetcher.
        return new RecordsBySplits(Collections.emptyMap(), Collections.emptySet());
      }
    }

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
      throw new UnsupportedOperationException(
          String.format("Unsupported split change: %s", splitsChange.getClass()));
    }

    LOG.info("Add {} splits to reader", splitsChange.splits().size());
    splits.addAll(splitsChange.splits());
    assignedSplits.inc(Long.valueOf(splitsChange.splits().size()));
    assignedBytes.inc(calculateBytes(splitsChange));
  }

  @Override
  public void wakeUp() {}

  @Override
  public void close() throws Exception {
    currentSplitId = null;
    if (currentReader != null) {
      currentReader.close();
    }
  }

  private long calculateBytes(IcebergSourceSplit split) {
    return split.task().files().stream().map(FileScanTask::length).reduce(0L, Long::sum);
  }

  private long calculateBytes(SplitsChange<IcebergSourceSplit> splitsChanges) {
    return splitsChanges.splits().stream().map(this::calculateBytes).reduce(0L, Long::sum);
  }

  private ArrayBatchRecords<T> finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }

    ArrayBatchRecords<T> finishRecords = ArrayBatchRecords.finishedSplit(currentSplitId);
    LOG.info("Split reader {} finished split: {}", indexOfSubtask, currentSplitId);
    finishedSplits.inc(1L);
    finishedBytes.inc(calculateBytes(currentSplit));
    currentSplitId = null;
    return finishRecords;
  }
}
