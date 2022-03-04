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
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergSourceSplitReader<T> implements SplitReader<RecordAndPosition<T>, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final ReaderFunction<T> readerFunction;
  private final int indexOfSubtask;
  private final IcebergSourceReaderMetrics metrics;
  private final Queue<IcebergSourceSplit> splits;

  private CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> currentReader;
  private IcebergSourceSplit currentSplit;
  private String currentSplitId;

  IcebergSourceSplitReader(ReaderFunction<T> readerFunction,
                           SourceReaderContext context,
                           IcebergSourceReaderMetrics metrics) {
    this.readerFunction = readerFunction;
    this.indexOfSubtask = context.getIndexOfSubtask();
    this.metrics = metrics;
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws IOException {
    metrics.incrementSplitReaderFetchCalls();
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
  public void handleSplitsChanges(SplitsChange<IcebergSourceSplit> splitsChanges) {
    LOG.info("Add splits to reader: {}", splitsChanges.splits());
    splits.addAll(splitsChanges.splits());
    metrics.incrementAssignedSplits(splitsChanges.splits().size());
    metrics.incrementAssignedBytes(calculateBytes(splitsChanges));
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
      throw new IOException("No split remaining");
    }

    currentSplit = nextSplit;
    currentSplitId = nextSplit.splitId();
    currentReader = readerFunction.apply(currentSplit);
  }

  private ArrayBatchRecords<T> finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }

    ArrayBatchRecords<T> finishRecords = ArrayBatchRecords.finishedSplit(currentSplitId);
    LOG.info("Split reader {} finished split: {}", indexOfSubtask, currentSplitId);
    metrics.incrementFinishedSplits(1L);
    metrics.incrementFinishedBytes(calculateBytes(currentSplit));
    currentSplitId = null;
    return finishRecords;
  }
}
