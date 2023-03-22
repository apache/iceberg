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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

public class VectorizedParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, VectorizedReader<?>> batchReaderFunc;
  private final Expression filter;
  private boolean reuseContainers;
  private final boolean caseSensitive;
  private final int batchSize;
  private final NameMapping nameMapping;

  public VectorizedParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.batchReaderFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.batchSize = maxRecordsPerBatch;
    this.nameMapping = nameMapping;
  }

  private ReadConf conf = null;

  private ReadConf init() {
    if (conf == null) {
      ReadConf readConf =
          new ReadConf(
              input,
              options,
              expectedSchema,
              filter,
              null,
              batchReaderFunc,
              nameMapping,
              reuseContainers,
              caseSensitive,
              batchSize);
      this.conf = readConf.copy();
      return readConf;
    }
    return conf;
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init());
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final ParquetFileReader reader;
    private final boolean[] shouldSkip;
    private final VectorizedReader<T> model;
    private final long totalValues;
    private final int batchSize;
    private final List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetadata;
    private final boolean reuseContainers;
    private final int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;
    private final long[] rowGroupsStartRowPos;
    private final int totalRowGroups;

    // state effected when next row group is read is
    // model, nextRowGroup, nextRowGroupStart
    private static final ExecutorService prefetchService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor)
                Executors.newFixedThreadPool(
                    4,
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("iceberg-parquet-row-group-prefetchNext-pool-%d")
                        .build()));

    FileIterator(ReadConf conf) {
      this.reader = conf.reader();
      this.shouldSkip = conf.shouldSkip();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
      this.model = conf.vectorizedModel();
      this.batchSize = conf.batchSize();
      this.model.setBatchSize(this.batchSize);
      this.columnChunkMetadata = conf.columnChunkMetadataForRowGroups();
      this.rowGroupsStartRowPos = conf.startRowPositions();
      this.totalRowGroups = shouldSkip.length;
      prefetchNextRowGroup();
      advance();
    }

    // State associated with prefetching row groups
    private int prefetchedRowGroup = 0;
    private Future<PageReadStore> prefetchRowGroupFuture;

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      // batchSize is an integer, so casting to integer is safe
      int numValuesToRead = (int) Math.min(nextRowGroupStart - valuesRead, batchSize);
      if (reuseContainers) {
        this.last = model.read(last, numValuesToRead);
      } else {
        this.last = model.read(null, numValuesToRead);
      }
      valuesRead += numValuesToRead;

      return last;
    }

    private void advance() {
      try {
        Preconditions.checkNotNull(prefetchRowGroupFuture, "future should not be null");
        PageReadStore pages = prefetchRowGroupFuture.get();
        // no row groups pre-fetched
        if (prefetchedRowGroup >= totalRowGroups) {
          return;
        }
        Preconditions.checkState(
            pages != null,
            "advance() should have been only when there was at least one row group to read");
        long rowPosition = rowGroupsStartRowPos[prefetchedRowGroup];
        model.setRowGroupInfo(pages, columnChunkMetadata.get(prefetchedRowGroup), rowPosition);
        nextRowGroupStart += pages.getRowCount();
        prefetchedRowGroup += 1;
        prefetchNextRowGroup(); // eagerly fetch the next row group
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private void prefetchNextRowGroup() {
      prefetchRowGroupFuture =
          prefetchService.submit(
              () -> {
                while (prefetchedRowGroup < totalRowGroups && shouldSkip[prefetchedRowGroup]) {
                  prefetchedRowGroup += 1;
                  reader.skipNextRowGroup();
                }
                try {
                  if (prefetchedRowGroup < totalRowGroups) {
                    PageReadStore pageReadStore = reader.readNextRowGroup();
                    return pageReadStore;
                  }
                  return null;
                } catch (IOException e) {
                  throw new RuntimeIOException(e);
                }
              });
    }

    @Override
    public void close() throws IOException {
      model.close();
      reader.close();
    }
  }
}
