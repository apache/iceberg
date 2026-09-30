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
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, ParquetValueReader<?>> readerFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;

  public ParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, ParquetValueReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.readerFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.nameMapping = nameMapping;
  }

  private ReadConf<T> conf = null;

  private ReadConf<T> init() {
    if (conf == null) {
      ReadConf<T> readConf =
          new ReadConf<>(
              input,
              options,
              expectedSchema,
              filter,
              readerFunc,
              null,
              nameMapping,
              reuseContainers,
              caseSensitive,
              null);
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

  private static class FileIterator<T> implements SkippingCloseableIterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(FileIterator.class);

    private final ParquetFileReader reader;
    private final List<BlockMetaData> rowGroups;
    private final boolean[] shouldSkip;
    private final ParquetValueReader<T> model;
    private final long totalValues;
    private final boolean reuseContainers;

    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private long nextPosition = 0;
    private T last = null;

    FileIterator(ReadConf<T> conf) {
      this.reader = conf.reader();
      this.rowGroups = conf.rowGroups();
      this.shouldSkip = conf.shouldSkip();
      this.model = conf.model();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
    }

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      try {
        if (valuesRead >= nextRowGroupStart) {
          advance();
        }

        return read();
      } catch (ParquetDecodingException e) {
        if (reader != null) {
          // Knowing the exact parquet file is essential for tracing bad nodes
          // that produced the corrupt file, parquet lib doesn't do this today.
          LOG.error("Error decoding Parquet file {}", reader.getFile(), e);
        }

        throw e;
      }
    }

    @Override
    public long position() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more rows");
      }

      if (valuesRead >= nextRowGroupStart) {
        return rowIndexOffset(rowGroups.get(nextReadableRowGroup()));
      }

      rowIndexOffset(rowGroups.get(nextRowGroup - 1));
      return nextPosition;
    }

    @Override
    public void advanceTo(long target) {
      while (hasNext() && position() < target) {
        if (valuesRead >= nextRowGroupStart) {
          skipFilteredRowGroups();
          BlockMetaData rowGroup = rowGroups.get(nextRowGroup);
          if (rowGroup.getRowIndexOffset() + rowGroup.getRowCount() <= target) {
            discardRowGroup();
            continue;
          }

          advance();
        }

        read();
      }
    }

    private T read() {
      if (reuseContainers) {
        this.last = model.read(last);
      } else {
        this.last = model.read(null);
      }

      valuesRead += 1;
      nextPosition += 1;

      return last;
    }

    private int nextReadableRowGroup() {
      int rowGroup = nextRowGroup;
      while (shouldSkip[rowGroup]) {
        rowGroup += 1;
      }

      return rowGroup;
    }

    private void skipFilteredRowGroups() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        reader.skipNextRowGroup();
      }
    }

    private void discardRowGroup() {
      long rowCount = rowGroups.get(nextRowGroup).getRowCount();
      reader.skipNextRowGroup();
      nextRowGroup += 1;
      nextRowGroupStart += rowCount;
      // discarded rows count as consumed so hasNext() reaches totalValues
      valuesRead += rowCount;
    }

    private void advance() {
      skipFilteredRowGroups();

      PageReadStore pages;
      try {
        pages = reader.readNextRowGroup();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      nextPosition = rowGroups.get(nextRowGroup).getRowIndexOffset();
      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;

      model.setPageSource(pages);
    }

    private long rowIndexOffset(BlockMetaData rowGroup) {
      long offset = rowGroup.getRowIndexOffset();
      Preconditions.checkState(
          offset >= 0,
          "Cannot find row positions: missing row index offsets in %s",
          reader.getFile());
      return offset;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
