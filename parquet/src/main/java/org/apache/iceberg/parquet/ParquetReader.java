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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import static org.apache.iceberg.parquet.ParquetSchemaUtil.addFallbackIds;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.hasIds;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.pruneColumns;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.pruneColumnsFallback;

public class ParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, ParquetValueReader<?>> readerFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;

  public ParquetReader(InputFile input, Schema expectedSchema, ParquetReadOptions options,
                       Function<MessageType, ParquetValueReader<?>> readerFunc,
                       Expression filter, boolean reuseContainers, boolean caseSensitive) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.readerFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
  }

  private static class ReadConf<T> {
    private final ParquetFileReader reader;
    private final InputFile file;
    private final ParquetReadOptions options;
    private final MessageType projection;
    private final ParquetValueReader<T> model;
    private final List<BlockMetaData> rowGroups;
    private final boolean[] shouldSkip;
    private final long totalValues;
    private final boolean reuseContainers;

    @SuppressWarnings("unchecked")
    ReadConf(InputFile file, ParquetReadOptions options, Schema expectedSchema, Expression filter,
             Function<MessageType, ParquetValueReader<?>> readerFunc, boolean reuseContainers,
             boolean caseSensitive) {
      this.file = file;
      this.options = options;
      this.reader = newReader(file, options);

      MessageType fileSchema = reader.getFileMetaData().getSchema();

      boolean hasIds = hasIds(fileSchema);
      MessageType typeWithIds = hasIds ? fileSchema : addFallbackIds(fileSchema);

      this.projection = hasIds ?
          pruneColumns(fileSchema, expectedSchema) :
          pruneColumnsFallback(fileSchema, expectedSchema);
      this.model = (ParquetValueReader<T>) readerFunc.apply(typeWithIds);
      this.rowGroups = reader.getRowGroups();
      this.shouldSkip = new boolean[rowGroups.size()];

      ParquetMetricsRowGroupFilter statsFilter = null;
      ParquetDictionaryRowGroupFilter dictFilter = null;
      if (filter != null) {
        statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
        dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
      }

      long totalValues = 0L;
      for (int i = 0; i < shouldSkip.length; i += 1) {
        BlockMetaData rowGroup = rowGroups.get(i);
        boolean shouldRead = filter == null || (
            statsFilter.shouldRead(typeWithIds, rowGroup) &&
            dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)));
        this.shouldSkip[i] = !shouldRead;
        if (shouldRead) {
          totalValues += rowGroup.getRowCount();
        }
      }

      this.totalValues = totalValues;
      this.reuseContainers = reuseContainers;
    }

    ReadConf(ReadConf<T> toCopy) {
      this.reader = null;
      this.file = toCopy.file;
      this.options = toCopy.options;
      this.projection = toCopy.projection;
      this.model = toCopy.model;
      this.rowGroups = toCopy.rowGroups;
      this.shouldSkip = toCopy.shouldSkip;
      this.totalValues = toCopy.totalValues;
      this.reuseContainers = toCopy.reuseContainers;
    }

    ParquetFileReader reader() {
      if (reader != null) {
        reader.setRequestedSchema(projection);
        return reader;
      }

      ParquetFileReader newReader = newReader(file, options);
      newReader.setRequestedSchema(projection);
      return newReader;
    }

    ParquetValueReader<T> model() {
      return model;
    }

    boolean[] shouldSkip() {
      return shouldSkip;
    }

    long totalValues() {
      return totalValues;
    }

    boolean reuseContainers() {
      return reuseContainers;
    }

    ReadConf<T> copy() {
      return new ReadConf<>(this);
    }

    private static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
      try {
        return ParquetFileReader.open(ParquetIO.file(file), options);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to open Parquet file: %s", file.location());
      }
    }
  }

  private ReadConf<T> conf = null;

  private ReadConf<T> init() {
    if (conf == null) {
      ReadConf<T> conf = new ReadConf<>(
          input, options, expectedSchema, filter, readerFunc, reuseContainers, caseSensitive);
      this.conf = conf.copy();
      return conf;
    }

    return conf;
  }

  @Override
  public Iterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init());
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements Iterator<T>, Closeable {
    private final ParquetFileReader reader;
    private final boolean[] shouldSkip;
    private final ParquetValueReader<T> model;
    private final long totalValues;
    private final boolean reuseContainers;

    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    FileIterator(ReadConf<T> conf) {
      this.reader = conf.reader();
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
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      if (reuseContainers) {
        this.last = model.read(last);
      } else {
        this.last = model.read(null);
      }
      valuesRead += 1;

      return last;
    }

    private void advance() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        reader.skipNextRowGroup();
      }

      PageReadStore pages;
      try {
        pages = reader.readNextRowGroup();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;

      model.setPageSource(pages);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
