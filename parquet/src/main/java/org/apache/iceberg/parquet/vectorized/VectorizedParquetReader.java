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

package org.apache.iceberg.parquet.vectorized;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetIO;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

public class VectorizedParquetReader<T> extends CloseableGroup implements CloseableIterable<T> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Function<MessageType, VectorizedReader<T>> batchReaderFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final int batchSize;

  public VectorizedParquetReader(
      InputFile input, Schema expectedSchema, ParquetReadOptions options,
      Function<MessageType, VectorizedReader<T>> readerFunc,
      Expression filter, boolean reuseContainers, boolean caseSensitive, int maxRecordsPerBatch) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.batchReaderFunc = readerFunc;
    // replace alwaysTrue with null to avoid extra work evaluating a trivial filter
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.batchSize = maxRecordsPerBatch;
  }

  private static class ReadConf<T> {
    private final ParquetFileReader reader;
    private final InputFile file;
    private final ParquetReadOptions options;
    private final MessageType projection;
    private final VectorizedReader<T> model;
    private final List<BlockMetaData> rowGroups;
    private final boolean[] shouldSkip;
    private final long totalValues;
    private final boolean reuseContainers;
    private final int batchSize;

    @SuppressWarnings("unchecked")
    ReadConf(InputFile file, ParquetReadOptions options, Schema expectedSchema, Expression filter,
        Function<MessageType, VectorizedReader<T>> readerFunc, boolean reuseContainers,
        boolean caseSensitive, int bSize) {
      this.file = file;
      this.options = options;
      this.reader = newReader(file, options);

      MessageType fileSchema = reader.getFileMetaData().getSchema();

      boolean hasIds = ParquetSchemaUtil.hasIds(fileSchema);
      MessageType typeWithIds = hasIds ? fileSchema : ParquetSchemaUtil.addFallbackIds(fileSchema);

      this.projection = hasIds ?
          ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema) :
          ParquetSchemaUtil.pruneColumnsFallback(fileSchema, expectedSchema);
      this.model = readerFunc.apply(typeWithIds);
      this.rowGroups = reader.getRowGroups();
      this.shouldSkip = new boolean[rowGroups.size()];

      ParquetMetricsRowGroupFilter statsFilter = null;
      ParquetDictionaryRowGroupFilter dictFilter = null;
      if (filter != null) {
        statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
        dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
      }

      long computedTotalValues = 0L;
      for (int i = 0; i < shouldSkip.length; i += 1) {
        BlockMetaData rowGroup = rowGroups.get(i);
        boolean shouldRead = filter == null || (
            statsFilter.shouldRead(typeWithIds, rowGroup) &&
                dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)));
        this.shouldSkip[i] = !shouldRead;
        if (shouldRead) {
          computedTotalValues += rowGroup.getRowCount();
        }
      }

      this.totalValues = computedTotalValues;
      this.reuseContainers = reuseContainers;
      this.batchSize = bSize;
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
      this.batchSize = toCopy.batchSize;
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

    VectorizedReader model() {
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

    int batchSize() {
      return batchSize;
    }

    private static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
      try {
        return ParquetFileReader.open(ParquetIO.file(file), options);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to open Parquet file: %s", file.location());
      }
    }

    ReadConf<T> copy() {
      return new ReadConf<>(this);
    }
  }

  private ReadConf conf = null;

  private ReadConf init() {
    if (conf == null) {
      ReadConf readConf = new ReadConf(
          input, options, expectedSchema, filter, batchReaderFunc, reuseContainers, caseSensitive, batchSize);
      this.conf = readConf.copy();
      return readConf;
    }

    return conf;
  }

  @Override
  public Iterator iterator() {
    FileIterator iter = new FileIterator(init());
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements Iterator<T>, Closeable {
    private final ParquetFileReader reader;
    private final boolean[] shouldSkip;
    private final VectorizedReader<T> model;
    private final long totalValues;
    private final int batchSize;

    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    FileIterator(ReadConf conf) {
      this.reader = conf.reader();
      this.shouldSkip = conf.shouldSkip();
      this.model = conf.model();
      this.totalValues = conf.totalValues();
      this.model.reuseContainers(conf.reuseContainers());
      this.batchSize = conf.batchSize();
    }

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
      this.last = model.read();
      valuesRead += Math.min(nextRowGroupStart - valuesRead, batchSize);
      return last;
    }

    private void advance() {
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        reader.skipNextRowGroup();
      }

      PageReadStore pages;
      DictionaryPageReadStore dictionaryPageReadStore;
      try {
        dictionaryPageReadStore = reader.getNextDictionaryReader();
        pages = reader.readNextRowGroup();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;
      model.setRowGroupInfo(
          pages,
          dictionaryPageReadStore,
          dictionaryPageReadStore == null ? null : buildColumnDictEncodedMap(reader.getRowGroups()));
    }

    /**
     * Retuns a map of {@link ColumnPath} -> whether all the pages in the row group for this column are dictionary
     * encoded
     */
    private static Map<ColumnPath, Boolean> buildColumnDictEncodedMap(List<BlockMetaData> blockMetaData) {
      Map<ColumnPath, Boolean> map = new HashMap<>();
      for (BlockMetaData b : blockMetaData) {
        for (ColumnChunkMetaData c : b.getColumns()) {
          map.put(c.getPath(), !ParquetUtil.hasNonDictionaryPages(c));
        }
      }
      return map;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
