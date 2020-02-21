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

  public VectorizedParquetReader(
      InputFile input, Schema expectedSchema, ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
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

  private ReadConf conf = null;

  private ReadConf init() {
    if (conf == null) {
      ReadConf readConf = new ReadConf(
          input, options, expectedSchema, filter, null, batchReaderFunc, reuseContainers, caseSensitive, batchSize);
      this.conf = readConf.copy();
      return readConf;
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
    private final VectorizedReader<T> model;
    private final long totalValues;
    private final int batchSize;
    private final List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetadata;
    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    FileIterator(ReadConf conf) {
      this.reader = conf.reader();
      this.shouldSkip = conf.shouldSkip();
      this.model = conf.vectorizedModel();
      this.totalValues = conf.totalValues();
      this.model.reuseContainers(conf.reuseContainers());
      this.batchSize = conf.batchSize();
      this.columnChunkMetadata = conf.columnChunkMetadataForRowGroups();
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
      long numValuesToRead = Math.min(nextRowGroupStart - valuesRead, batchSize);
      // batchSize is an integer, so casting to integer is safe
      this.last = model.read((int) numValuesToRead);
      valuesRead += numValuesToRead;
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
      model.setRowGroupInfo(pages, columnChunkMetadata.get(nextRowGroup));
      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;
    }

    @Override
    public void close() throws IOException {
      model.close();
      reader.close();
    }
  }
}
