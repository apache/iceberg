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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

public class CometVectorizedParquetReader<T> extends CloseableGroup
    implements CloseableIterable<T> {
  private final InputFile input;
  private final ParquetReadOptions options;
  private final Schema expectedSchema;
  private final Function<MessageType, VectorizedReader<?>> batchReaderFunc;
  private final Expression filter;
  private final boolean reuseContainers;
  private final boolean caseSensitive;
  private final int batchSize;
  private final NameMapping nameMapping;
  private final Map<String, String> properties;
  private Long start = null;
  private Long length = null;
  private ByteBuffer fileEncryptionKey = null;
  private ByteBuffer fileAADPrefix = null;

  public CometVectorizedParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch,
      Map<String, String> properties,
      Long start,
      Long length,
      ByteBuffer fileEncryptionKey,
      ByteBuffer fileAADPrefix) {
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
    this.properties = properties;
    this.start = start;
    this.length = length;
    this.fileEncryptionKey = fileEncryptionKey;
    this.fileAADPrefix = fileAADPrefix;
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
    FileIterator<T> iter =
        new FileIterator<>(init(), properties, start, length, fileEncryptionKey, fileAADPrefix);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final boolean[] shouldSkip;
    private final VectorizedReader<T> model;
    private final long totalValues;
    private final int batchSize;
    private final List<Map<ColumnPath, ColumnChunkMetaData>> columnChunkMetadata;
    private final boolean reuseContainers;
    private int nextRowGroup = 0;
    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;
    private final CometBridge.FileReaderWrapper cometReader;

    FileIterator(
        ReadConf conf,
        Map<String, String> properties,
        Long start,
        Long length,
        ByteBuffer fileEncryptionKey,
        ByteBuffer fileAADPrefix) {
      if (!CometBridge.isCometAvailable()) {
        throw new IllegalStateException(
            "Comet is not available in the classpath. "
                + "Please ensure the comet-spark jar is in the classpath.");
      }

      this.shouldSkip = conf.shouldSkip();
      this.totalValues = conf.totalValues();
      this.reuseContainers = conf.reuseContainers();
      this.model = conf.vectorizedModel();
      this.batchSize = conf.batchSize();
      this.model.setBatchSize(this.batchSize);
      this.columnChunkMetadata = conf.columnChunkMetadataForRowGroups();
      this.cometReader =
          newCometReader(
              conf.file(),
              conf.projection(),
              properties,
              start,
              length,
              fileEncryptionKey,
              fileAADPrefix);
    }

    private CometBridge.FileReaderWrapper newCometReader(
        InputFile file,
        MessageType projection,
        Map<String, String> properties,
        Long start,
        Long length,
        ByteBuffer fileEncryptionKey,
        ByteBuffer fileAADPrefix) {
      CometBridge.FileReaderWrapper fileReader = null;
      try {
        Configuration conf;
        if (file instanceof HadoopInputFile) {
          conf = new Configuration(((HadoopInputFile) file).getConf());
        } else {
          conf = new Configuration();
        }
        Object cometOptions = CometBridge.createReadOptions(conf);

        fileReader =
            CometBridge.FileReaderWrapper.create(
                file,
                cometOptions,
                properties,
                start,
                length,
                ByteBuffers.toByteArray(fileEncryptionKey),
                ByteBuffers.toByteArray(fileAADPrefix));

        List<ColumnDescriptor> columnDescriptors = projection.getColumns();

        List<Object> specs = Lists.newArrayList();

        for (ColumnDescriptor descriptor : columnDescriptors) {
          Object spec = CometBridge.createParquetColumnSpec(descriptor);
          specs.add(spec);
        }

        fileReader.setRequestedSchemaFromSpecs(specs);
        return fileReader;
      } catch (Exception e) {
        // Clean up the fileReader if it was created but configuration failed
        if (fileReader != null) {
          try {
            fileReader.close();
          } catch (Exception closeException) {
            // Log the close exception but don't mask the original exception
            e.addSuppressed(closeException);
          }
        }
        throw CometIOException.fromException("Failed to open Parquet file: " + file.location(), e);
      }
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
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        try {
          cometReader.skipNextRowGroup();
        } catch (Exception e) {
          throw CometIOException.fromException("Failed to skip row group", e);
        }
      }
      CometBridge.RowGroupReaderWrapper pages;
      try {
        pages = cometReader.readNextRowGroup();
      } catch (Exception e) {
        throw CometIOException.fromException("Failed to read row group", e);
      }

      try {
        CometPageReadStore pageReadStore = new CometPageReadStore(pages.getRowGroupReader());
        model.setRowGroupInfo(pageReadStore, columnChunkMetadata.get(nextRowGroup));
      } catch (Exception e) {
        throw CometIOException.fromException("Failed to read row group info", e);
      }
      try {
        nextRowGroupStart += pages.getRowCount();
      } catch (Exception e) {
        throw CometIOException.fromException("Failed to get row count", e);
      }
      nextRowGroup += 1;
    }

    @Override
    public void close() throws IOException {
      model.close();
      try {
        cometReader.close();
      } catch (Exception e) {
        throw new IOException("Failed to close Comet reader", e);
      }
    }
  }
}
