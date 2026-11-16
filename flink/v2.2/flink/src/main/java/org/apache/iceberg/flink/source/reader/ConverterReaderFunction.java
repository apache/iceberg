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

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
public class ConverterReaderFunction<T> extends DataIteratorReaderFunction<T> {
  private final RowDataConverter<T> converter;
  private final Schema tableSchema;
  private final Schema readSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final List<Expression> filters;
  private final long limit;

  private transient RecordLimiter recordLimiter = null;

  public ConverterReaderFunction(
      RowDataConverter<T> converter,
      ReadableConfig config,
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters,
      long limit) {
    super(new ListDataIteratorBatcher<>(config));
    this.converter = converter;
    this.tableSchema = tableSchema;
    this.readSchema = readSchema(tableSchema, projectedSchema);
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryption = encryption;
    this.filters = filters;
    this.limit = limit;
  }

  @Override
  protected DataIterator<T> createDataIterator(IcebergSourceSplit split) {
    RowDataFileScanTaskReader rowDataReader =
        new RowDataFileScanTaskReader(tableSchema, readSchema, nameMapping, caseSensitive, filters);
    return new LimitableDataIterator<>(
        new ConverterFileScanTaskReader<>(rowDataReader, converter),
        split.task(),
        io,
        encryption,
        lazyLimiter());
  }

  private static Schema readSchema(Schema tableSchema, Schema projectedSchema) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    return projectedSchema == null ? tableSchema : projectedSchema;
  }

  /** Lazily create RecordLimiter to avoid the need to make it serializable */
  private RecordLimiter lazyLimiter() {
    if (recordLimiter == null) {
      this.recordLimiter = RecordLimiter.create(limit);
    }

    return recordLimiter;
  }

  private static class ConverterFileScanTaskReader<T> implements FileScanTaskReader<T> {
    private final RowDataFileScanTaskReader rowDataReader;
    private final RowDataConverter<T> converter;

    ConverterFileScanTaskReader(
        RowDataFileScanTaskReader rowDataReader, RowDataConverter<T> converter) {
      this.rowDataReader = rowDataReader;
      this.converter = converter;
    }

    @Override
    public CloseableIterator<T> open(
        FileScanTask fileScanTask, InputFilesDecryptor inputFilesDecryptor) {
      return CloseableIterator.transform(
          rowDataReader.open(fileScanTask, inputFilesDecryptor), converter);
    }
  }
}
