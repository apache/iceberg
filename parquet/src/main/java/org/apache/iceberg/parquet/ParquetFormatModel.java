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
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;

public class ParquetFormatModel<D, S> implements FormatModel<D, S> {
  public static final String WRITER_VERSION_KEY = "parquet.writer.version";

  private final Class<D> type;
  private final Class<S> schemaType;
  private final ReaderFunction<D> readerFunction;
  private final BatchReaderFunction<D> batchReaderFunction;
  private final WriterFunction<S> writerFunction;

  private ParquetFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      BatchReaderFunction<D> batchReaderFunction,
      WriterFunction<S> writerFunction) {
    this.type = type;
    this.schemaType = schemaType;
    this.readerFunction = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.writerFunction = writerFunction;
  }

  public ParquetFormatModel(Class<D> type) {
    this(type, null, null, null);
  }

  public ParquetFormatModel(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<D> readerFunction,
      WriterFunction<S> writerFunction) {
    this(type, schemaType, readerFunction, null, writerFunction);
  }

  public ParquetFormatModel(
      Class<D> type, Class<S> schemaType, BatchReaderFunction<D> batchReaderFunction) {
    this(type, schemaType, null, batchReaderFunction, null);
  }

  @Override
  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Override
  public Class<D> type() {
    return type;
  }

  @Override
  public Class<S> schemaType() {
    return schemaType;
  }

  @Override
  public WriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction);
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction, batchReaderFunction);
  }

  @FunctionalInterface
  public interface ReaderFunction<D> {
    ParquetValueReader<D> read(
        Schema schema, MessageType messageType, Map<Integer, ?> constantValues);
  }

  @FunctionalInterface
  public interface BatchReaderFunction<D> {
    VectorizedReader<D> read(
        Schema schema,
        MessageType messageType,
        Map<Integer, ?> constantValues,
        Map<String, String> config);
  }

  @FunctionalInterface
  public interface WriterFunction<S> {
    ParquetValueWriter<?> write(Schema icebergSchema, MessageType messageType, S engineSchema);
  }

  private static class WriteBuilderWrapper<D, S> implements WriteBuilder<D, S> {
    private final Parquet.WriteBuilder internal;
    private final WriterFunction<S> writerFunction;
    private S inputSchema;
    private FileContent content;

    private WriteBuilderWrapper(EncryptedOutputFile outputFile, WriterFunction<S> writerFunction) {
      this.internal = Parquet.write(outputFile);
      this.writerFunction = writerFunction;
    }

    @Override
    public WriteBuilder<D, S> schema(Schema schema) {
      internal.schema(schema);
      return this;
    }

    @Override
    public WriteBuilder<D, S> inputSchema(S schema) {
      this.inputSchema = schema;
      return this;
    }

    @Override
    public WriteBuilder<D, S> set(String property, String value) {
      if (WRITER_VERSION_KEY.equals(property)) {
        internal.writerVersion(ParquetProperties.WriterVersion.valueOf(value));
      }

      internal.set(property, value);
      return this;
    }

    @Override
    public WriteBuilder<D, S> setAll(Map<String, String> properties) {
      internal.setAll(properties);
      return this;
    }

    @Override
    public WriteBuilder<D, S> meta(String property, String value) {
      internal.meta(property, value);
      return this;
    }

    @Override
    public WriteBuilder<D, S> meta(Map<String, String> properties) {
      internal.meta(properties);
      return this;
    }

    @Override
    public WriteBuilder<D, S> content(FileContent newContent) {
      this.content = newContent;
      return this;
    }

    @Override
    public WriteBuilder<D, S> metricsConfig(MetricsConfig metricsConfig) {
      internal.metricsConfig(metricsConfig);
      return this;
    }

    @Override
    public WriteBuilder<D, S> overwrite() {
      internal.overwrite();
      return this;
    }

    @Override
    public WriteBuilder<D, S> withFileEncryptionKey(ByteBuffer encryptionKey) {
      internal.withFileEncryptionKey(encryptionKey);
      return this;
    }

    @Override
    public WriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      internal.withAADPrefix(aadPrefix);
      return this;
    }

    @Override
    public FileAppender<D> build() throws IOException {
      switch (content) {
        case DATA:
          internal.createContextFunc(Parquet.WriteBuilder.Context::dataContext);
          internal.createWriterFunc(
              (icebergSchema, messageType) ->
                  writerFunction.write(icebergSchema, messageType, inputSchema));
          break;
        case EQUALITY_DELETES:
          internal.createContextFunc(Parquet.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              (icebergSchema, messageType) ->
                  writerFunction.write(icebergSchema, messageType, inputSchema));
          break;
        case POSITION_DELETES:
          internal.createContextFunc(Parquet.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              (icebergSchema, messageType) ->
                  new ParquetValueWriters.PositionDeleteStructWriter<D>(
                      (ParquetValueWriters.StructWriter<?>)
                          GenericParquetWriter.create(icebergSchema, messageType),
                      Function.identity()));
          internal.schema(DeleteSchemaUtil.pathPosSchema());
          break;
        default:
          throw new IllegalArgumentException("Unknown file content: " + content);
      }

      return internal.build();
    }
  }

  private static class ReadBuilderWrapper<D, S> implements ReadBuilder<D, S> {
    private final Parquet.ReadBuilder internal;
    private final ReaderFunction<D> readerFunction;
    private final BatchReaderFunction<D> batchReaderFunction;
    private final Map<String, String> config = Maps.newHashMap();
    private Schema icebergSchema;
    private Map<Integer, ?> idToConstant = ImmutableMap.of();

    private ReadBuilderWrapper(
        InputFile inputFile,
        ReaderFunction<D> readerFunction,
        BatchReaderFunction<D> batchReaderFunction) {
      this.internal = Parquet.read(inputFile);
      this.readerFunction = readerFunction;
      this.batchReaderFunction = batchReaderFunction;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      internal.split(newStart, newLength);
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema schema) {
      this.icebergSchema = schema;
      internal.project(schema);
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      internal.caseSensitive(caseSensitive);
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      internal.filter(filter);
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      this.config.put(key, value);
      internal.set(key, value);
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      internal.reuseContainers();
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      internal.recordsPerBatch(numRowsPerBatch);
      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      internal.withNameMapping(nameMapping);
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      if (readerFunction != null) {
        return internal
            .createReaderFunc(
                messageType -> readerFunction.read(icebergSchema, messageType, idToConstant))
            .build();
      } else if (batchReaderFunction != null) {
        return internal
            .createBatchedReaderFunc(
                messageType ->
                    batchReaderFunction.read(icebergSchema, messageType, idToConstant, config))
            .build();
      } else {
        throw new IllegalStateException("Either readerFunction or batchReaderFunction must be set");
      }
    }
  }
}
