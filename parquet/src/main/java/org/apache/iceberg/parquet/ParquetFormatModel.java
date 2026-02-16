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
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;

public class ParquetFormatModel<D, S, R>
    extends BaseFormatModel<D, S, ParquetValueWriter<?>, R, MessageType> {
  public static final String WRITER_VERSION_KEY = "parquet.writer.version";
  private final boolean isBatchReader;

  public static <D> ParquetFormatModel<PositionDelete<D>, Void, Object> forPositionDeletes() {
    return new ParquetFormatModel<>(PositionDelete.deleteClass(), Void.class, null, null, false);
  }

  public static <D, S> ParquetFormatModel<D, S, ParquetValueReader<?>> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<ParquetValueWriter<?>, S, MessageType> writerFunction,
      ReaderFunction<ParquetValueReader<?>, S, MessageType> readerFunction) {
    return new ParquetFormatModel<>(type, schemaType, writerFunction, readerFunction, false);
  }

  public static <D, S> ParquetFormatModel<D, S, VectorizedReader<?>> create(
      Class<? extends D> type,
      Class<S> schemaType,
      ReaderFunction<VectorizedReader<?>, S, MessageType> batchReaderFunction) {
    return new ParquetFormatModel<>(type, schemaType, null, batchReaderFunction, true);
  }

  private ParquetFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<ParquetValueWriter<?>, S, MessageType> writerFunction,
      ReaderFunction<R, S, MessageType> readerFunction,
      boolean isBatchReader) {
    super(type, schemaType, writerFunction, readerFunction);
    this.isBatchReader = isBatchReader;
  }

  @Override
  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction());
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction(), isBatchReader);
  }

  private static class WriteBuilderWrapper<D, S> implements ModelWriteBuilder<D, S> {
    private final Parquet.WriteBuilder internal;
    private final WriterFunction<ParquetValueWriter<?>, S, MessageType> writerFunction;
    private Schema schema;
    private S engineSchema;
    private FileContent content;

    private WriteBuilderWrapper(
        EncryptedOutputFile outputFile,
        WriterFunction<ParquetValueWriter<?>, S, MessageType> writerFunction) {
      this.internal = Parquet.write(outputFile);
      this.writerFunction = writerFunction;
    }

    @Override
    public ModelWriteBuilder<D, S> schema(Schema newSchema) {
      this.schema = newSchema;
      internal.schema(newSchema);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> engineSchema(S newSchema) {
      this.engineSchema = newSchema;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> set(String property, String value) {
      if (WRITER_VERSION_KEY.equals(property)) {
        internal.writerVersion(ParquetProperties.WriterVersion.valueOf(value));
      }

      internal.set(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> setAll(Map<String, String> properties) {
      internal.setAll(properties);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(String property, String value) {
      internal.meta(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(Map<String, String> properties) {
      internal.meta(properties);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> content(FileContent newContent) {
      this.content = newContent;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> metricsConfig(MetricsConfig metricsConfig) {
      internal.metricsConfig(metricsConfig);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> overwrite() {
      internal.overwrite();
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withFileEncryptionKey(ByteBuffer encryptionKey) {
      internal.withFileEncryptionKey(encryptionKey);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
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
                  writerFunction.write(icebergSchema, messageType, engineSchema));
          break;
        case EQUALITY_DELETES:
          internal.createContextFunc(Parquet.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              (icebergSchema, messageType) ->
                  writerFunction.write(icebergSchema, messageType, engineSchema));
          break;
        case POSITION_DELETES:
          Preconditions.checkState(
              schema == null,
              "Invalid schema: %s. Position deletes with schema are not supported by the API.",
              schema);
          Preconditions.checkState(
              engineSchema == null,
              "Invalid engineSchema: %s. Position deletes with schema are not supported by the API.",
              engineSchema);

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

  private static class ReadBuilderWrapper<R, D, S> implements ReadBuilder<D, S> {
    private final Parquet.ReadBuilder internal;
    private final ReaderFunction<R, S, MessageType> readerFunction;
    private final boolean isBatchReader;
    private S engineSchema;
    private Map<Integer, ?> idToConstant = ImmutableMap.of();

    private ReadBuilderWrapper(
        InputFile inputFile,
        ReaderFunction<R, S, MessageType> readerFunction,
        boolean isBatchReader) {
      this.internal = Parquet.read(inputFile);
      this.readerFunction = readerFunction;
      this.isBatchReader = isBatchReader;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      internal.split(newStart, newLength);
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema schema) {
      internal.project(schema);
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S schema) {
      this.engineSchema = schema;
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
      if (isBatchReader) {
        return internal
            .createBatchedReaderFunc(
                (icebergSchema, messageType) ->
                    (VectorizedReader<?>)
                        readerFunction.read(icebergSchema, messageType, engineSchema, idToConstant))
            .build();
      } else {
        return internal
            .createReaderFunc(
                (icebergSchema, messageType) ->
                    (ParquetValueReader<?>)
                        readerFunction.read(icebergSchema, messageType, engineSchema, idToConstant))
            .build();
      }
    }
  }
}
