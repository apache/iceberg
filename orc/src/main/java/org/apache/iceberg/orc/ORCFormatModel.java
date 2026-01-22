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
package org.apache.iceberg.orc;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.orc.TypeDescription;

public class ORCFormatModel<D, S, R>
    extends BaseFormatModel<D, S, OrcRowWriter<?>, R, TypeDescription> {
  private final boolean batchReader;

  public static ORCFormatModel<PositionDelete, Object, Object> forDelete() {
    return new ORCFormatModel<>(PositionDelete.class, null, null, null, false);
  }

  public static <D, S> ORCFormatModel<D, S, OrcRowReader<D>> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<OrcRowWriter<?>, S, TypeDescription> writerFunction,
      ReaderFunction<OrcRowReader<D>, S, TypeDescription> readerFunction) {
    return new ORCFormatModel<>(type, schemaType, writerFunction, readerFunction, false);
  }

  public static <D, S> ORCFormatModel<D, S, OrcBatchReader<?>> create(
      Class<D> type,
      Class<S> schemaType,
      ReaderFunction<OrcBatchReader<?>, S, TypeDescription> batchReaderFunction) {
    return new ORCFormatModel<>(type, schemaType, null, batchReaderFunction, true);
  }

  private ORCFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<OrcRowWriter<?>, S, TypeDescription> writerFunction,
      ReaderFunction<R, S, TypeDescription> readerFunction,
      boolean batchReader) {
    super(type, schemaType, writerFunction, readerFunction);
    this.batchReader = batchReader;
  }

  @Override
  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public WriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction());
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction(), batchReader);
  }

  private static class WriteBuilderWrapper<D, S> implements WriteBuilder<D, S> {
    private final ORC.WriteBuilder internal;
    private final WriterFunction<OrcRowWriter<?>, S, TypeDescription> writerFunction;
    private S inputSchema;

    private FileContent content;

    private WriteBuilderWrapper(
        EncryptedOutputFile outputFile,
        WriterFunction<OrcRowWriter<?>, S, TypeDescription> writerFunction) {
      this.internal = ORC.write(outputFile);
      this.writerFunction = writerFunction;
    }

    @Override
    public WriteBuilder<D, S> schema(Schema schema) {
      internal.schema(schema);
      return this;
    }

    @Override
    public WriteBuilder<D, S> engineSchema(S schema) {
      this.inputSchema = schema;
      return this;
    }

    @Override
    public WriteBuilder<D, S> set(String property, String value) {
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
      internal.metadata(property, value);
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
      // ORC doesn't support file encryption
      throw new UnsupportedOperationException("ORC does not support file encryption keys");
    }

    @Override
    public WriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      // ORC doesn't support file encryption
      throw new UnsupportedOperationException("ORC does not support AAD prefix");
    }

    @Override
    public org.apache.iceberg.io.FileAppender<D> build() {
      switch (content) {
        case DATA:
          internal.createContextFunc(ORC.WriteBuilder.Context::dataContext);
          internal.createWriterFunc(
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, inputSchema));
          break;
        case EQUALITY_DELETES:
          internal.createContextFunc(ORC.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              (icebergSchema, typeDescription) ->
                  writerFunction.write(icebergSchema, typeDescription, inputSchema));
          break;
        case POSITION_DELETES:
          internal.createContextFunc(ORC.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              (icebergSchema, typeDescription) ->
                  GenericOrcWriters.positionDelete(
                      GenericOrcWriter.buildWriter(icebergSchema, typeDescription),
                      Function.identity()));
          internal.schema(DeleteSchemaUtil.pathPosSchema());
          break;
        default:
          throw new IllegalArgumentException("Unknown file content: " + content);
      }

      return internal.build();
    }
  }

  private static class ReadBuilderWrapper<D, S, R> implements ReadBuilder<D, S> {
    private final ORC.ReadBuilder internal;
    private final ReaderFunction<R, S, TypeDescription> readerFunction;
    private final boolean batchReader;
    private S engineSchema;
    private boolean reuseContainers = false;
    private Schema icebergSchema;
    private Map<Integer, ?> idToConstant = ImmutableMap.of();

    private ReadBuilderWrapper(
        InputFile inputFile,
        ReaderFunction<R, S, TypeDescription> readerFunction,
        boolean batchReader) {
      this.internal = ORC.read(inputFile);
      this.readerFunction = readerFunction;
      this.batchReader = batchReader;
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
      internal.config(key, value);
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      this.reuseContainers = true;
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      internal.recordsPerBatch(numRowsPerBatch);
      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      internal.constantFieldIds(newIdToConstant.keySet());
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      internal.withNameMapping(nameMapping);
      return this;
    }

    @Override
    public org.apache.iceberg.io.CloseableIterable<D> build() {
      Preconditions.checkNotNull(reuseContainers, "Reuse containers is required for ORC read");
      return batchReader
          ? internal
              .createBatchedReaderFunc(
                  typeDescription ->
                      (OrcBatchReader<?>)
                          readerFunction.read(
                              icebergSchema, typeDescription, engineSchema, idToConstant))
              .build()
          : internal
              .createReaderFunc(
                  typeDescription ->
                      (OrcRowReader<?>)
                          readerFunction.read(
                              icebergSchema, typeDescription, engineSchema, idToConstant))
              .build();
    }
  }
}
