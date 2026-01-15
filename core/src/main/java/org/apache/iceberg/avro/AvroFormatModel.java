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
package org.apache.iceberg.avro;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.formats.WriteBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class AvroFormatModel<D, S>
    extends BaseFormatModel<D, S, DatumWriter<D>, DatumReader<D>, Schema> {

  public AvroFormatModel(Class<D> type) {
    super(type, null, null, null, false);
  }

  public AvroFormatModel(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<DatumWriter<D>, S, Schema> writerFunction,
      ReaderFunction<DatumReader<D>, S, Schema> readerFunction) {
    super(type, schemaType, writerFunction, readerFunction, false /* batchReader */);
  }

  @Override
  public FileFormat format() {
    return FileFormat.AVRO;
  }

  @Override
  public WriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction());
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction());
  }

  private static class WriteBuilderWrapper<D, S> implements WriteBuilder<D, S> {
    private final Avro.WriteBuilder internal;
    private final WriterFunction<DatumWriter<D>, S, Schema> writerFunction;
    private S inputSchema;
    private FileContent content;

    private WriteBuilderWrapper(
        EncryptedOutputFile outputFile, WriterFunction<DatumWriter<D>, S, Schema> writerFunction) {
      this.internal = Avro.write(outputFile.encryptingOutputFile());
      this.writerFunction = writerFunction;
    }

    @Override
    public WriteBuilder<D, S> schema(org.apache.iceberg.Schema schema) {
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
      throw new UnsupportedOperationException("Avro does not support file encryption keys");
    }

    @Override
    public WriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      throw new UnsupportedOperationException("Avro does not support AAD prefix");
    }

    @Override
    public org.apache.iceberg.io.FileAppender<D> build() throws java.io.IOException {
      switch (content) {
        case DATA:
          internal.createContextFunc(Avro.WriteBuilder.Context::dataContext);
          internal.createWriterFunc(
              avroSchema -> writerFunction.write(null, avroSchema, inputSchema));
          break;
        case EQUALITY_DELETES:
          internal.createContextFunc(Avro.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              avroSchema -> writerFunction.write(null, avroSchema, inputSchema));
          break;
        case POSITION_DELETES:
          internal.createContextFunc(Avro.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(unused -> new Avro.PositionDatumWriter());
          internal.schema(DeleteSchemaUtil.pathPosSchema());
          break;
        default:
          throw new IllegalArgumentException("Unknown file content: " + content);
      }

      return internal.build();
    }
  }

  private static class ReadBuilderWrapper<D, S> implements ReadBuilder<D, S> {
    private final Avro.ReadBuilder internal;
    private final ReaderFunction<DatumReader<D>, S, Schema> readerFunction;
    private Map<Integer, ?> idToConstant = ImmutableMap.of();

    private ReadBuilderWrapper(
        InputFile inputFile, ReaderFunction<DatumReader<D>, S, Schema> readerFunction) {
      this.internal = Avro.read(inputFile);
      this.readerFunction = readerFunction;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      internal.split(newStart, newLength);
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(org.apache.iceberg.Schema schema) {
      internal.project(schema);
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      // Filtering is not supported in Avro reader, so case sensitivity does not matter
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      // Filtering is not supported in Avro reader
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      // Configuration is not used for Avro reader creation
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      internal.reuseContainers();
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      throw new UnsupportedOperationException("Batch reading is not supported in Avro reader");
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(org.apache.iceberg.mapping.NameMapping nameMapping) {
      internal.withNameMapping(nameMapping);
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      return internal
          .createResolvingReader(
              icebergSchema -> readerFunction.read(icebergSchema, null, null, idToConstant))
          .build();
    }
  }
}
