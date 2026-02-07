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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class AvroFormatModel<D, S>
    extends BaseFormatModel<D, S, DatumWriter<D>, DatumReader<D>, Schema> {

  @SuppressWarnings("rawtypes")
  public static AvroFormatModel<PositionDelete, Object> forDelete() {
    return new AvroFormatModel<>(PositionDelete.class, null, null, null);
  }

  public static <D, S> AvroFormatModel<D, S> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<DatumWriter<D>, S, Schema> writerFunction,
      ReaderFunction<DatumReader<D>, S, Schema> readerFunction) {
    return new AvroFormatModel<>(type, schemaType, writerFunction, readerFunction);
  }

  private AvroFormatModel(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<DatumWriter<D>, S, Schema> writerFunction,
      ReaderFunction<DatumReader<D>, S, Schema> readerFunction) {
    super(type, schemaType, writerFunction, readerFunction);
  }

  @Override
  public FileFormat format() {
    return FileFormat.AVRO;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction());
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction());
  }

  private static class WriteBuilderWrapper<D, S> implements ModelWriteBuilder<D, S> {
    private final Avro.WriteBuilder internal;
    private final WriterFunction<DatumWriter<D>, S, Schema> writerFunction;
    private org.apache.iceberg.Schema schema;
    private S engineSchema;
    private FileContent content;

    private WriteBuilderWrapper(
        EncryptedOutputFile outputFile, WriterFunction<DatumWriter<D>, S, Schema> writerFunction) {
      this.internal = Avro.write(outputFile.encryptingOutputFile());
      this.writerFunction = writerFunction;
    }

    @Override
    public ModelWriteBuilder<D, S> schema(org.apache.iceberg.Schema newSchema) {
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
      throw new UnsupportedOperationException("Avro does not support file encryption keys");
    }

    @Override
    public ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      throw new UnsupportedOperationException("Avro does not support AAD prefix");
    }

    @Override
    public org.apache.iceberg.io.FileAppender<D> build() throws IOException {
      switch (content) {
        case DATA:
          internal.createContextFunc(Avro.WriteBuilder.Context::dataContext);
          internal.createWriterFunc(
              avroSchema -> writerFunction.write(schema, avroSchema, engineSchema));
          break;
        case EQUALITY_DELETES:
          internal.createContextFunc(Avro.WriteBuilder.Context::deleteContext);
          internal.createWriterFunc(
              avroSchema -> writerFunction.write(schema, avroSchema, engineSchema));
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
    private S engineSchema;
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
    public ReadBuilder<D, S> engineProjection(S schema) {
      this.engineSchema = schema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      // Filtering is not supported in Avro reader, so case sensitivity does not matter
      // This is not an error since filtering is best-effort.
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      // Filtering is not supported in Avro reader
      // This is not an error since filtering is best-effort.
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
      // The file schema is passed directly to the DatumReader by the Avro read path, so null is
      // passed here
      return internal
          .createResolvingReader(
              icebergSchema -> readerFunction.read(icebergSchema, null, engineSchema, idToConstant))
          .build();
    }
  }
}
