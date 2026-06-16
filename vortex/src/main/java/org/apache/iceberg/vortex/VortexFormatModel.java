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
package org.apache.iceberg.vortex;

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import dev.vortex.jni.NativeRuntime;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class VortexFormatModel<D, S, R>
    extends BaseFormatModel<D, S, VortexValueWriter<?>, R, Schema> {
  private final boolean isBatchReader;

  public interface ReaderFunction<R> {
    VortexRowReader<R> read(
        org.apache.iceberg.Schema schema, Schema fileArrowSchema, Map<Integer, ?> idToConstant);
  }

  public interface BatchReaderFunction<T> {
    VortexBatchReader<T> batchRead(
        org.apache.iceberg.Schema icebergSchema,
        Schema fileArrowSchema,
        Map<Integer, ?> idToConstant);
  }

  public static <D, S> VortexFormatModel<D, S, VortexRowReader<?>> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<VortexValueWriter<?>, S, Schema> writerFunction,
      ReaderFunction<D> readerFunction) {
    return new VortexFormatModel<>(
        type,
        schemaType,
        writerFunction,
        (icebergSchema, fileSchema, engineSchema, idToConstant) ->
            readerFunction.read(icebergSchema, fileSchema, idToConstant),
        false);
  }

  public static <D, S> VortexFormatModel<D, S, VortexBatchReader<?>> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<VortexValueWriter<?>, S, Schema> writerFunction,
      BatchReaderFunction<D> batchReaderFunction) {
    return new VortexFormatModel<>(
        type,
        schemaType,
        writerFunction,
        (icebergSchema, fileSchema, engineSchema, idToConstant) ->
            batchReaderFunction.batchRead(icebergSchema, fileSchema, idToConstant),
        true);
  }

  private VortexFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<VortexValueWriter<?>, S, Schema> writerFunction,
      BaseFormatModel.ReaderFunction<R, S, Schema> readerFunction,
      boolean isBatchReader) {
    super(type, schemaType, writerFunction, readerFunction);
    this.isBatchReader = isBatchReader;
  }

  @Override
  public FileFormat format() {
    return FileFormat.VORTEX;
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
    private final EncryptedOutputFile outputFile;
    private final WriterFunction<VortexValueWriter<?>, S, Schema> writerFunction;
    private org.apache.iceberg.Schema schema;
    private S engineSchema;
    private FileContent content;
    private MetricsConfig metricsConfig = MetricsConfig.getDefault();
    private final Map<String, String> writerProperties = Maps.newHashMap();
    private final Map<String, String> metadata = Maps.newHashMap();
    private int workerThreads = TableProperties.VORTEX_WORKER_THREADS_DEFAULT;

    private WriteBuilderWrapper(
        EncryptedOutputFile outputFile,
        WriterFunction<VortexValueWriter<?>, S, Schema> writerFunction) {
      this.outputFile = outputFile;
      this.writerFunction = writerFunction;
    }

    @Override
    public ModelWriteBuilder<D, S> schema(org.apache.iceberg.Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> engineSchema(S newSchema) {
      this.engineSchema = newSchema;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> set(String property, String value) {
      if (TableProperties.WRITE_VORTEX_WORKER_THREADS.equals(property)) {
        workerThreads = Integer.parseInt(value);
      }

      writerProperties.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> setAll(Map<String, String> properties) {
      properties.forEach(this::set);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(Map<String, String> properties) {
      metadata.putAll(properties);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> content(FileContent newContent) {
      this.content = newContent;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> metricsConfig(MetricsConfig newMetricsConfig) {
      this.metricsConfig = newMetricsConfig;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> overwrite() {
      // VortexWriter overwrites by default
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withFileEncryptionKey(ByteBuffer encryptionKey) {
      throw new UnsupportedOperationException("Vortex does not support file encryption keys");
    }

    @Override
    public ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      throw new UnsupportedOperationException("Vortex does not support AAD prefix");
    }

    @Override
    public FileAppender<D> build() throws IOException {
      Preconditions.checkNotNull(schema, "Schema is required");
      Preconditions.checkNotNull(content, "Content type is required");

      return switch (content) {
        case DATA, EQUALITY_DELETES -> buildAppender(schema);
        case POSITION_DELETES, DATA_MANIFEST, DELETE_MANIFEST ->
            throw new UnsupportedOperationException(
                "Position deletes are not yet supported for Vortex format");
      };
    }

    @SuppressWarnings("unchecked")
    private FileAppender<D> buildAppender(org.apache.iceberg.Schema writeSchema)
        throws IOException {
      Schema arrowSchema = VortexSchemas.toArrowSchema(writeSchema);
      dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexSchema =
          VortexSchemas.toVortexArrowSchema(writeSchema);

      VortexValueWriter<D> valueWriter =
          (VortexValueWriter<D>) writerFunction.write(writeSchema, arrowSchema, engineSchema);

      OutputFile rawOutputFile = outputFile.encryptingOutputFile();
      String uri = VortexFileUtil.resolveUri(rawOutputFile.location());
      Map<String, String> properties =
          Maps.newHashMap(VortexFileUtil.resolveOutputProperties(rawOutputFile));
      properties.putAll(writerProperties);
      properties.putAll(metadata);

      // Apply worker-thread setting on this executor JVM before any Vortex native work begins.
      NativeRuntime.setWorkerThreads(workerThreads);
      BufferAllocator allocator = VortexArrowBridge.arrowAllocator();
      dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator =
          VortexArrowBridge.vortexAllocator();
      Session session = Session.create();
      VortexWriter vortexWriter =
          VortexWriter.create(session, uri, vortexSchema, properties, vortexAllocator);

      return new VortexFileAppender<>(
          vortexWriter,
          valueWriter,
          arrowSchema,
          allocator,
          VortexFileAppender.DEFAULT_BATCH_SIZE,
          rawOutputFile,
          writeSchema,
          metricsConfig);
    }
  }

  private static class ReadBuilderWrapper<R, D, S> implements ReadBuilder<D, S> {
    private final InputFile inputFile;
    private final BaseFormatModel.ReaderFunction<R, S, Schema> readerFunction;
    private final boolean isBatchReader;
    private org.apache.iceberg.Schema schema;
    private S engineSchema;
    private Map<Integer, ?> idToConstant;
    private Optional<Expression> filterPredicate = Optional.empty();
    private boolean caseSensitive = true;
    private long[] rowRange;
    private int workerThreads = TableProperties.VORTEX_WORKER_THREADS_DEFAULT;

    private ReadBuilderWrapper(
        InputFile inputFile,
        BaseFormatModel.ReaderFunction<R, S, Schema> readerFunction,
        boolean isBatchReader) {
      this.inputFile = inputFile;
      this.readerFunction = readerFunction;
      this.isBatchReader = isBatchReader;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      this.rowRange = new long[] {newStart, newStart + newLength};
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(org.apache.iceberg.Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S newSchema) {
      this.engineSchema = newSchema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      this.filterPredicate = Optional.ofNullable(filter);
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      if (TableProperties.READ_VORTEX_WORKER_THREADS.equals(key)) {
        workerThreads = Integer.parseInt(value);
      }
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      if (!isBatchReader) {
        throw new UnsupportedOperationException(
            "Batch reading is not supported in non-vectorized reader");
      }

      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CloseableIterable<D> build() {
      Function<Schema, VortexRowReader<D>> readerFunc = null;
      Function<Schema, VortexBatchReader<D>> batchReaderFunc = null;

      if (isBatchReader) {
        batchReaderFunc =
            fileSchema ->
                (VortexBatchReader<D>)
                    readerFunction.read(schema, fileSchema, engineSchema, idToConstant);
      } else {
        readerFunc =
            fileSchema ->
                (VortexRowReader<D>)
                    readerFunction.read(schema, fileSchema, engineSchema, idToConstant);
      }

      // Compute the columns to scan from the data file. Constants (identity partition values and
      // metadata columns such as _file, _spec_id and _partition) come from idToConstant, and
      // _is_deleted is synthesized by the reader, so none of those are projected from the file.
      // _pos is also excluded and currently resolves to null: Vortex exposes row positions through
      // a `row_idx` scan expression that the Java bindings (<= 0.73.0) do not yet surface.
      Map<Integer, ?> constants = idToConstant == null ? Collections.emptyMap() : idToConstant;
      List<String> projection =
          schema.columns().stream()
              .filter(
                  field ->
                      (field.fieldId() == MetadataColumns.ROW_POSITION.fieldId())
                          || !constants.containsKey(field.fieldId())
                              && !MetadataColumns.isMetadataColumn(field.name()))
              .map(Types.NestedField::name)
              .toList();

      return new VortexIterable<>(
          inputFile,
          projection,
          filterPredicate,
          rowRange,
          readerFunc,
          batchReaderFunc,
          caseSensitive,
          workerThreads);
    }
  }
}
