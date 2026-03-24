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
package org.apache.iceberg.lance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.lance.file.LanceFileReader;

/**
 * {@link org.apache.iceberg.formats.FormatModel} implementation for the Lance file format.
 *
 * <p>Lance is a columnar data format optimized for AI/ML workloads, featuring fast random access,
 * native vector search, and an Arrow-native data model. This format model bridges Lance's
 * file-level reader/writer APIs with Iceberg's FormatModel abstraction.
 *
 * <p>The file schema type {@code F} is {@link org.apache.arrow.vector.types.pojo.Schema} (Arrow
 * Schema), since Lance is natively Arrow-based.
 *
 * @param <D> the data type being read/written (e.g., Record for generic, InternalRow for Spark)
 * @param <S> the engine schema type (e.g., Void for generic)
 */
public class LanceFormatModel<D, S>
    extends BaseFormatModel<
        D,
        S,
        Function<D, Map<String, Object>>,
        Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>,
        org.apache.arrow.vector.types.pojo.Schema> {

  private static final int DEFAULT_BATCH_SIZE = 1024;

  /**
   * Creates a LanceFormatModel for position deletes. Position deletes use a fixed schema (file path
   * + position) and the generic writer.
   */
  @SuppressWarnings("unchecked")
  public static <D> LanceFormatModel<PositionDelete<D>, Void> forPositionDeletes() {
    return new LanceFormatModel<>(
        (Class<PositionDelete<D>>) (Class<?>) PositionDelete.class, Void.class, null, null);
  }

  /**
   * Creates a LanceFormatModel for the given data and schema types.
   *
   * @param type the data type class
   * @param schemaType the engine schema type class
   * @param writerFunction function that converts data records to Maps for Arrow writing
   * @param readerFunction function that creates a CloseableIterable from Arrow batches
   */
  public static <D, S> LanceFormatModel<D, S> create(
      Class<D> type,
      Class<S> schemaType,
      WriterFunction<Function<D, Map<String, Object>>, S, org.apache.arrow.vector.types.pojo.Schema>
          writerFunction,
      ReaderFunction<
              Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>,
              S,
              org.apache.arrow.vector.types.pojo.Schema>
          readerFunction) {
    return new LanceFormatModel<>(type, schemaType, writerFunction, readerFunction);
  }

  private LanceFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      WriterFunction<Function<D, Map<String, Object>>, S, org.apache.arrow.vector.types.pojo.Schema>
          writerFunction,
      ReaderFunction<
              Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>,
              S,
              org.apache.arrow.vector.types.pojo.Schema>
          readerFunction) {
    super(type, schemaType, writerFunction, readerFunction);
  }

  @Override
  public FileFormat format() {
    return FileFormat.LANCE;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    return new WriteBuilderWrapper<>(outputFile, writerFunction());
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction());
  }

  // ---------------------------------------------------------------------------
  // WriteBuilder
  // ---------------------------------------------------------------------------

  private static class WriteBuilderWrapper<D, S> implements ModelWriteBuilder<D, S> {
    private final EncryptedOutputFile outputFile;
    private final WriterFunction<
            Function<D, Map<String, Object>>, S, org.apache.arrow.vector.types.pojo.Schema>
        writerFunction;
    private Schema schema;
    private S engineSchema;
    private FileContent content;
    private final Map<String, String> metadata = Maps.newHashMap();
    private final Map<String, String> config = Maps.newHashMap();
    private boolean overwrite;

    WriteBuilderWrapper(
        EncryptedOutputFile outputFile,
        WriterFunction<
                Function<D, Map<String, Object>>, S, org.apache.arrow.vector.types.pojo.Schema>
            writerFunction) {
      this.outputFile = outputFile;
      this.writerFunction = writerFunction;
    }

    @Override
    public ModelWriteBuilder<D, S> schema(Schema newSchema) {
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
      config.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> meta(String property, String value) {
      metadata.put(property, value);
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> content(FileContent newContent) {
      this.content = newContent;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> metricsConfig(MetricsConfig metricsConfig) {
      // Lance metrics are tracked in Java; MetricsConfig is noted but not yet used
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> overwrite() {
      this.overwrite = true;
      return this;
    }

    @Override
    public ModelWriteBuilder<D, S> withFileEncryptionKey(ByteBuffer encryptionKey) {
      throw new UnsupportedOperationException("Lance does not support file encryption");
    }

    @Override
    public ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix) {
      throw new UnsupportedOperationException("Lance does not support file encryption");
    }

    @Override
    public FileAppender<D> build() throws IOException {
      Schema writeSchema;
      if (content == FileContent.POSITION_DELETES) {
        writeSchema = DeleteSchemaUtil.pathPosSchema();
      } else {
        writeSchema = schema;
      }

      org.apache.arrow.vector.types.pojo.Schema arrowSchema =
          LanceSchemaUtil.icebergToArrow(writeSchema);

      Function<D, Map<String, Object>> recordConverter;
      if (content == FileContent.POSITION_DELETES) {
        recordConverter = positionDeleteConverter();
      } else {
        recordConverter = writerFunction.write(writeSchema, arrowSchema, engineSchema);
      }

      String path = outputFile.encryptingOutputFile().location();

      // Extract storage options from config (s3.*, gcs.*, azure.* prefixed properties)
      Map<String, String> storageOptions = Maps.newHashMap();
      for (Map.Entry<String, String> entry : config.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith("s3.")
            || key.startsWith("gcs.")
            || key.startsWith("azure.")
            || key.startsWith("lance.storage.")) {
          storageOptions.put(key, entry.getValue());
        }
      }

      int batchSize = DEFAULT_BATCH_SIZE;
      if (config.containsKey("lance.batch-size")) {
        batchSize = Integer.parseInt(config.get("lance.batch-size"));
      }

      return new LanceFileAppender<>(
          path,
          outputFile.encryptingOutputFile(),
          writeSchema,
          arrowSchema,
          recordConverter,
          metadata,
          storageOptions,
          overwrite,
          batchSize);
    }

    @SuppressWarnings("unchecked")
    private Function<D, Map<String, Object>> positionDeleteConverter() {
      return datum -> {
        PositionDelete<?> posDelete = (PositionDelete<?>) datum;
        Map<String, Object> map = Maps.newHashMap();
        map.put("file_path", posDelete.path().toString());
        map.put("pos", posDelete.pos());
        return map;
      };
    }
  }

  // ---------------------------------------------------------------------------
  // ReadBuilder
  // ---------------------------------------------------------------------------

  private static class ReadBuilderWrapper<D, S> implements ReadBuilder<D, S> {
    private final InputFile inputFile;
    private final ReaderFunction<
            Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>,
            S,
            org.apache.arrow.vector.types.pojo.Schema>
        readerFunction;
    private Schema projectedSchema;
    private S engineSchema;
    private Map<Integer, ?> idToConstant = Collections.emptyMap();
    private int batchSize = DEFAULT_BATCH_SIZE;
    private final Map<String, String> config = Maps.newHashMap();

    ReadBuilderWrapper(
        InputFile inputFile,
        ReaderFunction<
                Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>,
                S,
                org.apache.arrow.vector.types.pojo.Schema>
            readerFunction) {
      this.inputFile = inputFile;
      this.readerFunction = readerFunction;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      // Lance does not support byte-range splits; ignore
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema schema) {
      this.projectedSchema = schema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S schema) {
      this.engineSchema = schema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      // Lance column names are case-sensitive by default; no toggle available
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      // No file-level filter pushdown in Lance file reader (only dataset scanner supports it)
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      config.put(key, value);
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      // Arrow vectors naturally manage memory; no explicit reuse needed
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      this.batchSize = numRowsPerBatch;
      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      // Name mapping not yet supported for Lance
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      String path = inputFile.location();

      // Extract storage options
      Map<String, String> storageOptions = Maps.newHashMap();
      for (Map.Entry<String, String> entry : config.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith("s3.")
            || key.startsWith("gcs.")
            || key.startsWith("azure.")
            || key.startsWith("lance.storage.")) {
          storageOptions.put(key, entry.getValue());
        }
      }

      List<String> projectedColumns = null;
      org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
      if (projectedSchema != null) {
        projectedColumns = LanceSchemaUtil.columnNames(projectedSchema);
        arrowSchema = LanceSchemaUtil.icebergToArrow(projectedSchema);
      }

      final List<String> columns = projectedColumns;
      final org.apache.arrow.vector.types.pojo.Schema finalArrowSchema = arrowSchema;
      final Schema readSchema = projectedSchema;
      final Map<Integer, ?> constants = idToConstant;
      final int readBatchSize = batchSize;

      Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>> readerFunc =
          readerFunction.read(readSchema, finalArrowSchema, engineSchema, constants);

      return new CloseableIterable<D>() {
        @Override
        public CloseableIterator<D> iterator() {
          try {
            return new LanceCloseableIterator<>(
                path, storageOptions, columns, readBatchSize, constants, readerFunc);
          } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to open Lance file reader", e);
          }
        }

        @Override
        public void close() throws IOException {
          // Iterator manages its own resources
        }
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Iterator
  // ---------------------------------------------------------------------------

  private static class LanceCloseableIterator<D> implements CloseableIterator<D> {
    private final BufferAllocator allocator;
    private final LanceFileReader fileReader;
    private final ArrowReader arrowReader;
    private final Map<Integer, ?> idToConstant;
    private final Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>>
        readerFunc;

    private CloseableIterator<D> currentBatchIterator;
    private boolean finished;

    LanceCloseableIterator(
        String path,
        Map<String, String> storageOptions,
        List<String> columns,
        int batchSize,
        Map<Integer, ?> idToConstant,
        Function<Map.Entry<VectorSchemaRoot, Map<Integer, ?>>, CloseableIterable<D>> readerFunc)
        throws IOException {
      this.allocator = new RootAllocator(Long.MAX_VALUE);
      this.idToConstant = idToConstant;
      this.readerFunc = readerFunc;
      this.finished = false;

      this.fileReader = LanceFileReader.open(path, storageOptions, allocator);
      this.arrowReader = fileReader.readAll(columns, null, batchSize);
    }

    @Override
    public boolean hasNext() {
      if (finished) {
        return false;
      }

      if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
        return true;
      }

      // Try to load next batch
      try {
        if (arrowReader.loadNextBatch()) {
          VectorSchemaRoot batch = arrowReader.getVectorSchemaRoot();
          CloseableIterable<D> records =
              readerFunc.apply(new java.util.AbstractMap.SimpleEntry<>(batch, idToConstant));
          currentBatchIterator = records.iterator();
          return currentBatchIterator.hasNext();
        } else {
          finished = true;
          return false;
        }
      } catch (IOException e) {
        throw new java.io.UncheckedIOException("Failed to load next batch from Lance reader", e);
      }
    }

    @Override
    public D next() {
      return currentBatchIterator.next();
    }

    @Override
    public void close() throws IOException {
      try {
        if (currentBatchIterator != null) {
          currentBatchIterator.close();
        }
        arrowReader.close();
        fileReader.close();
      } catch (Exception e) {
        throw new IOException("Failed to close Lance reader", e);
      } finally {
        allocator.close();
      }
    }
  }
}
