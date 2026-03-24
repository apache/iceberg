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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.lance.file.LanceFileWriter;

/**
 * A {@link FileAppender} implementation that writes data to Lance files.
 *
 * <p>This appender bridges Iceberg's record-at-a-time {@code add(D)} interface with Lance's
 * batch-oriented {@code write(VectorSchemaRoot)} API. Records are accumulated in an Arrow
 * VectorSchemaRoot buffer and flushed to the Lance writer when the batch is full.
 *
 * @param <D> the data type being written
 */
public class LanceFileAppender<D> implements FileAppender<D> {
  private final LanceFileWriter writer;
  private final BufferAllocator allocator;
  private final Function<D, Map<String, Object>> recordToMap;
  private final org.apache.iceberg.Schema icebergSchema;
  private final OutputFile outputFile;
  private final int batchSize;

  private VectorSchemaRoot currentBatch;
  private int currentRowIndex;
  private long totalRecordCount;
  private long estimatedFlushedBytes;
  private boolean closed;

  LanceFileAppender(
      String path,
      OutputFile outputFile,
      org.apache.iceberg.Schema icebergSchema,
      Schema arrowSchema,
      Function<D, Map<String, Object>> recordToMap,
      Map<String, String> metadata,
      Map<String, String> storageOptions,
      boolean overwrite,
      int batchSize)
      throws IOException {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.icebergSchema = icebergSchema;
    this.outputFile = outputFile;
    this.recordToMap = recordToMap;
    this.batchSize = batchSize;
    this.currentRowIndex = 0;
    this.totalRecordCount = 0;
    this.estimatedFlushedBytes = 0;
    this.closed = false;

    DictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
    this.writer = LanceFileWriter.open(path, allocator, dictProvider, storageOptions);

    // Store Iceberg schema JSON and any user metadata in the Lance file
    Map<String, String> allMetadata = Maps.newHashMap(metadata);
    allMetadata.put(
        LanceSchemaUtil.ICEBERG_SCHEMA_KEY, org.apache.iceberg.SchemaParser.toJson(icebergSchema));
    writer.addSchemaMetadata(allMetadata);

    this.currentBatch = VectorSchemaRoot.create(arrowSchema, allocator);
    this.currentBatch.allocateNew();
  }

  @Override
  public void add(D datum) {
    Map<String, Object> values = recordToMap.apply(datum);
    LanceArrowConverter.writeRow(currentBatch, currentRowIndex, values, icebergSchema);
    currentRowIndex++;
    totalRecordCount++;

    if (currentRowIndex >= batchSize) {
      flushBatch();
    }
  }

  @Override
  public Metrics metrics() {
    return new Metrics(totalRecordCount, null, null, null, null);
  }

  @Override
  public long length() {
    if (closed) {
      return outputFile.toInputFile().getLength();
    }

    // Estimate: flushed bytes so far + current unflushed batch buffer sizes
    long currentBatchBytes = 0;
    for (FieldVector vector : currentBatch.getFieldVectors()) {
      currentBatchBytes += vector.getBufferSize();
    }
    return estimatedFlushedBytes + currentBatchBytes;
  }

  @Override
  public List<Long> splitOffsets() {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        if (currentRowIndex > 0) {
          flushBatch();
        } else if (totalRecordCount == 0) {
          // Lance requires at least one batch to establish the schema.
          // Write an empty batch with row count 0.
          currentBatch.setRowCount(0);
          writer.write(currentBatch);
        }
        writer.close();
      } catch (Exception e) {
        throw new IOException("Failed to close Lance writer", e);
      } finally {
        currentBatch.close();
        allocator.close();
        closed = true;
      }
    }
  }

  private void flushBatch() {
    try {
      currentBatch.setRowCount(currentRowIndex);
      // Accumulate buffer sizes before clearing for length() estimates
      for (FieldVector vector : currentBatch.getFieldVectors()) {
        estimatedFlushedBytes += vector.getBufferSize();
      }
      writer.write(currentBatch);
      currentBatch.clear();
      currentBatch.allocateNew();
      currentRowIndex = 0;
    } catch (IOException e) {
      throw new java.io.UncheckedIOException("Failed to flush batch to Lance writer", e);
    }
  }
}
