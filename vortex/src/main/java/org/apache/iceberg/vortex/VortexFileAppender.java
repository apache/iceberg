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

import dev.vortex.api.VortexWriter;
import dev.vortex.arrow.ArrowAllocation;
import java.io.IOException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

/**
 * A {@link FileAppender} that writes data to Vortex files via the Arrow C-data interface.
 *
 * <p>Rows are buffered in an Arrow {@link VectorSchemaRoot} and flushed to the underlying {@link
 * VortexWriter} when the batch reaches the configured size, by exporting each batch through the
 * Arrow C-data interface as a pair of {@code (array, schema)} pointers.
 */
class VortexFileAppender<D> implements FileAppender<D> {
  static final int DEFAULT_BATCH_SIZE = 2048;

  private final VortexWriter writer;
  private final VortexValueWriter<D> valueWriter;
  private final BufferAllocator allocator;
  private final VectorSchemaRoot root;
  private final int batchSize;
  private final OutputFile outputFile;
  private final org.apache.iceberg.Schema icebergSchema;
  private final MetricsConfig metricsConfig;

  private int currentBatchIndex = 0;
  private long totalRowCount = 0;
  private boolean closed = false;

  VortexFileAppender(
      VortexWriter writer,
      VortexValueWriter<D> valueWriter,
      Schema arrowSchema,
      BufferAllocator allocator,
      int batchSize,
      OutputFile outputFile,
      org.apache.iceberg.Schema icebergSchema,
      MetricsConfig metricsConfig) {
    this.writer = writer;
    this.valueWriter = valueWriter;
    // Use Vortex's shared root allocator: VortexWriter performs allocations against this
    // and the native side manages lifetime via Cleaner references. Owning our own
    // RootAllocator and closing it eagerly trips strict leak checks when Vortex retains
    // small per-writer state.
    this.allocator = allocator != null ? allocator : ArrowAllocation.rootAllocator();
    this.root = VectorSchemaRoot.create(arrowSchema, this.allocator);
    this.batchSize = batchSize;
    this.outputFile = outputFile;
    this.icebergSchema = icebergSchema;
    this.metricsConfig = metricsConfig;
  }

  @Override
  public void add(D datum) {
    if (currentBatchIndex == 0) {
      root.allocateNew();
    }

    valueWriter.write(datum, root, currentBatchIndex);
    currentBatchIndex++;
    totalRowCount++;

    if (currentBatchIndex >= batchSize) {
      flushBatch();
    }
  }

  private void flushBatch() {
    if (currentBatchIndex == 0) {
      return;
    }

    root.setRowCount(currentBatchIndex);

    try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, null, cArray, cSchema);
      writer.writeBatch(cArray.memoryAddress(), cSchema.memoryAddress());
    } catch (IOException e) {
      throw new org.apache.iceberg.exceptions.RuntimeIOException(e, "Failed to write Vortex batch");
    }

    root.clear();
    currentBatchIndex = 0;
  }

  @Override
  public Metrics metrics() {
    return VortexMetrics.buildMetrics(
        totalRowCount, icebergSchema, metricsConfig, valueWriter.metrics());
  }

  @Override
  public long length() {
    if (closed) {
      return outputFile.toInputFile().getLength();
    }

    return 0;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        flushBatch();
        writer.close();
      } finally {
        root.close();
        // Don't close `allocator`: it is Vortex's shared root allocator and is managed for
        // the lifetime of the process.
        closed = true;
      }
    }
  }
}
