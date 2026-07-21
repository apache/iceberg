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

import dev.vortex.api.VortexWriteSummary;
import dev.vortex.api.VortexWriter;
import dev.vortex.io.NativeWritable;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A {@link FileAppender} that writes data to Vortex files via the Arrow C-data interface.
 *
 * <p>Rows are buffered in an Arrow {@link VectorSchemaRoot} and flushed to the underlying {@link
 * VortexWriter} when the batch reaches the configured size, by exporting each batch through the
 * Arrow C-data interface as a pair of {@code (array, schema)} pointers.
 *
 * <p>The appender owns the {@link NativeWritable} byte sink the writer streams into: closing the
 * appender finalizes the Vortex file and then closes the sink. Iceberg {@link Metrics} are built
 * from the {@link VortexWriteSummary} statistics computed natively during the write.
 */
class VortexFileAppender<D> implements FileAppender<D> {
  static final int DEFAULT_BATCH_SIZE = 2048;

  private final VortexWriter writer;
  private final VortexValueWriter<D> valueWriter;
  private final BufferAllocator allocator;
  private final VectorSchemaRoot root;
  private final int batchSize;
  private final NativeWritable outputStream;
  private final org.apache.iceberg.Schema icebergSchema;
  private final MetricsConfig metricsConfig;
  private final long splitSize;

  // Nominal per-row width used to size rows buffered before the first batch is handed to the
  // native writer, so rolling writers see a non-zero length as soon as rows are buffered.
  private static final long PRE_FLUSH_ROW_SIZE_ESTIMATE = 8;

  // Lower bound for the split-offset granularity so the persisted offset list stays small even
  // when the configured split size is tiny.
  private static final long MIN_SPLIT_SIZE = 1024;

  private int currentBatchIndex = 0;
  private long flushedRows = 0;
  private long flushedArrowBytes = 0;
  private boolean closed = false;
  private VortexWriteSummary summary = null;
  private Metrics metrics = null;

  VortexFileAppender(
      VortexWriter writer,
      VortexValueWriter<D> valueWriter,
      Schema arrowSchema,
      BufferAllocator allocator,
      int batchSize,
      NativeWritable outputStream,
      org.apache.iceberg.Schema icebergSchema,
      MetricsConfig metricsConfig,
      long splitSize) {
    this.writer = writer;
    this.valueWriter = valueWriter;
    this.allocator = allocator != null ? allocator : VortexArrowBridge.arrowAllocator();
    this.root = VectorSchemaRoot.create(arrowSchema, this.allocator);
    this.batchSize = batchSize;
    this.outputStream = outputStream;
    this.icebergSchema = icebergSchema;
    this.metricsConfig = metricsConfig;
    this.splitSize = Math.max(splitSize, MIN_SPLIT_SIZE);
  }

  @Override
  public void add(D datum) {
    if (currentBatchIndex == 0) {
      root.allocateNew();
    }

    valueWriter.write(datum, root, currentBatchIndex);
    currentBatchIndex++;

    if (currentBatchIndex >= batchSize) {
      flushBatch();
    }
  }

  private void flushBatch() {
    if (currentBatchIndex == 0) {
      return;
    }

    root.setRowCount(currentBatchIndex);
    flushedRows += currentBatchIndex;
    flushedArrowBytes += arrowBufferSize();

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

  private long arrowBufferSize() {
    long size = 0;
    for (org.apache.arrow.vector.FieldVector vector : root.getFieldVectors()) {
      size += vector.getBufferSize();
    }

    return size;
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Cannot return metrics while appending to an open file");
    Preconditions.checkState(summary != null, "Vortex writer did not produce a write summary");
    if (metrics == null) {
      metrics = VortexMetrics.fromWriteSummary(icebergSchema, metricsConfig, summary);
    }

    return metrics;
  }

  @Override
  public List<Long> splitOffsets() {
    if (!closed || summary == null) {
      return null;
    }

    // Vortex files have no physical split-offset metadata, so persist synthetic byte offsets at
    // the configured granularity. Readers approximate byte ranges to row ranges by file fraction,
    // so any strictly ascending offsets partition the rows without gaps or overlaps.
    ImmutableList.Builder<Long> offsets = ImmutableList.builder();
    for (long offset = 0; offset == 0 || offset < summary.fileSize(); offset += splitSize) {
      offsets.add(offset);
    }

    return offsets.build();
  }

  @Override
  public long length() {
    if (closed) {
      Preconditions.checkState(summary != null, "Vortex writer did not produce a write summary");
      return summary.fileSize();
    }

    // The native writer compresses and flushes asynchronously, so the bytes that reached the sink
    // lag the rows accepted by this appender. Estimate the volume of rows still buffered (in the
    // current Arrow batch and the native writer) from the uncompressed Arrow bytes handed over so
    // far, so that rolling writers see the file grow as rows are appended.
    long avgRowSize =
        flushedRows > 0 ? flushedArrowBytes / flushedRows : PRE_FLUSH_ROW_SIZE_ESTIMATE;
    long bufferedEstimate = flushedArrowBytes + (currentBatchIndex * avgRowSize);
    return Math.max(writer.bytesWritten(), bufferedEstimate);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        flushBatch();
        this.summary = writer.finish();
      } finally {
        closed = true;
        try {
          root.close();
          // Don't close `allocator`: it is shared for the lifetime of the process.
        } finally {
          outputStream.close();
        }
      }
    }
  }
}
