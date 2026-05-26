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

import dev.vortex.api.DataSource;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.api.Partition;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.jni.NativeRuntime;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(VortexIterable.class);

  private final InputFile inputFile;
  private final Optional<Expression> filterPredicate;
  private final long[] rowRange;
  private final Function<org.apache.arrow.vector.types.pojo.Schema, VortexRowReader<T>>
      rowReaderFunc;
  private final Function<org.apache.arrow.vector.types.pojo.Schema, VortexBatchReader<T>>
      batchReaderFunction;
  private final List<String> projection;
  private final int workerThreads;

  VortexIterable(
      InputFile inputFile,
      Schema icebergSchema,
      Optional<Expression> filterPredicate,
      long[] rowRange,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexRowReader<T>> readerFunction,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexBatchReader<T>> batchReaderFunction,
      int workerThreads) {
    this.inputFile = inputFile;
    this.projection = Lists.transform(icebergSchema.columns(), Types.NestedField::name);
    this.filterPredicate = filterPredicate;
    this.rowRange = rowRange;
    this.rowReaderFunc = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.workerThreads = workerThreads;
  }

  @Override
  public CloseableIterator<T> iterator() {
    LOG.debug("opening Vortex file: {}", inputFile);

    // Apply the worker-thread setting on this executor JVM before any Vortex native work
    // begins for this read.
    NativeRuntime.setWorkerThreads(workerThreads);

    Session session = Session.create();
    String uri = VortexFileUtil.resolveUri(inputFile.location());
    Map<String, String> properties = VortexFileUtil.resolveInputProperties(inputFile);
    DataSource dataSource = DataSource.open(session, uri, properties);

    BufferAllocator allocator = ArrowAllocation.rootAllocator();
    org.apache.arrow.vector.types.pojo.Schema fileArrowSchema = dataSource.arrowSchema(allocator);

    Optional<dev.vortex.api.Expression> scanFilter =
        filterPredicate.map(
            icebergExpression -> {
              Schema icebergFileSchema = VortexSchemas.convert(fileArrowSchema);
              return ConvertFilterToVortex.convert(icebergFileSchema, icebergExpression);
            });

    String[] projectionNames = projection.toArray(new String[0]);
    dev.vortex.api.Expression scanProjection =
        dev.vortex.api.Expression.select(projectionNames, dev.vortex.api.Expression.root());

    ImmutableScanOptions.Builder optionsBuilder = ScanOptions.builder().projection(scanProjection);
    scanFilter.ifPresent(optionsBuilder::filter);
    if (rowRange != null) {
      optionsBuilder.rowRangeBegin(rowRange[0]).rowRangeEnd(rowRange[1]);
    }

    Scan scan = dataSource.scan(optionsBuilder.build());
    Preconditions.checkNotNull(scan, "scan");

    PartitionBatchIterator batchIterator = new PartitionBatchIterator(scan, allocator);

    if (rowReaderFunc != null) {
      VortexRowReader<T> rowFunction = rowReaderFunc.apply(fileArrowSchema);
      return new VortexRowIterator<>(batchIterator, rowFunction);
    } else {
      VortexBatchReader<T> batchTransform = batchReaderFunction.apply(fileArrowSchema);
      return new VortexBatchIterator<>(batchIterator, batchTransform);
    }
  }

  /** Iterator that pulls Arrow {@link VectorSchemaRoot} batches across Vortex partitions. */
  static class PartitionBatchIterator implements CloseableIterator<VectorSchemaRoot> {
    private final Scan scan;
    private final BufferAllocator allocator;
    private ArrowReader currentReader;
    private VectorSchemaRoot currentRoot;
    private boolean hasPending = false;
    private boolean exhausted = false;

    PartitionBatchIterator(Scan scan, BufferAllocator allocator) {
      this.scan = scan;
      this.allocator = allocator;
    }

    @Override
    public boolean hasNext() {
      if (hasPending) {
        return true;
      }
      if (exhausted) {
        return false;
      }
      try {
        while (true) {
          if (currentReader == null) {
            if (!scan.hasNext()) {
              exhausted = true;
              return false;
            }
            Partition partition = scan.next();
            currentReader = partition.scanArrow(allocator);
            currentRoot = currentReader.getVectorSchemaRoot();
          }
          if (currentReader.loadNextBatch()) {
            hasPending = true;
            return true;
          } else {
            currentReader.close();
            currentReader = null;
            currentRoot = null;
          }
        }
      } catch (IOException e) {
        throw new org.apache.iceberg.exceptions.RuntimeIOException(
            e, "Failed to load next Vortex batch");
      }
    }

    @Override
    public VectorSchemaRoot next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      hasPending = false;
      return currentRoot;
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      // Don't close `allocator`: it is Vortex's shared root allocator.
    }
  }

  static class VortexRowIterator<T> implements CloseableIterator<T> {
    private final PartitionBatchIterator batches;
    private final VortexRowReader<T> rowReader;

    private VectorSchemaRoot currentBatch = null;
    private int batchIndex = 0;
    private int batchLen = 0;

    VortexRowIterator(PartitionBatchIterator batches, VortexRowReader<T> rowReader) {
      this.batches = batches;
      this.rowReader = rowReader;
    }

    @Override
    public void close() throws IOException {
      batches.close();
    }

    @Override
    public boolean hasNext() {
      if (currentBatch != null && batchIndex < batchLen) {
        return true;
      }
      if (batches.hasNext()) {
        currentBatch = batches.next();
        batchIndex = 0;
        batchLen = currentBatch.getRowCount();
        if (batchLen > 0) {
          return true;
        }
        return hasNext();
      }
      currentBatch = null;
      batchLen = 0;
      return false;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T nextRow = rowReader.read(currentBatch, batchIndex);
      batchIndex++;
      return nextRow;
    }
  }

  static class VortexBatchIterator<T> implements CloseableIterator<T> {
    private final PartitionBatchIterator batches;
    private final VortexBatchReader<T> batchReader;

    VortexBatchIterator(PartitionBatchIterator batches, VortexBatchReader<T> batchReader) {
      this.batches = batches;
      this.batchReader = batchReader;
    }

    @Override
    public boolean hasNext() {
      return batches.hasNext();
    }

    @Override
    public T next() {
      return batchReader.read(batches.next());
    }

    @Override
    public void close() throws IOException {
      batches.close();
    }
  }
}
