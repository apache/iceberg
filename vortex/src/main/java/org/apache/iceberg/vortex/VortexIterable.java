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
import dev.vortex.jni.NativeRuntime;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
      List<String> projection,
      Optional<Expression> filterPredicate,
      long[] rowRange,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexRowReader<T>> readerFunction,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexBatchReader<T>> batchReaderFunction,
      int workerThreads) {
    this.inputFile = inputFile;
    this.projection = projection;
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

    dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator =
        VortexArrowBridge.vortexAllocator();
    BufferAllocator allocator = VortexArrowBridge.arrowAllocator();
    dev.vortex.relocated.org.apache.arrow.vector.types.pojo.Schema vortexArrowSchema =
        dataSource.arrowSchema(vortexAllocator);
    org.apache.arrow.vector.types.pojo.Schema fileArrowSchema =
        VortexSchemas.toArrowSchema(vortexArrowSchema);

    Optional<dev.vortex.api.Expression> scanFilter =
        filterPredicate.map(
            icebergExpression -> {
              Schema icebergFileSchema = VortexSchemas.convert(vortexArrowSchema);
              return ConvertFilterToVortex.convert(icebergFileSchema, icebergExpression);
            });

    // Vortex resolves projected columns by name and errors on any name not in the file. Drop
    // requested columns the file does not contain (e.g. fields added after the file was written) so
    // the reader fills them with null/constants instead of crashing the scan. Binding is by name:
    // Vortex stores no Iceberg field ids (its Java bindings drop Arrow field/schema metadata), so a
    // column renamed since write time cannot be rebound to its old physical column here.
    Set<String> fileColumns =
        fileArrowSchema.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toUnmodifiableSet());

    String[] projectionNames =
        projection.stream().filter(fileColumns::contains).toArray(String[]::new);
    dev.vortex.api.Expression scanProjection =
        dev.vortex.api.Expression.select(projectionNames, dev.vortex.api.Expression.root());

    ImmutableScanOptions.Builder optionsBuilder = ScanOptions.builder().projection(scanProjection);
    scanFilter.ifPresent(optionsBuilder::filter);
    if (rowRange != null) {
      optionsBuilder.rowRangeBegin(rowRange[0]).rowRangeEnd(rowRange[1]);
    }

    Scan scan = dataSource.scan(optionsBuilder.build());
    Preconditions.checkNotNull(scan, "scan");

    PartitionBatchIterator batchIterator =
        new PartitionBatchIterator(scan, vortexAllocator, allocator);

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
    private final dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator;
    private final BufferAllocator allocator;
    private dev.vortex.relocated.org.apache.arrow.vector.ipc.ArrowReader currentReader;
    private VectorSchemaRoot currentRoot;
    private boolean hasPending = false;
    private boolean exhausted = false;

    PartitionBatchIterator(
        Scan scan,
        dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator,
        BufferAllocator allocator) {
      this.scan = scan;
      this.vortexAllocator = vortexAllocator;
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
        closeCurrentRoot();
        while (true) {
          if (currentReader == null) {
            if (!scan.hasNext()) {
              exhausted = true;
              return false;
            }
            Partition partition = scan.next();
            currentReader = partition.scanArrow(vortexAllocator);
          }
          if (currentReader.loadNextBatch()) {
            currentRoot =
                VortexArrowBridge.importVortexRoot(
                    currentReader.getVectorSchemaRoot(), vortexAllocator, allocator);
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
      closeCurrentRoot();
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      // Don't close shared allocators; they are managed for the lifetime of the process.
    }

    private void closeCurrentRoot() {
      if (currentRoot != null) {
        currentRoot.close();
        currentRoot = null;
      }
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
