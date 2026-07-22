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
import dev.vortex.io.NativeReadable;
import dev.vortex.jni.NativeRuntime;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(VortexIterable.class);

  private final InputFile inputFile;
  private final Optional<Expression> filterPredicate;
  private final long[] splitByteRange;
  private final byte[] posDeleteBitmap;
  private final boolean includeRowPosition;
  private final boolean includeRowId;
  private final boolean includeLastUpdatedSeq;
  private final boolean reuseContainers;
  private final Function<org.apache.arrow.vector.types.pojo.Schema, VortexRowReader<T>>
      rowReaderFunc;
  private final Function<org.apache.arrow.vector.types.pojo.Schema, VortexBatchReader<T>>
      batchReaderFunction;
  private final List<String> projection;
  private final boolean caseSensitive;
  private final int workerThreads;

  VortexIterable(
      InputFile inputFile,
      List<String> projection,
      Optional<Expression> filterPredicate,
      long[] splitByteRange,
      byte[] posDeleteBitmap,
      boolean includeRowPosition,
      boolean includeRowId,
      boolean includeLastUpdatedSeq,
      boolean reuseContainers,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexRowReader<T>> readerFunction,
      Function<org.apache.arrow.vector.types.pojo.Schema, VortexBatchReader<T>> batchReaderFunction,
      boolean caseSensitive,
      int workerThreads) {
    this.inputFile = inputFile;
    this.projection = projection;
    this.filterPredicate = filterPredicate;
    this.splitByteRange = splitByteRange;
    this.posDeleteBitmap = posDeleteBitmap;
    this.includeRowPosition = includeRowPosition;
    this.includeRowId = includeRowId;
    this.includeLastUpdatedSeq = includeLastUpdatedSeq;
    this.reuseContainers = reuseContainers;
    this.rowReaderFunc = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
    this.caseSensitive = caseSensitive;
    this.workerThreads = workerThreads;
  }

  @Override
  public CloseableIterator<T> iterator() {
    LOG.debug("opening Vortex file: {}", inputFile);

    // Apply the worker-thread setting on this executor JVM before any Vortex native work
    // begins for this read.
    NativeRuntime.setWorkerThreads(workerThreads);

    Session session = VortexSessions.shared();
    // Read through the table's FileIO instead of Vortex's native storage clients. The returned
    // iterator owns the readable and closes it when the scan is exhausted or abandoned.
    NativeReadable readable = VortexIO.readable(inputFile);
    try {
      return open(session, readable);
    } catch (RuntimeException e) {
      try {
        readable.close();
      } catch (IOException suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
  }

  private CloseableIterator<T> open(Session session, NativeReadable readable) {
    DataSource dataSource = DataSource.open(session, readable);

    long[] rowRange = null;
    if (splitByteRange != null) {
      rowRange = toRowRange(splitByteRange, readable.length(), dataSource);
      if (rowRange[0] >= rowRange[1]) {
        // The byte range maps to zero rows: skip the scan entirely. An all-zero range must not
        // reach the native scan, which interprets it as "no range set".
        try {
          readable.close();
        } catch (IOException e) {
          throw new org.apache.iceberg.exceptions.RuntimeIOException(
              e, "Failed to close input source for %s", inputFile.location());
        }
        return CloseableIterator.empty();
      }
    }

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
              return ConvertFilterToVortex.convert(
                  icebergFileSchema, icebergExpression, caseSensitive);
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

    ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
    ImmutableList.Builder<dev.vortex.api.Expression> expressions = ImmutableList.builder();

    for (String name : projection) {
      if (fileColumns.contains(name)) {
        fieldNames.add(name);
        expressions.add(dev.vortex.api.Expression.column(name));
      }
    }

    // Row position is not a stored column. When requested, materialize it from Vortex's `row_idx`
    // scan expression packed under the _pos metadata-column name, and append a matching _pos field
    // to the schema handed to the reader. Both bind by name, so _pos resolves regardless of where
    // it lands in the projected column order.
    org.apache.arrow.vector.types.pojo.Schema readerArrowSchema = fileArrowSchema;
    if (includeRowPosition) {
      fieldNames.add(MetadataColumns.ROW_POSITION.name());
      expressions.add(dev.vortex.api.Expression.rowIdx());
      readerArrowSchema = appendRowPosition(fileArrowSchema);
    }

    addRowLineageProjections(fileColumns, fieldNames, expressions);

    dev.vortex.api.Expression scanProjection =
        dev.vortex.api.Expression.pack(
            fieldNames.build().toArray(String[]::new),
            expressions.build().toArray(dev.vortex.api.Expression[]::new),
            false);

    ImmutableScanOptions.Builder optionsBuilder = ScanOptions.builder().projection(scanProjection);
    // Vortex scans resolve partitions concurrently and yield them out of order by default. Row
    // readers surface records positionally (like every other iceberg file format reader), so the
    // row path must preserve file order. The batch path stays unordered: Spark derives row
    // positions from the scan's row_idx values rather than emission order.
    if (rowReaderFunc != null) {
      optionsBuilder.ordered(true);
    }
    scanFilter.ifPresent(optionsBuilder::filter);
    if (rowRange != null) {
      optionsBuilder.rowRangeBegin(rowRange[0]).rowRangeEnd(rowRange[1]);
    }

    // Apply position deletes natively: the bitmap holds file-relative row positions of deleted rows
    // (portable 64-bit Roaring), and Vortex drops them from the scan so they are never
    // materialized.
    if (posDeleteBitmap != null) {
      optionsBuilder
          .selectionRoaringBitmap(posDeleteBitmap)
          .selectionMode(ScanOptions.SelectionMode.EXCLUDE_ROARING);
    }

    Scan scan = dataSource.scan(optionsBuilder.build());
    Preconditions.checkNotNull(scan, "scan");

    PartitionBatchIterator batchIterator =
        new PartitionBatchIterator(scan, vortexAllocator, allocator, readable);

    if (rowReaderFunc != null) {
      VortexRowReader<T> rowFunction = rowReaderFunc.apply(readerArrowSchema);
      if (reuseContainers) {
        rowFunction.reuseContainers(true);
      }
      return new VortexRowIterator<>(batchIterator, rowFunction);
    } else {
      VortexBatchReader<T> batchTransform = batchReaderFunction.apply(readerArrowSchema);
      return new VortexBatchIterator<>(batchIterator, batchTransform);
    }
  }

  /**
   * Row lineage: projects {@code _row_id} as a nested pack of the stored column (when the file has
   * one) and the row position, so readers can prefer stored values and fall back to {@code
   * firstRowId + position}. The last-updated sequence number column is projected when stored;
   * readers substitute the file's sequence number for null or absent values.
   */
  private void addRowLineageProjections(
      Set<String> fileColumns,
      ImmutableList.Builder<String> fieldNames,
      ImmutableList.Builder<dev.vortex.api.Expression> expressions) {
    if (includeRowId) {
      String rowIdName = MetadataColumns.ROW_ID.name();
      fieldNames.add(rowIdName);
      if (fileColumns.contains(rowIdName)) {
        expressions.add(
            dev.vortex.api.Expression.pack(
                new String[] {"value", "pos"},
                new dev.vortex.api.Expression[] {
                  dev.vortex.api.Expression.column(rowIdName), dev.vortex.api.Expression.rowIdx()
                },
                false));
      } else {
        expressions.add(
            dev.vortex.api.Expression.pack(
                new String[] {"pos"},
                new dev.vortex.api.Expression[] {dev.vortex.api.Expression.rowIdx()},
                false));
      }
    }

    String lastUpdatedName = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name();
    if (includeLastUpdatedSeq && fileColumns.contains(lastUpdatedName)) {
      fieldNames.add(lastUpdatedName);
      expressions.add(dev.vortex.api.Expression.column(lastUpdatedName));
    }
  }

  /**
   * Approximates a byte-range split as a row range. Iceberg plans splits as byte ranges, but Vortex
   * scans select rows: both boundaries map to rows by their fraction of the file, using the file's
   * exact row count. Because every boundary maps through the same function, the contiguous byte
   * splits of a file map to contiguous, non-overlapping row ranges that cover every row exactly
   * once.
   */
  private static long[] toRowRange(long[] byteRange, long fileLength, DataSource dataSource) {
    Preconditions.checkState(
        dataSource.rowCount() instanceof DataSource.RowCount.Exact,
        "Cannot map a byte-range split to rows without an exact row count: %s",
        dataSource.rowCount());
    long totalRows = ((DataSource.RowCount.Exact) dataSource.rowCount()).value();
    long begin = rowAt(byteRange[0], fileLength, totalRows);
    long end = rowAt(byteRange[0] + byteRange[1], fileLength, totalRows);
    return new long[] {begin, end};
  }

  private static long rowAt(long byteOffset, long fileLength, long totalRows) {
    if (byteOffset >= fileLength) {
      return totalRows;
    }

    return (long) (((double) byteOffset / fileLength) * totalRows);
  }

  /**
   * Appends a required {@code _pos} (int64) field to an Arrow schema so readers can bind the
   * synthetic row-position column produced by the {@code row_idx} scan projection.
   */
  private static org.apache.arrow.vector.types.pojo.Schema appendRowPosition(
      org.apache.arrow.vector.types.pojo.Schema base) {
    List<Field> fields = Lists.newArrayList(base.getFields());
    fields.add(
        new Field(
            MetadataColumns.ROW_POSITION.name(),
            new FieldType(false, new ArrowType.Int(Long.SIZE, true), null),
            null));
    return new org.apache.arrow.vector.types.pojo.Schema(fields, base.getCustomMetadata());
  }

  /** Iterator that pulls Arrow {@link VectorSchemaRoot} batches across Vortex partitions. */
  static class PartitionBatchIterator implements CloseableIterator<VectorSchemaRoot> {
    private final Scan scan;
    private final dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator;
    private final BufferAllocator allocator;
    private final Closeable inputSource;
    private dev.vortex.relocated.org.apache.arrow.vector.ipc.ArrowReader currentReader;
    private VectorSchemaRoot currentRoot;
    private boolean hasPending = false;
    private boolean exhausted = false;

    PartitionBatchIterator(
        Scan scan,
        dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator,
        BufferAllocator allocator,
        Closeable inputSource) {
      this.scan = scan;
      this.vortexAllocator = vortexAllocator;
      this.allocator = allocator;
      this.inputSource = inputSource;
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
      try {
        closeCurrentRoot();
        if (currentReader != null) {
          currentReader.close();
          currentReader = null;
        }
        // Don't close shared allocators; they are managed for the lifetime of the process.
      } finally {
        // All scans over the input source are done; release its underlying streams.
        inputSource.close();
      }
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
        rowReader.newBatch(currentBatch);
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
      T nextRow = rowReader.read(batchIndex);
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
