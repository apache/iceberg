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
package org.apache.iceberg.arrow.vectorized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.parquet.schema.MessageType;

/**
 * Reads an Iceberg position delete file directly into a {@link PositionDeleteIndex} via Arrow
 * vectors, without materializing intermediate {@link org.apache.iceberg.data.Record} instances or
 * boxing positions into {@link Long}.
 *
 * <p>Compared with iterating a record-based reader and inserting positions one at a time, this
 * reader:
 *
 * <ul>
 *   <li>reads the {@code pos} column directly from a {@link BigIntVector} as primitive longs,
 *       eliminating boxing and per-row {@code GenericRecord} allocations,
 *   <li>coalesces consecutive matching positions into range inserts via {@link
 *       PositionDeleteIndex#delete(long, long)}, which the underlying Roaring bitmap can apply in
 *       bulk, and
 *   <li>only projects the {@code file_path} column when the caller is filtering, so a
 *       single-data-file delete file is decoded with a single column.
 * </ul>
 *
 * <p>This class is engine-agnostic: it operates on a Parquet {@link InputFile} and does not require
 * a table scan. It is the building block for v2 / v3 delete-file support in {@link ArrowReader}
 * (see <a href="https://github.com/apache/iceberg/issues/2487">#2487</a>).
 */
public final class VectorizedPositionDeleteReader {

  /**
   * Default Arrow record-batch size for decoding position-delete files. Larger than {@link
   * VectorizedArrowReader#DEFAULT_BATCH_SIZE} because position-delete files project at most two
   * narrow columns ({@code file_path} and {@code pos}), so a larger batch amortizes the per-batch
   * decoding cost without materially increasing memory pressure.
   */
  public static final int DEFAULT_BATCH_SIZE = 1 << 13;

  private static final Schema POS_ONLY_SCHEMA = new Schema(MetadataColumns.DELETE_FILE_POS);
  private static final Schema FULL_SCHEMA = DeleteSchemaUtil.pathPosSchema();

  private VectorizedPositionDeleteReader() {}

  /**
   * Reads a position delete file and returns the positions for the given data file as a {@link
   * PositionDeleteIndex}.
   *
   * <p>See {@link #read(InputFile, CharSequence, DeleteFile, int)} for invariants and behavior;
   * this overload uses {@link #DEFAULT_BATCH_SIZE}.
   *
   * @param file the delete file to read
   * @param dataLocation the data file path to filter by, or {@code null} to include all positions
   *     in the delete file
   * @param deleteFile the delete file metadata recorded with the returned index, may be {@code
   *     null}
   * @return a {@link PositionDeleteIndex} containing all matching positions
   */
  public static PositionDeleteIndex read(
      InputFile file, CharSequence dataLocation, DeleteFile deleteFile) {
    return read(file, dataLocation, deleteFile, DEFAULT_BATCH_SIZE);
  }

  /**
   * Reads a position delete file and returns the positions for the given data file as a {@link
   * PositionDeleteIndex}.
   *
   * <p>The call blocks on the calling thread until the file is fully decoded; concurrent calls are
   * independent (each builds its own Parquet reader and Arrow allocator child). The returned index
   * is mutable and is not safe for concurrent mutation by multiple threads.
   *
   * <p>When {@code dataLocation} is non-null, rows are filtered by comparing {@code file_path}
   * byte-for-byte against {@code dataLocation.toString().getBytes(UTF-8)}. Callers should pass the
   * exact {@link String} that Iceberg used to write the delete file (typically the value returned
   * by {@code DataFile#location()}); a path that is logically equivalent but encoded differently
   * (e.g. unicode-normalized differently, or with a different scheme/host) matches no rows and
   * yields an empty index.
   *
   * <p>When {@code dataLocation} is null, every row in the file is added to the same index
   * regardless of its {@code file_path}. This is the intended mode for single-data-file delete
   * files (the typical DV case); on a delete file referencing multiple data files it returns the
   * union of all rows' positions, which is rarely what callers want.
   *
   * <p>The file is expected to conform to the Iceberg position-delete spec: {@code pos} is required
   * and {@code file_path} is required when present in the projected schema. The {@code pos} column
   * is read directly from the Arrow data buffer for performance, so behavior on a malformed file
   * with null {@code pos} values is undefined (the index may contain arbitrary positions). Engines
   * that ingest delete files written outside Iceberg should validate them upstream.
   *
   * @param file the delete file to read
   * @param dataLocation the data file path to filter by, or {@code null} to include all positions
   *     in the delete file regardless of {@code file_path}
   * @param deleteFile the delete file metadata recorded with the returned index, may be {@code
   *     null}
   * @param batchSize the Arrow batch size to use when decoding the file
   * @return a mutable {@link PositionDeleteIndex} containing all matching positions
   */
  public static PositionDeleteIndex read(
      InputFile file, CharSequence dataLocation, DeleteFile deleteFile, int batchSize) {
    Preconditions.checkArgument(file != null, "Invalid input file: null");
    Preconditions.checkArgument(batchSize > 0, "Invalid batch size: %s", batchSize);

    Schema projection = dataLocation == null ? POS_ONLY_SCHEMA : FULL_SCHEMA;
    PositionDeleteIndex index = PositionDeleteIndex.create(deleteFile);
    RangeCoalescer coalescer = new RangeCoalescer(index);

    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(file)
            .project(projection)
            .recordsPerBatch(batchSize)
            .createBatchedReaderFunc(fileSchema -> buildBatchReader(projection, fileSchema))
            .build()) {

      try (CloseableIterator<ColumnarBatch> it = batches.iterator()) {
        if (dataLocation == null) {
          appendAll(it, coalescer);
        } else {
          appendFiltered(it, dataLocation, coalescer);
        }
      }
      coalescer.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read position delete file: " + file.location(), e);
    }

    return index;
  }

  /**
   * Reads a position delete file and returns one {@link PositionDeleteIndex} per data file path
   * referenced by the delete file. See {@link #readAllByDataFile(InputFile, DeleteFile, int)} for
   * details; this overload uses {@link #DEFAULT_BATCH_SIZE}.
   *
   * @param file the delete file to read
   * @param deleteFile the delete file metadata recorded with each returned index, may be {@code
   *     null}
   * @return a map of data file path to {@link PositionDeleteIndex}
   */
  public static CharSequenceMap<PositionDeleteIndex> readAllByDataFile(
      InputFile file, DeleteFile deleteFile) {
    return readAllByDataFile(file, deleteFile, DEFAULT_BATCH_SIZE);
  }

  /**
   * Reads a position delete file and returns one {@link PositionDeleteIndex} per data file path
   * referenced by the delete file.
   *
   * <p>Position delete files are required to be sorted by {@code (file_path, pos)} ascending, so
   * rows for the same data file path arrive in contiguous runs. The reader exploits this by
   * tracking the active path's bytes between rows and only finalizing the run on a path change, but
   * it also handles unsorted files correctly by looking up an existing entry in the result map
   * before creating a new one.
   *
   * <p>Each returned index is mutable and is not safe for concurrent mutation by multiple threads.
   *
   * @param file the delete file to read
   * @param deleteFile the delete file metadata recorded with each returned index, may be {@code
   *     null}
   * @param batchSize the Arrow batch size to use when decoding the file
   * @return a map of data file path to {@link PositionDeleteIndex}
   */
  public static CharSequenceMap<PositionDeleteIndex> readAllByDataFile(
      InputFile file, DeleteFile deleteFile, int batchSize) {
    Preconditions.checkArgument(file != null, "Invalid input file: null");
    Preconditions.checkArgument(batchSize > 0, "Invalid batch size: %s", batchSize);

    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();

    try (CloseableIterable<ColumnarBatch> batches =
        Parquet.read(file)
            .project(FULL_SCHEMA)
            .recordsPerBatch(batchSize)
            .createBatchedReaderFunc(fileSchema -> buildBatchReader(FULL_SCHEMA, fileSchema))
            .build()) {

      try (CloseableIterator<ColumnarBatch> it = batches.iterator()) {
        appendGroupedByPath(it, indexes, deleteFile);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read position delete file: " + file.location(), e);
    }

    return indexes;
  }

  /**
   * Coalescing append for the no-filter case: every row in every batch is added. Consecutive
   * positions are merged into a single range insert.
   */
  private static void appendAll(CloseableIterator<ColumnarBatch> it, RangeCoalescer coalescer) {
    while (it.hasNext()) {
      try (ColumnarBatch batch = it.next()) {
        BigIntVector posVec = (BigIntVector) posColumn(batch).getArrowVector();
        ArrowBuf posBuf = posVec.getDataBuffer();
        int rows = batch.numRows();
        for (int i = 0; i < rows; i++) {
          coalescer.accept(readLong(posBuf, i));
        }
      }
    }
  }

  /**
   * Coalescing append for the filtered case: only rows whose {@code file_path} equals {@code
   * dataLocation} contribute. The run is broken whenever a non-matching row is seen so we never
   * coalesce across a gap caused by another data file.
   */
  private static void appendFiltered(
      CloseableIterator<ColumnarBatch> it, CharSequence dataLocation, RangeCoalescer coalescer) {
    byte[] target = dataLocation.toString().getBytes(StandardCharsets.UTF_8);

    while (it.hasNext()) {
      try (ColumnarBatch batch = it.next()) {
        VarCharVector pathVec = (VarCharVector) pathColumn(batch).getArrowVector();
        BigIntVector posVec = (BigIntVector) posColumn(batch).getArrowVector();
        ArrowBuf pathBuf = pathVec.getDataBuffer();
        ArrowBuf posBuf = posVec.getDataBuffer();
        int rows = batch.numRows();
        for (int i = 0; i < rows; i++) {
          if (matches(pathVec, pathBuf, i, target)) {
            coalescer.accept(readLong(posBuf, i));
          } else {
            coalescer.breakRun();
          }
        }
      }
    }
  }

  /**
   * Coalescing append for the multi-data-file case: rows are grouped by {@code file_path} into
   * separate {@link PositionDeleteIndex} instances. Position delete files are required to be sorted
   * by {@code (file_path, pos)}, so paths arrive in contiguous runs; the active path's bytes are
   * tracked and a run is finalized only on a path change. If a previously seen path reappears (an
   * unsorted file), the existing index is reused so positions are not lost.
   *
   * <p>To avoid a 50+-byte comparison per row in the common single-data-file case, the inner loop
   * uses a length-plus-first-byte fast filter via {@link #endOfPathRun}: only when the cheap check
   * suggests a transition do we materialize the new path bytes and verify with a full comparison.
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  private static void appendGroupedByPath(
      CloseableIterator<ColumnarBatch> it,
      CharSequenceMap<PositionDeleteIndex> indexes,
      DeleteFile deleteFile) {
    byte[] currentPath = null;
    RangeCoalescer coalescer = null;

    while (it.hasNext()) {
      try (ColumnarBatch batch = it.next()) {
        VarCharVector pathVec = (VarCharVector) pathColumn(batch).getArrowVector();
        BigIntVector posVec = (BigIntVector) posColumn(batch).getArrowVector();
        ArrowBuf pathBuf = pathVec.getDataBuffer();
        ArrowBuf posBuf = posVec.getDataBuffer();
        int rows = batch.numRows();
        int cursor = 0;
        while (cursor < rows) {
          if (currentPath == null || !matches(pathVec, pathBuf, cursor, currentPath)) {
            if (coalescer != null) {
              coalescer.flush();
            }
            currentPath = readPath(pathVec, pathBuf, cursor);
            String pathKey = new String(currentPath, StandardCharsets.UTF_8);
            PositionDeleteIndex existing = indexes.get(pathKey);
            if (existing == null) {
              existing = PositionDeleteIndex.create(deleteFile);
              indexes.put(pathKey, existing);
            }
            coalescer = new RangeCoalescer(existing);
          }

          int runEnd = endOfPathRun(pathVec, pathBuf, cursor, rows, currentPath);
          for (int i = cursor; i < runEnd; i++) {
            coalescer.accept(readLong(posBuf, i));
          }
          cursor = runEnd;
        }
      }
    }

    if (coalescer != null) {
      coalescer.flush();
    }
  }

  /**
   * Finds the smallest row index {@code i} in {@code [start + 1, rows]} for which the {@code
   * file_path} differs from {@code currentPath}, or returns {@code rows} if no such row exists. The
   * caller must guarantee that {@code currentPath} matches row {@code start}.
   *
   * <p>For each row, the cheap filter checks (a) non-null, (b) the same value length, and (c) the
   * same first byte. Rows that pass all three are presumed to share the path with row {@code start}
   * (which holds for sorted position-delete files since two adjacent paths cannot share both length
   * and leading byte without being equal in their span). The run length is verified by a full
   * byte-for-byte comparison of the last row in the candidate run; if it disagrees, the method
   * falls back to a full per-row comparison so unsorted files still produce a correct grouping.
   * Empty paths skip the first-byte check.
   */
  private static int endOfPathRun(
      VarCharVector pathVec, ArrowBuf dataBuf, int start, int rows, byte[] currentPath) {
    int len = currentPath.length;
    int cursor = start + 1;

    if (len == 0) {
      while (cursor < rows && !pathVec.isNull(cursor) && pathVec.getValueLength(cursor) == 0) {
        cursor++;
      }
      return cursor;
    }

    byte first = currentPath[0];
    while (cursor < rows) {
      if (pathVec.isNull(cursor)) {
        return cursor;
      }
      if (pathVec.getValueLength(cursor) != len) {
        return cursor;
      }
      if (dataBuf.getByte(pathVec.getStartOffset(cursor)) != first) {
        return cursor;
      }
      cursor++;
    }

    // Fast filter accepted every row. Confirm by verifying the last row's full path.
    if (matches(pathVec, dataBuf, rows - 1, currentPath)) {
      return rows;
    }

    // Same length and first byte but a divergent middle byte (rare, possible only for unsorted
    // files). Re-scan with full byte comparisons to find the actual boundary.
    for (int j = start + 1; j < rows; j++) {
      if (!matches(pathVec, dataBuf, j, currentPath)) {
        return j;
      }
    }
    return rows;
  }

  /**
   * Reads the {@code file_path} bytes for {@code row} into a freshly-allocated byte array. Used to
   * track the active path between batches in {@link #appendGroupedByPath}.
   */
  private static byte[] readPath(VarCharVector pathVec, ArrowBuf dataBuf, int row) {
    int length = pathVec.getValueLength(row);
    byte[] bytes = new byte[length];
    long offset = pathVec.getStartOffset(row);
    for (int i = 0; i < length; i++) {
      bytes[i] = dataBuf.getByte(offset + i);
    }
    return bytes;
  }

  /**
   * Tracks the active run of consecutive positions and emits range inserts to the underlying index.
   * Single-threaded; intended for one decoding pass per delete file.
   */
  private static final class RangeCoalescer {
    private final PositionDeleteIndex index;
    private boolean hasRun;
    private long runStart;
    private long runEnd;

    RangeCoalescer(PositionDeleteIndex index) {
      this.index = index;
    }

    void accept(long pos) {
      if (!hasRun) {
        runStart = pos;
        runEnd = pos;
        hasRun = true;
      } else if (pos == runEnd + 1) {
        runEnd = pos;
      } else {
        emit();
        runStart = pos;
        runEnd = pos;
      }
    }

    void breakRun() {
      if (hasRun) {
        emit();
        hasRun = false;
      }
    }

    void flush() {
      if (hasRun) {
        emit();
        hasRun = false;
      }
    }

    private void emit() {
      if (runStart == runEnd) {
        index.delete(runStart);
      } else {
        index.delete(runStart, runEnd + 1);
      }
    }
  }

  /**
   * Returns true if {@code pathVec[row]} equals {@code target} byte-for-byte. Reads the UTF-8 bytes
   * directly from the Arrow data buffer; no String or byte[] allocation per row.
   */
  private static boolean matches(VarCharVector pathVec, ArrowBuf dataBuf, int row, byte[] target) {
    if (pathVec.isNull(row)) {
      return false;
    }

    int length = pathVec.getValueLength(row);
    if (length != target.length) {
      return false;
    }

    long offset = pathVec.getStartOffset(row);
    for (int i = 0; i < length; i++) {
      if (dataBuf.getByte(offset + i) != target[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Reads a {@code long} directly from the value buffer at row {@code i}, bypassing {@link
   * BigIntVector#get(int)}'s null check. The {@code pos} column in a position delete file is
   * required and therefore never null; reading the data buffer is both correct and avoids one
   * branch per row.
   */
  private static long readLong(ArrowBuf posBuf, int row) {
    return posBuf.getLong((long) row * BigIntVector.TYPE_WIDTH);
  }

  /**
   * The {@code pos} column is the last column in both projections: index 0 for {@link
   * #POS_ONLY_SCHEMA}, index 1 for {@link #FULL_SCHEMA}.
   */
  private static ColumnVector posColumn(ColumnarBatch batch) {
    return batch.column(batch.numCols() - 1);
  }

  /** Only valid when {@link #FULL_SCHEMA} is projected; {@code file_path} is at index 0. */
  private static ColumnVector pathColumn(ColumnarBatch batch) {
    return batch.column(0);
  }

  private static VectorizedReader<?> buildBatchReader(Schema projection, MessageType fileSchema) {
    return (VectorizedReader<?>)
        TypeWithSchemaVisitor.visit(
            projection.asStruct(),
            fileSchema,
            new VectorizedReaderBuilder(
                projection,
                fileSchema,
                NullCheckingForGet.NULL_CHECKING_ENABLED,
                ImmutableMap.of(),
                ArrowBatchReader::new));
  }
}
