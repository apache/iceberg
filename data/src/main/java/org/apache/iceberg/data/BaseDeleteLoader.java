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
package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteIndexUtil;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseDeleteLoader implements DeleteLoader {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDeleteLoader.class);
  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();

  private final Function<DeleteFile, InputFile> loadInputFile;
  private final ExecutorService workerPool;

  public BaseDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
    this(loadInputFile, ThreadPools.getDeleteWorkerPool());
  }

  public BaseDeleteLoader(
      Function<DeleteFile, InputFile> loadInputFile, ExecutorService workerPool) {
    this.loadInputFile = loadInputFile;
    this.workerPool = workerPool;
  }

  /**
   * Checks if the given number of bytes can be cached.
   *
   * <p>Implementations should override this method if they support caching. It is also recommended
   * to use the provided size as a guideline to decide whether the value is eligible for caching.
   * For instance, it may be beneficial to discard values that are too large to optimize the cache
   * performance and utilization.
   */
  protected boolean canCache(long size) {
    return false;
  }

  /**
   * Gets the cached value for the key or populates the cache with a new mapping.
   *
   * <p>If the value for the specified key is in the cache, it should be returned. If the value is
   * not in the cache, implementations should compute the value using the provided supplier, cache
   * it, and then return it.
   *
   * <p>This method will be called only if {@link #canCache(long)} returned true.
   */
  protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
    throw new UnsupportedOperationException(getClass().getName() + " does not support caching");
  }

  @Override
  public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection) {
    Iterable<Iterable<StructLike>> deletes =
        execute(deleteFiles, deleteFile -> getOrReadEqDeletes(deleteFile, projection));
    StructLikeSet deleteSet = StructLikeSet.create(projection.asStruct());
    Iterables.addAll(deleteSet, Iterables.concat(deletes));
    return deleteSet;
  }

  private Iterable<StructLike> getOrReadEqDeletes(DeleteFile deleteFile, Schema projection) {
    long estimatedSize = estimateEqDeletesSize(deleteFile, projection);
    if (canCache(estimatedSize)) {
      String cacheKey = deleteFile.location();
      return getOrLoad(cacheKey, () -> readEqDeletes(deleteFile, projection), estimatedSize);
    } else {
      return readEqDeletes(deleteFile, projection);
    }
  }

  private Iterable<StructLike> readEqDeletes(DeleteFile deleteFile, Schema projection) {
    CloseableIterable<Record> deletes = openDeletes(deleteFile, projection);
    CloseableIterable<Record> copiedDeletes = CloseableIterable.transform(deletes, Record::copy);
    CloseableIterable<StructLike> copiedDeletesAsStructs = toStructs(copiedDeletes, projection);
    return materialize(copiedDeletesAsStructs);
  }

  private CloseableIterable<StructLike> toStructs(
      CloseableIterable<Record> records, Schema schema) {
    InternalRecordWrapper wrapper = new InternalRecordWrapper(schema.asStruct());
    return CloseableIterable.transform(records, wrapper::copyFor);
  }

  // materializes the iterable and releases resources so that the result can be cached
  private <T> Iterable<T> materialize(CloseableIterable<T> iterable) {
    try (CloseableIterable<T> closeableIterable = iterable) {
      return ImmutableList.copyOf(closeableIterable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close iterable", e);
    }
  }

  /**
   * Loads the content of a deletion vector or position delete files for a given data file path into
   * a position index.
   *
   * <p>The deletion vector is currently loaded without caching as the existing Puffin reader
   * requires at least 3 requests to fetch the entire file. Caching a single deletion vector may
   * only be useful when multiple data file splits are processed on the same node, which is unlikely
   * as task locality is not guaranteed.
   *
   * <p>For position delete files, however, there is no efficient way to read deletes for a
   * particular data file. Therefore, caching may be more effective as such delete files potentially
   * apply to many data files, especially in unpartitioned tables and tables with deep partitions.
   * If a position delete file qualifies for caching, this method will attempt to cache a position
   * index for each referenced data file.
   *
   * @param deleteFiles a deletion vector or position delete files
   * @param filePath the data file path for which to load deletes
   * @return a position delete index for the provided data file path
   */
  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
      validateDV(dv, filePath);
      return readDV(dv);
    } else {
      return getOrReadPosDeletes(deleteFiles, filePath);
    }
  }

  private PositionDeleteIndex readDV(DeleteFile dv) {
    LOG.trace("Opening DV file {}", dv.location());
    InputFile inputFile = loadInputFile.apply(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = readBytes(inputFile, offset, length);
    return PositionDeleteIndex.deserialize(bytes, dv);
  }

  private PositionDeleteIndex getOrReadPosDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    Iterable<PositionDeleteIndex> deletes =
        execute(deleteFiles, deleteFile -> getOrReadPosDeletes(deleteFile, filePath));
    return PositionDeleteIndexUtil.merge(deletes);
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private PositionDeleteIndex getOrReadPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    long estimatedSize = estimatePosDeletesSize(deleteFile);
    if (canCache(estimatedSize)) {
      String cacheKey = deleteFile.location();
      CharSequenceMap<PositionDeleteIndex> indexes =
          getOrLoad(cacheKey, () -> readPosDeletes(deleteFile), estimatedSize);
      return indexes.getOrDefault(filePath, PositionDeleteIndex.empty());
    } else {
      return readPosDeletes(deleteFile, filePath);
    }
  }

  private CharSequenceMap<PositionDeleteIndex> readPosDeletes(DeleteFile deleteFile) {
    CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA);
    return Deletes.toPositionIndexes(deletes, deleteFile);
  }

  private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    Expression filter = Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), filePath);
    CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA, filter);
    return Deletes.toPositionIndex(filePath, deletes, deleteFile);
  }

  private CloseableIterable<Record> openDeletes(DeleteFile deleteFile, Schema projection) {
    return openDeletes(deleteFile, projection, null /* no filter */);
  }

  private CloseableIterable<Record> openDeletes(
      DeleteFile deleteFile, Schema projection, Expression filter) {

    FileFormat format = deleteFile.format();
    LOG.trace("Opening delete file {}", deleteFile.location());
    InputFile inputFile = loadInputFile.apply(deleteFile);

    ReadBuilder<?, Record> builder = GenericFileAccessor.INSTANCE.readBuilder(format, inputFile);
    return builder.project(projection).reuseContainers().filter(filter).build();
  }

  private <I, O> Iterable<O> execute(Iterable<I> objects, Function<I, O> func) {
    Queue<O> output = new ConcurrentLinkedQueue<>();

    Tasks.foreach(objects)
        .executeWith(workerPool)
        .stopOnFailure()
        .onFailure((object, exc) -> LOG.error("Failed to process {}", object, exc))
        .run(object -> output.add(func.apply(object)));

    return output;
  }

  // estimates the memory required to cache position deletes (in bytes)
  private long estimatePosDeletesSize(DeleteFile deleteFile) {
    // the space consumption highly depends on the nature of deleted positions (sparse vs compact)
    // testing shows Roaring bitmaps require around 8 bits (1 byte) per value on average
    return deleteFile.recordCount();
  }

  // estimates the memory required to cache equality deletes (in bytes)
  private long estimateEqDeletesSize(DeleteFile deleteFile, Schema projection) {
    try {
      long recordCount = deleteFile.recordCount();
      int recordSize = estimateRecordSize(projection);
      return LongMath.checkedMultiply(recordCount, recordSize);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  private int estimateRecordSize(Schema schema) {
    return schema.columns().stream().mapToInt(TypeUtil::estimateSize).sum();
  }

  private void validateDV(DeleteFile dv, CharSequence filePath) {
    Preconditions.checkArgument(
        dv.contentOffset() != null,
        "Invalid DV, offset cannot be null: %s",
        ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(
        dv.contentSizeInBytes() != null,
        "Invalid DV, length is null: %s",
        ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(
        dv.contentSizeInBytes() <= Integer.MAX_VALUE,
        "Can't read DV larger than 2GB: %s",
        dv.contentSizeInBytes());
    Preconditions.checkArgument(
        filePath.toString().equals(dv.referencedDataFile()),
        "DV is expected to reference %s, not %s",
        filePath,
        dv.referencedDataFile());
  }

  private static byte[] readBytes(InputFile inputFile, long offset, int length) {
    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] bytes = new byte[length];

      if (stream instanceof RangeReadable) {
        RangeReadable rangeReadable = (RangeReadable) stream;
        rangeReadable.readFully(offset, bytes);
      } else {
        stream.seek(offset);
        ByteStreams.readFully(stream, bytes);
      }

      return bytes;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
