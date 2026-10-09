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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
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
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.hash.Hasher;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
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
  // distinguishes merged equality delete sets from the per-file entries keyed by a file location
  private static final String EQ_DELETE_SET_KEY_PREFIX = "eq-delete-set|";

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

  /**
   * Checks if the merged equality delete set of a group of files may be cached as one entry.
   *
   * <p>Disabled by default. When enabled, and the merged set passes {@link #canCache(long)}, the
   * set is cached under a key for the exact group of files, so tasks whose data files reference the
   * same delete files share one set instead of each building it again. This pays off when many
   * tasks reference the same group of delete files. It costs memory and reads when groups only
   * partially overlap: a delete file shared by several different groups is read and held once per
   * group, while the per-file entries used otherwise hold it once.
   *
   * <p>Implementations that support caching may override this method to enable it.
   */
  protected boolean cacheEqualityDeleteSets() {
    return false;
  }

  /**
   * Loads the equality deletes of the given files, projected on the given schema, into one set.
   *
   * <p>Every scan task calls this with the equality delete files that apply to its data file. By
   * default the rows of each file are cached individually (see {@link #canCache(long)}) and merged
   * into a new {@link StructLikeSet} per call. When {@link #cacheEqualityDeleteSets()} is enabled
   * and the merged set may be cached, the merged set itself is cached under a key for the exact
   * files and the projection; its files are then read directly, not through their per-file entries,
   * which it supersedes. The returned set is only read afterwards; {@link StructLikeSet#contains}
   * is safe for concurrent readers.
   */
  @Override
  public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection) {
    List<DeleteFile> files = Lists.newArrayList(deleteFiles);
    if (cacheEqualityDeleteSets()) {
      long estimatedSize = estimateEqDeletesSize(files, projection);
      if (canCache(estimatedSize)) {
        // the loader reads the files directly rather than through their own cache entries: a
        // cache load must not start other cache loads (the delete worker threads would wait on the
        // same cache while this load holds it), and the merged set supersedes the per-file entries
        String cacheKey = eqDeleteSetKey(files, projection);
        return getOrLoad(
            cacheKey,
            () ->
                buildEqDeleteSet(
                    files, projection, deleteFile -> readEqDeletes(deleteFile, projection)),
            estimatedSize);
      }
    }

    return buildEqDeleteSet(
        files, projection, deleteFile -> getOrReadEqDeletes(deleteFile, projection));
  }

  private StructLikeSet buildEqDeleteSet(
      List<DeleteFile> deleteFiles,
      Schema projection,
      Function<DeleteFile, Iterable<StructLike>> readFile) {
    Iterable<Iterable<StructLike>> deletes = execute(deleteFiles, readFile);
    StructLikeSet deleteSet = StructLikeSet.create(projection.asStruct());
    Iterables.addAll(deleteSet, Iterables.concat(deletes));
    return deleteSet;
  }

  // the key of a merged equality delete set: a SHA-256 digest of the projection and the sorted file
  // locations, so its length does not grow with the number of files; a task whose delete files
  // differ in any file (sequence numbers can exclude some) gets its own set. A cryptographic digest
  // because two groups sharing a key would apply one group's deletes to the other's data.
  private static String eqDeleteSetKey(List<DeleteFile> deleteFiles, Schema projection) {
    List<String> locations = Lists.newArrayListWithCapacity(deleteFiles.size());
    for (DeleteFile deleteFile : deleteFiles) {
      locations.add(deleteFile.location());
    }

    Collections.sort(locations);
    Hasher hasher = Hashing.sha256().newHasher();
    putString(hasher, projection.asStruct().toString());
    for (String location : locations) {
      putString(hasher, location);
    }

    return EQ_DELETE_SET_KEY_PREFIX + locations.size() + "|" + hasher.hash();
  }

  // length-prefixed, so that no two different sequences of strings hash the same input
  private static void putString(Hasher hasher, String value) {
    hasher.putInt(value.length()).putString(value, StandardCharsets.UTF_8);
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
    byte[] bytes = new byte[length];
    try {
      IOUtil.readFully(inputFile, offset, bytes, 0, length);
      return PositionDeleteIndex.deserialize(bytes, dv);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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

    ReadBuilder<Record, ?> builder =
        FormatModelRegistry.readBuilder(format, Record.class, inputFile);
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

  // estimates the memory required to cache the merged equality deletes of several files (in bytes)
  private long estimateEqDeletesSize(List<DeleteFile> deleteFiles, Schema projection) {
    long size = 0;
    for (DeleteFile deleteFile : deleteFiles) {
      long fileSize = estimateEqDeletesSize(deleteFile, projection);
      if (fileSize == Long.MAX_VALUE || size > Long.MAX_VALUE - fileSize) {
        return Long.MAX_VALUE;
      }

      size += fileSize;
    }

    return size;
  }

  // estimates the memory required to cache equality deletes (in bytes)
  private long estimateEqDeletesSize(DeleteFile deleteFile, Schema projection) {
    try {
      long recordCount = deleteFile.recordCount();
      int recordSize = estimateRecordSize(projection);
      return Math.multiplyExact(recordCount, recordSize);
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
}
