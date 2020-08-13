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

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.Tasks;

/**
 * An index of {@link DeleteFile delete files} by sequence number.
 * <p>
 * Use {@link #builderFor(FileIO, Iterable)} to construct an index, and {@link #forDataFile(long, DataFile)} or
 * {@link #forEntry(ManifestEntry)} to get the the delete files to apply to a given data file.
 */
class DeleteFileIndex {
  private final Schema schema;
  private final Map<Integer, Types.StructType> partitionTypeById;
  private final Map<Integer, ThreadLocal<StructLikeWrapper>> wrapperById;
  private final long[] globalSeqs;
  private final DeleteFile[] globalDeletes;
  private final Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> sortedDeletesByPartition;

  DeleteFileIndex(Schema schema, Map<Integer, PartitionSpec> specsById, long[] globalSeqs, DeleteFile[] globalDeletes,
                  Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> sortedDeletesByPartition) {
    this.schema = schema;
    ImmutableMap.Builder<Integer, Types.StructType> builder = ImmutableMap.builder();
    specsById.forEach((specId, spec) -> builder.put(specId, spec.partitionType()));
    this.partitionTypeById = builder.build();
    this.wrapperById = Maps.newHashMap();
    this.globalSeqs = globalSeqs;
    this.globalDeletes = globalDeletes;
    this.sortedDeletesByPartition = sortedDeletesByPartition;
  }

  private StructLikeWrapper newWrapper(int specId) {
    return StructLikeWrapper.forType(partitionTypeById.get(specId));
  }

  private Pair<Integer, StructLikeWrapper> partition(int specId, StructLike struct) {
    ThreadLocal<StructLikeWrapper> wrapper = wrapperById.computeIfAbsent(specId,
        id -> ThreadLocal.withInitial(() -> newWrapper(id)));
    return Pair.of(specId, wrapper.get().set(struct));
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.sequenceNumber(), entry.file());
  }

  DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    Pair<Integer, StructLikeWrapper> partition = partition(file.specId(), file.partition());
    Pair<long[], DeleteFile[]> partitionDeletes = sortedDeletesByPartition.get(partition);

    Stream<DeleteFile> matchingDeletes;
    if (partitionDeletes == null) {
      matchingDeletes = limitBySequenceNumber(sequenceNumber, globalSeqs, globalDeletes);
    } else if (globalDeletes == null) {
      matchingDeletes = limitBySequenceNumber(sequenceNumber, partitionDeletes.first(), partitionDeletes.second());
    } else {
      matchingDeletes = Stream.concat(
          limitBySequenceNumber(sequenceNumber, globalSeqs, globalDeletes),
          limitBySequenceNumber(sequenceNumber, partitionDeletes.first(), partitionDeletes.second()));
    }

    return matchingDeletes
        .filter(deleteFile -> canContainDeletesForFile(file, deleteFile, schema))
        .toArray(DeleteFile[]::new);
  }

  private static boolean canContainDeletesForFile(DataFile dataFile, DeleteFile deleteFile, Schema schema) {
    switch (deleteFile.content()) {
      case POSITION_DELETES:
        // check that the delete file can contain the data file's file_path
        Map<Integer, ByteBuffer> lowers = deleteFile.lowerBounds();
        Map<Integer, ByteBuffer> uppers = deleteFile.upperBounds();
        if (lowers == null || uppers == null) {
          return true;
        }

        Type pathType = MetadataColumns.DELETE_FILE_PATH.type();
        int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
        ByteBuffer lower = lowers.get(pathId);
        if (lower != null &&
            Comparators.charSequences().compare(dataFile.path(), Conversions.fromByteBuffer(pathType, lower)) < 0) {
          return false;
        }

        ByteBuffer upper = uppers.get(pathId);
        if (upper != null &&
            Comparators.charSequences().compare(dataFile.path(), Conversions.fromByteBuffer(pathType, upper)) > 0) {
          return false;
        }

        break;

      case EQUALITY_DELETES:
        if (dataFile.lowerBounds() == null || dataFile.upperBounds() == null ||
            deleteFile.lowerBounds() == null || deleteFile.upperBounds() == null) {
          return true;
        }

        Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
        Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();
        Map<Integer, ByteBuffer> deleteLowers = deleteFile.lowerBounds();
        Map<Integer, ByteBuffer> deleteUppers = deleteFile.upperBounds();

        for (int id : deleteFile.equalityFieldIds()) {
          Type type = schema.findType(id);
          if (!type.isPrimitiveType()) {
            return true;
          }

          if (!rangesOverlap(type.asPrimitiveType(),
              dataLowers.get(id), dataUppers.get(id), deleteLowers.get(id), deleteUppers.get(id))) {
            return false;
          }
        }
        break;
    }

    return true;
  }

  private static <T> boolean rangesOverlap(Type.PrimitiveType type,
                                           ByteBuffer dataLower, ByteBuffer dataUpper,
                                           ByteBuffer deleteLower, ByteBuffer deleteUpper) {
    Comparator<T> comparator = Comparators.forType(type);
    T low = Conversions.fromByteBuffer(type, dataLower);
    T high = Conversions.fromByteBuffer(type, dataUpper);

    if (contains(comparator, low, high, Conversions.fromByteBuffer(type, deleteLower))) {
      return true;
    }

    if (contains(comparator, low, high, Conversions.fromByteBuffer(type, deleteUpper))) {
      return true;
    }

    return false;
  }

  private static <T> boolean contains(Comparator<T> comparator, T low, T high, T value) {
    return comparator.compare(low, value) <= 0 && comparator.compare(value, high) <= 0;
  }

  private static Stream<DeleteFile> limitBySequenceNumber(long sequenceNumber, long[] seqs, DeleteFile[] files) {
    if (files == null) {
      return Stream.empty();
    }

    int pos = Arrays.binarySearch(seqs, sequenceNumber);
    int start;
    if (pos < 0) {
      // the sequence number was not found, where it would be inserted is -(pos + 1)
      start = -(pos + 1);
    } else {
      // the sequence number was found, but may not be the first
      // find the first delete file with the given sequence number by decrementing the position
      start = pos;
      while (start > 0 && seqs[start - 1] >= sequenceNumber) {
        start -= 1;
      }
    }

    return Arrays.stream(files, start, files.length);
  }

  static Builder builderFor(FileIO io, Iterable<ManifestFile> deleteManifests) {
    return new Builder(io, Sets.newHashSet(deleteManifests));
  }

  static class Builder {
    private final FileIO io;
    private final Set<ManifestFile> deleteManifests;
    private Schema schema = null;
    private Map<Integer, PartitionSpec> specsById = null;
    private Expression dataFilter = Expressions.alwaysTrue();
    private Expression partitionFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private ExecutorService executorService = null;

    Builder(FileIO io, Set<ManifestFile> deleteManifests) {
      this.io = io;
      this.deleteManifests = Sets.newHashSet(deleteManifests);
    }

    Builder schema(Schema tableSchema) {
      this.schema = tableSchema;
      return this;
    }

    Builder specsById(Map<Integer, PartitionSpec> newSpecsById) {
      this.specsById = newSpecsById;
      return this;
    }

    Builder filterData(Expression newDataFilter) {
      this.dataFilter = Expressions.and(dataFilter, newDataFilter);
      return this;
    }

    Builder filterPartitions(Expression newPartitionFilter) {
      this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
      return this;
    }

    Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    Builder planWith(ExecutorService newExecutorService) {
      this.executorService = newExecutorService;
      return this;
    }

    DeleteFileIndex build() {
      // read all of the matching delete manifests in parallel and accumulate the matching files in a queue
      Queue<ManifestEntry<DeleteFile>> deleteEntries = new ConcurrentLinkedQueue<>();
      Tasks.foreach(deleteManifestReaders())
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(executorService)
          .run(deleteFile -> {
            try (CloseableIterable<ManifestEntry<DeleteFile>> reader = deleteFile) {
              for (ManifestEntry<DeleteFile> entry : reader) {
                // copy with stats for better filtering against data file stats
                deleteEntries.add(entry.copy());
              }
            } catch (IOException e) {
              throw new RuntimeIOException("Failed to close", e);
            }
          });

      // build a map from (specId, partition) to delete file entries
      ListMultimap<Pair<Integer, StructLikeWrapper>, ManifestEntry<DeleteFile>> deleteFilesByPartition =
          Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
      for (ManifestEntry<DeleteFile> entry : deleteEntries) {
        int specId = entry.file().specId();
        StructLikeWrapper wrapper = StructLikeWrapper.forType(specsById.get(specId).partitionType())
            .set(entry.file().partition());
        deleteFilesByPartition.put(Pair.of(specId, wrapper), entry);
      }

      // sort the entries in each map value by sequence number and split into sequence numbers and delete files lists
      Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> sortedDeletesByPartition = Maps.newHashMap();
      // also, separate out equality deletes in an unpartitioned spec that should be applied globally
      long[] globalApplySeqs = null;
      DeleteFile[] globalDeletes = null;
      for (Pair<Integer, StructLikeWrapper> partition : deleteFilesByPartition.keySet()) {
        if (specsById.get(partition.first()).isUnpartitioned()) {
          Preconditions.checkState(globalDeletes == null, "Detected multiple partition specs with no partitions");

          List<Pair<Long, DeleteFile>> eqFilesSortedBySeq = deleteFilesByPartition.get(partition).stream()
              .filter(entry -> entry.file().content() == FileContent.EQUALITY_DELETES)
              .map(entry ->
                  // a delete file is indexed by the sequence number it should be applied to
                  Pair.of(entry.sequenceNumber() - 1, entry.file()))
              .sorted(Comparator.comparingLong(Pair::first))
              .collect(Collectors.toList());

          globalApplySeqs = eqFilesSortedBySeq.stream().mapToLong(Pair::first).toArray();
          globalDeletes = eqFilesSortedBySeq.stream().map(Pair::second).toArray(DeleteFile[]::new);

          List<Pair<Long, DeleteFile>> posFilesSortedBySeq = deleteFilesByPartition.get(partition).stream()
              .filter(entry -> entry.file().content() == FileContent.POSITION_DELETES)
              .map(entry -> Pair.of(entry.sequenceNumber(), entry.file()))
              .sorted(Comparator.comparingLong(Pair::first))
              .collect(Collectors.toList());

          long[] seqs = posFilesSortedBySeq.stream().mapToLong(Pair::first).toArray();
          DeleteFile[] files = posFilesSortedBySeq.stream().map(Pair::second).toArray(DeleteFile[]::new);

          sortedDeletesByPartition.put(partition, Pair.of(seqs, files));

        } else {
          List<Pair<Long, DeleteFile>> filesSortedBySeq = deleteFilesByPartition.get(partition).stream()
              .map(entry -> {
                // a delete file is indexed by the sequence number it should be applied to
                long applySeq = entry.sequenceNumber() -
                    (entry.file().content() == FileContent.EQUALITY_DELETES ? 1 : 0);
                return Pair.of(applySeq, entry.file());
              })
              .sorted(Comparator.comparingLong(Pair::first))
              .collect(Collectors.toList());

          long[] seqs = filesSortedBySeq.stream().mapToLong(Pair::first).toArray();
          DeleteFile[] files = filesSortedBySeq.stream().map(Pair::second).toArray(DeleteFile[]::new);

          sortedDeletesByPartition.put(partition, Pair.of(seqs, files));
        }
      }

      return new DeleteFileIndex(schema, specsById, globalApplySeqs, globalDeletes, sortedDeletesByPartition);
    }

    private Iterable<CloseableIterable<ManifestEntry<DeleteFile>>> deleteManifestReaders() {
      LoadingCache<Integer, ManifestEvaluator> evalCache = specsById == null ? null :
          Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            return ManifestEvaluator.forPartitionFilter(
                Expressions.and(partitionFilter, Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                spec, caseSensitive);
          });

      Iterable<ManifestFile> matchingManifests = evalCache == null ? deleteManifests :
          Iterables.filter(deleteManifests, manifest ->
              manifest.content() == ManifestContent.DELETES &&
                  (manifest.hasAddedFiles() || manifest.hasDeletedFiles()) &&
                  evalCache.get(manifest.partitionSpecId()).eval(manifest));

      return Iterables.transform(
          matchingManifests,
          manifest ->
              ManifestFiles.readDeleteManifest(manifest, io, specsById)
                  .filterRows(dataFilter)
                  .filterPartitions(partitionFilter)
                  .caseSensitive(caseSensitive)
                  .liveEntries()
      );
    }
  }
}
