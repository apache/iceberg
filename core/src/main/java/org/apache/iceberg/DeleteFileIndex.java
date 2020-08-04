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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.Tasks;

/**
 * An index of {@link DeleteFile delete files} by sequence number.
 * <p>
 * Use {@link #builderFor(FileIO, Iterable)} to construct an index, and {@link #forDataFile(int, long, DataFile)} or
 * {@link #forEntry(int, ManifestEntry)} to get the the delete files to apply to a given data file.
 */
class DeleteFileIndex {
  private static final DeleteFile[] NO_DELETE_FILES = new DeleteFile[0];

  private final long[] globalSeqs;
  private final DeleteFile[] globalDeletes;
  private final Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> index;
  private final ThreadLocal<StructLikeWrapper> lookupWrapper = ThreadLocal.withInitial(
      () -> StructLikeWrapper.wrap(null));

  DeleteFileIndex(long[] globalSeqs, DeleteFile[] globalDeletes,
                  Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> index) {
    this.globalSeqs = globalSeqs;
    this.globalDeletes = globalDeletes;
    this.index = index;
  }

  DeleteFile[] forEntry(int specId, ManifestEntry<DataFile> entry) {
    return forDataFile(specId, entry.sequenceNumber(), entry.file());
  }

  DeleteFile[] forDataFile(int specId, long sequenceNumber, DataFile file) {
    Pair<long[], DeleteFile[]> partitionDeletes = index.get(Pair.of(specId, lookupWrapper.get().set(file.partition())));
    if (partitionDeletes == null) {
      return limitBySequenceNumber(sequenceNumber, globalSeqs, globalDeletes);
    } else if (globalDeletes == null) {
      return limitBySequenceNumber(sequenceNumber, partitionDeletes.first(), partitionDeletes.second());
    } else {
      return Stream.concat(
          Stream.of(limitBySequenceNumber(sequenceNumber, globalSeqs, globalDeletes)),
          Stream.of(limitBySequenceNumber(sequenceNumber, partitionDeletes.first(), partitionDeletes.second()))
      ).toArray(DeleteFile[]::new);
    }
  }

  private static DeleteFile[] limitBySequenceNumber(long sequenceNumber, long[] seqs, DeleteFile[] files) {
    if (files == null) {
      return NO_DELETE_FILES;
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

    return Arrays.copyOfRange(files, start, files.length);
  }

  static Builder builderFor(FileIO io, Iterable<ManifestFile> deleteManifests) {
    return new Builder(io, Sets.newHashSet(deleteManifests));
  }

  static class Builder {
    private final FileIO io;
    private final Set<ManifestFile> deleteManifests;
    private Map<Integer, PartitionSpec> specsById;
    private Expression dataFilter;
    private Expression partitionFilter;
    private boolean caseSensitive;
    private ExecutorService executorService;

    Builder(FileIO io, Set<ManifestFile> deleteManifests) {
      this.io = io;
      this.deleteManifests = Sets.newHashSet(deleteManifests);
      this.specsById = null;
      this.dataFilter = Expressions.alwaysTrue();
      this.partitionFilter = Expressions.alwaysTrue();
      this.caseSensitive = true;
      this.executorService = null;
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
      Queue<Pair<Integer, ManifestEntry<DeleteFile>>> deleteEntries = new ConcurrentLinkedQueue<>();
      Tasks.foreach(deleteManifestReaders())
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(executorService)
          .run(specIdAndReader -> {
            try (CloseableIterable<ManifestEntry<DeleteFile>> reader = specIdAndReader.second()) {
              for (ManifestEntry<DeleteFile> entry : reader) {
                // copy with stats for better filtering against data file stats
                deleteEntries.add(Pair.of(specIdAndReader.first(), entry.copy()));
              }
            } catch (IOException e) {
              throw new RuntimeIOException("Failed to close", e);
            }
          });

      ListMultimap<Pair<Integer, StructLikeWrapper>, ManifestEntry<DeleteFile>> deleteFilesByPartition =
          Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
      for (Pair<Integer, ManifestEntry<DeleteFile>> specIdAndEntry : deleteEntries) {
        int specId = specIdAndEntry.first();
        ManifestEntry<DeleteFile> entry = specIdAndEntry.second();
        deleteFilesByPartition.put(Pair.of(specId, StructLikeWrapper.wrap(entry.file().partition())), entry);
      }

      Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> deletesByPartition = Maps.newHashMap();
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

          deletesByPartition.put(partition, Pair.of(seqs, files));

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

          deletesByPartition.put(partition, Pair.of(seqs, files));
        }
      }

      return new DeleteFileIndex(globalApplySeqs, globalDeletes, deletesByPartition);
    }

    private Iterable<Pair<Integer, CloseableIterable<ManifestEntry<DeleteFile>>>> deleteManifestReaders() {
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
          manifest -> Pair.of(
              manifest.partitionSpecId(),
              ManifestFiles.readDeleteManifest(manifest, io, specsById)
                  .filterRows(dataFilter)
                  .filterPartitions(partitionFilter)
                  .caseSensitive(caseSensitive)
                  .liveEntries())
      );
    }
  }
}
