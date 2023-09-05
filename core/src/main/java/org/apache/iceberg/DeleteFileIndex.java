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
import java.util.Collection;
import java.util.Collections;
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
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.Tasks;

/**
 * An index of {@link DeleteFile delete files} by sequence number.
 *
 * <p>Use {@link #builderFor(FileIO, Iterable)} to construct an index, and {@link #forDataFile(long,
 * DataFile)} or {@link #forEntry(ManifestEntry)} to get the delete files to apply to a given data
 * file.
 */
class DeleteFileIndex {
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final Map<Integer, Types.StructType> partitionTypeById;
  private final Map<Integer, ThreadLocal<StructLikeWrapper>> wrapperById;
  private final DeleteFileGroup globalDeletes;
  private final Map<Pair<Integer, StructLikeWrapper>, DeleteFileGroup> deletesByPartition;
  private final boolean isEmpty;
  private final boolean useColumnStatsFiltering;

  /** @deprecated since 1.4.0, will be removed in 1.5.0. */
  @Deprecated
  DeleteFileIndex(
      Map<Integer, PartitionSpec> specs,
      long[] globalSeqs,
      DeleteFile[] globalDeletes,
      Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> deletesByPartition) {
    this(specs, index(specs, globalSeqs, globalDeletes), index(specs, deletesByPartition), true);
  }

  private DeleteFileIndex(
      Map<Integer, PartitionSpec> specs,
      DeleteFileGroup globalDeletes,
      Map<Pair<Integer, StructLikeWrapper>, DeleteFileGroup> deletesByPartition,
      boolean useColumnStatsFiltering) {
    ImmutableMap.Builder<Integer, Types.StructType> builder = ImmutableMap.builder();
    specs.forEach((specId, spec) -> builder.put(specId, spec.partitionType()));
    this.partitionTypeById = builder.build();
    this.wrapperById = wrappers(specs);
    this.globalDeletes = globalDeletes;
    this.deletesByPartition = deletesByPartition;
    this.isEmpty = globalDeletes == null && deletesByPartition.isEmpty();
    this.useColumnStatsFiltering = useColumnStatsFiltering;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public Iterable<DeleteFile> referencedDeleteFiles() {
    Iterable<DeleteFile> deleteFiles = Collections.emptyList();

    if (globalDeletes != null) {
      deleteFiles = Iterables.concat(deleteFiles, globalDeletes.referencedDeleteFiles());
    }

    for (DeleteFileGroup partitionDeletes : deletesByPartition.values()) {
      deleteFiles = Iterables.concat(deleteFiles, partitionDeletes.referencedDeleteFiles());
    }

    return deleteFiles;
  }

  // use HashMap with precomputed values instead of thread-safe collections loaded on demand
  // as the cache is being accessed for each data file and the lookup speed is critical
  private Map<Integer, ThreadLocal<StructLikeWrapper>> wrappers(Map<Integer, PartitionSpec> specs) {
    Map<Integer, ThreadLocal<StructLikeWrapper>> wrappers = Maps.newHashMap();
    specs.forEach((specId, spec) -> wrappers.put(specId, newWrapper(specId)));
    return wrappers;
  }

  private ThreadLocal<StructLikeWrapper> newWrapper(int specId) {
    return ThreadLocal.withInitial(() -> StructLikeWrapper.forType(partitionTypeById.get(specId)));
  }

  private Pair<Integer, StructLikeWrapper> partition(int specId, StructLike struct) {
    ThreadLocal<StructLikeWrapper> wrapper = wrapperById.get(specId);
    return Pair.of(specId, wrapper.get().set(struct));
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

  DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
      return NO_DELETES;
    }

    Pair<Integer, StructLikeWrapper> partition = partition(file.specId(), file.partition());
    DeleteFileGroup partitionDeletes = deletesByPartition.get(partition);

    if (globalDeletes == null && partitionDeletes == null) {
      return NO_DELETES;
    } else if (useColumnStatsFiltering) {
      return limitWithColumnStatsFiltering(sequenceNumber, file, partitionDeletes);
    } else {
      return limitWithoutColumnStatsFiltering(sequenceNumber, partitionDeletes);
    }
  }

  // limits deletes using sequence numbers and checks whether columns stats overlap
  private DeleteFile[] limitWithColumnStatsFiltering(
      long sequenceNumber, DataFile file, DeleteFileGroup partitionDeletes) {

    Stream<IndexedDeleteFile> matchingDeletes;
    if (partitionDeletes == null) {
      matchingDeletes = globalDeletes.limit(sequenceNumber);
    } else if (globalDeletes == null) {
      matchingDeletes = partitionDeletes.limit(sequenceNumber);
    } else {
      Stream<IndexedDeleteFile> matchingGlobalDeletes = globalDeletes.limit(sequenceNumber);
      Stream<IndexedDeleteFile> matchingPartitionDeletes = partitionDeletes.limit(sequenceNumber);
      matchingDeletes = Stream.concat(matchingGlobalDeletes, matchingPartitionDeletes);
    }

    return matchingDeletes
        .filter(deleteFile -> canContainDeletesForFile(file, deleteFile))
        .map(IndexedDeleteFile::wrapped)
        .toArray(DeleteFile[]::new);
  }

  // limits deletes using sequence numbers but skips expensive column stats filtering
  private DeleteFile[] limitWithoutColumnStatsFiltering(
      long sequenceNumber, DeleteFileGroup partitionDeletes) {

    if (partitionDeletes == null) {
      return globalDeletes.filter(sequenceNumber);
    } else if (globalDeletes == null) {
      return partitionDeletes.filter(sequenceNumber);
    } else {
      DeleteFile[] matchingGlobalDeletes = globalDeletes.filter(sequenceNumber);
      DeleteFile[] matchingPartitionDeletes = partitionDeletes.filter(sequenceNumber);
      return ObjectArrays.concat(matchingGlobalDeletes, matchingPartitionDeletes, DeleteFile.class);
    }
  }

  private static boolean canContainDeletesForFile(DataFile dataFile, IndexedDeleteFile deleteFile) {
    switch (deleteFile.content()) {
      case POSITION_DELETES:
        return canContainPosDeletesForFile(dataFile, deleteFile);

      case EQUALITY_DELETES:
        return canContainEqDeletesForFile(dataFile, deleteFile, deleteFile.spec().schema());
    }

    return true;
  }

  private static boolean canContainPosDeletesForFile(
      DataFile dataFile, IndexedDeleteFile deleteFile) {
    // check that the delete file can contain the data file's file_path
    if (deleteFile.hasNoLowerOrUpperBounds()) {
      return true;
    }

    int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    Comparator<CharSequence> comparator = Comparators.charSequences();

    CharSequence lower = deleteFile.lowerBound(pathId);
    if (lower != null && comparator.compare(dataFile.path(), lower) < 0) {
      return false;
    }

    CharSequence upper = deleteFile.upperBound(pathId);
    if (upper != null && comparator.compare(dataFile.path(), upper) > 0) {
      return false;
    }

    return true;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static boolean canContainEqDeletesForFile(
      DataFile dataFile, IndexedDeleteFile deleteFile, Schema schema) {
    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    // whether to check data ranges or to assume that the ranges match
    // if upper/lower bounds are missing, null counts may still be used to determine delete files
    // can be skipped
    boolean checkRanges =
        dataLowers != null && dataUppers != null && deleteFile.hasLowerAndUpperBounds();

    Map<Integer, Long> dataNullCounts = dataFile.nullValueCounts();
    Map<Integer, Long> dataValueCounts = dataFile.valueCounts();
    Map<Integer, Long> deleteNullCounts = deleteFile.nullValueCounts();
    Map<Integer, Long> deleteValueCounts = deleteFile.valueCounts();

    for (int id : deleteFile.equalityFieldIds()) {
      Types.NestedField field = schema.findField(id);
      if (!field.type().isPrimitiveType()) {
        // stats are not kept for nested types. assume that the delete file may match
        continue;
      }

      if (containsNull(dataNullCounts, field) && containsNull(deleteNullCounts, field)) {
        // the data has null values and null has been deleted, so the deletes must be applied
        continue;
      }

      if (allNull(dataNullCounts, dataValueCounts, field) && allNonNull(deleteNullCounts, field)) {
        // the data file contains only null values for this field, but there are no deletes for null
        // values
        return false;
      }

      if (allNull(deleteNullCounts, deleteValueCounts, field)
          && allNonNull(dataNullCounts, field)) {
        // the delete file removes only null rows with null for this field, but there are no data
        // rows with null
        return false;
      }

      if (!checkRanges) {
        // some upper and lower bounds are missing, assume they match
        continue;
      }

      ByteBuffer dataLower = dataLowers.get(id);
      ByteBuffer dataUpper = dataUppers.get(id);
      Object deleteLower = deleteFile.lowerBound(id);
      Object deleteUpper = deleteFile.upperBound(id);
      if (dataLower == null || dataUpper == null || deleteLower == null || deleteUpper == null) {
        // at least one bound is not known, assume the delete file may match
        continue;
      }

      if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
        // no values overlap between the data file and the deletes
        return false;
      }
    }

    return true;
  }

  private static <T> boolean rangesOverlap(
      Types.NestedField field,
      ByteBuffer dataLowerBuf,
      ByteBuffer dataUpperBuf,
      T deleteLower,
      T deleteUpper) {
    Type.PrimitiveType type = field.type().asPrimitiveType();
    Comparator<T> comparator = Comparators.forType(type);

    T dataLower = Conversions.fromByteBuffer(type, dataLowerBuf);
    if (comparator.compare(dataLower, deleteUpper) > 0) {
      return false;
    }

    T dataUpper = Conversions.fromByteBuffer(type, dataUpperBuf);
    if (comparator.compare(deleteLower, dataUpper) > 0) {
      return false;
    }

    return true;
  }

  private static boolean allNonNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return true;
    }

    if (nullValueCounts == null) {
      return false;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    if (nullValueCount == null) {
      return false;
    }

    return nullValueCount <= 0;
  }

  private static boolean allNull(
      Map<Integer, Long> nullValueCounts, Map<Integer, Long> valueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return false;
    }

    if (nullValueCounts == null || valueCounts == null) {
      return false;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    Long valueCount = valueCounts.get(field.fieldId());
    if (nullValueCount == null || valueCount == null) {
      return false;
    }

    return nullValueCount.equals(valueCount);
  }

  private static boolean containsNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
      return false;
    }

    if (nullValueCounts == null) {
      return true;
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    if (nullValueCount == null) {
      return true;
    }

    return nullValueCount > 0;
  }

  private static DeleteFileGroup index(
      Map<Integer, PartitionSpec> specs, Pair<long[], DeleteFile[]> pairs) {
    return index(specs, pairs.first(), pairs.second());
  }

  private static DeleteFileGroup index(
      Map<Integer, PartitionSpec> specs, long[] seqs, DeleteFile[] files) {
    if (files == null || files.length == 0) {
      return null;
    }

    IndexedDeleteFile[] indexedGlobalDeleteFiles = new IndexedDeleteFile[files.length];

    for (int pos = 0; pos < files.length; pos++) {
      DeleteFile file = files[pos];
      PartitionSpec spec = specs.get(file.specId());
      long applySequenceNumber = seqs[pos];
      indexedGlobalDeleteFiles[pos] = new IndexedDeleteFile(spec, file, applySequenceNumber);
    }

    return new DeleteFileGroup(seqs, indexedGlobalDeleteFiles);
  }

  private static Map<Pair<Integer, StructLikeWrapper>, DeleteFileGroup> index(
      Map<Integer, PartitionSpec> specs,
      Map<Pair<Integer, StructLikeWrapper>, Pair<long[], DeleteFile[]>> deletesByPartition) {
    Map<Pair<Integer, StructLikeWrapper>, DeleteFileGroup> indexed = Maps.newHashMap();
    deletesByPartition.forEach((key, value) -> indexed.put(key, index(specs, value)));
    return indexed;
  }

  static Builder builderFor(FileIO io, Iterable<ManifestFile> deleteManifests) {
    return new Builder(io, Sets.newHashSet(deleteManifests));
  }

  static Builder builderFor(Iterable<DeleteFile> deleteFiles) {
    return new Builder(deleteFiles);
  }

  static class Builder {
    private final FileIO io;
    private final Set<ManifestFile> deleteManifests;
    private final Iterable<DeleteFile> deleteFiles;
    private long minSequenceNumber = 0L;
    private Map<Integer, PartitionSpec> specsById = null;
    private Expression dataFilter = Expressions.alwaysTrue();
    private Expression partitionFilter = Expressions.alwaysTrue();
    private PartitionSet partitionSet = null;
    private boolean caseSensitive = true;
    private ExecutorService executorService = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();

    Builder(FileIO io, Set<ManifestFile> deleteManifests) {
      this.io = io;
      this.deleteManifests = Sets.newHashSet(deleteManifests);
      this.deleteFiles = null;
    }

    Builder(Iterable<DeleteFile> deleteFiles) {
      this.io = null;
      this.deleteManifests = null;
      this.deleteFiles = deleteFiles;
    }

    Builder afterSequenceNumber(long seq) {
      this.minSequenceNumber = seq;
      return this;
    }

    Builder specsById(Map<Integer, PartitionSpec> newSpecsById) {
      this.specsById = newSpecsById;
      return this;
    }

    Builder filterData(Expression newDataFilter) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support data filters");
      this.dataFilter = Expressions.and(dataFilter, newDataFilter);
      return this;
    }

    Builder filterPartitions(Expression newPartitionFilter) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support partition filters");
      this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
      return this;
    }

    Builder filterPartitions(PartitionSet newPartitionSet) {
      Preconditions.checkArgument(
          deleteFiles == null, "Index constructed from files does not support partition filters");
      this.partitionSet = newPartitionSet;
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

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      this.scanMetrics = newScanMetrics;
      return this;
    }

    private Iterable<DeleteFile> filterDeleteFiles() {
      return Iterables.filter(deleteFiles, file -> file.dataSequenceNumber() > minSequenceNumber);
    }

    private Collection<DeleteFile> loadDeleteFiles() {
      // read all of the matching delete manifests in parallel and accumulate the matching files in
      // a queue
      Queue<DeleteFile> files = new ConcurrentLinkedQueue<>();
      Tasks.foreach(deleteManifestReaders())
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(executorService)
          .run(
              deleteFile -> {
                try (CloseableIterable<ManifestEntry<DeleteFile>> reader = deleteFile) {
                  for (ManifestEntry<DeleteFile> entry : reader) {
                    if (entry.dataSequenceNumber() > minSequenceNumber) {
                      // copy with stats for better filtering against data file stats
                      files.add(entry.file().copy());
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeIOException(e, "Failed to close");
                }
              });
      return files;
    }

    DeleteFileIndex build() {
      Iterable<DeleteFile> files = deleteFiles != null ? filterDeleteFiles() : loadDeleteFiles();

      boolean useColumnStatsFiltering = false;

      // build a map from (specId, partition) to delete file entries
      Map<Integer, StructLikeWrapper> wrappersBySpecId = Maps.newHashMap();
      ListMultimap<Pair<Integer, StructLikeWrapper>, IndexedDeleteFile> deleteFilesByPartition =
          Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
      for (DeleteFile file : files) {
        int specId = file.specId();
        PartitionSpec spec = specsById.get(specId);
        StructLikeWrapper wrapper =
            wrappersBySpecId
                .computeIfAbsent(specId, id -> StructLikeWrapper.forType(spec.partitionType()))
                .copyFor(file.partition());
        IndexedDeleteFile indexedFile = new IndexedDeleteFile(spec, file);
        deleteFilesByPartition.put(Pair.of(specId, wrapper), indexedFile);

        if (!useColumnStatsFiltering) {
          useColumnStatsFiltering = indexedFile.hasLowerAndUpperBounds();
        }

        ScanMetricsUtil.indexedDeleteFile(scanMetrics, file);
      }

      // sort the entries in each map value by sequence number and split into sequence numbers and
      // delete files lists
      Map<Pair<Integer, StructLikeWrapper>, DeleteFileGroup> sortedDeletesByPartition =
          Maps.newHashMap();
      // also, separate out equality deletes in an unpartitioned spec that should be applied
      // globally
      DeleteFileGroup globalDeletes = null;
      for (Pair<Integer, StructLikeWrapper> partition : deleteFilesByPartition.keySet()) {
        if (specsById.get(partition.first()).isUnpartitioned()) {
          Preconditions.checkState(
              globalDeletes == null, "Detected multiple partition specs with no partitions");

          IndexedDeleteFile[] eqFilesSortedBySeq =
              deleteFilesByPartition.get(partition).stream()
                  .filter(file -> file.content() == FileContent.EQUALITY_DELETES)
                  .sorted(Comparator.comparingLong(IndexedDeleteFile::applySequenceNumber))
                  .toArray(IndexedDeleteFile[]::new);
          if (eqFilesSortedBySeq.length > 0) {
            globalDeletes = new DeleteFileGroup(eqFilesSortedBySeq);
          }

          IndexedDeleteFile[] posFilesSortedBySeq =
              deleteFilesByPartition.get(partition).stream()
                  .filter(file -> file.content() == FileContent.POSITION_DELETES)
                  .sorted(Comparator.comparingLong(IndexedDeleteFile::applySequenceNumber))
                  .toArray(IndexedDeleteFile[]::new);
          sortedDeletesByPartition.put(partition, new DeleteFileGroup(posFilesSortedBySeq));

        } else {
          IndexedDeleteFile[] filesSortedBySeq =
              deleteFilesByPartition.get(partition).stream()
                  .sorted(Comparator.comparingLong(IndexedDeleteFile::applySequenceNumber))
                  .toArray(IndexedDeleteFile[]::new);
          sortedDeletesByPartition.put(partition, new DeleteFileGroup(filesSortedBySeq));
        }
      }

      return new DeleteFileIndex(
          specsById, globalDeletes, sortedDeletesByPartition, useColumnStatsFiltering);
    }

    private Iterable<CloseableIterable<ManifestEntry<DeleteFile>>> deleteManifestReaders() {
      LoadingCache<Integer, ManifestEvaluator> evalCache =
          specsById == null
              ? null
              : Caffeine.newBuilder()
                  .build(
                      specId -> {
                        PartitionSpec spec = specsById.get(specId);
                        return ManifestEvaluator.forPartitionFilter(
                            Expressions.and(
                                partitionFilter,
                                Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                            spec,
                            caseSensitive);
                      });

      CloseableIterable<ManifestFile> closeableDeleteManifests =
          CloseableIterable.withNoopClose(deleteManifests);
      CloseableIterable<ManifestFile> matchingManifests =
          evalCache == null
              ? closeableDeleteManifests
              : CloseableIterable.filter(
                  scanMetrics.skippedDeleteManifests(),
                  closeableDeleteManifests,
                  manifest ->
                      manifest.content() == ManifestContent.DELETES
                          && (manifest.hasAddedFiles() || manifest.hasExistingFiles())
                          && evalCache.get(manifest.partitionSpecId()).eval(manifest));

      matchingManifests =
          CloseableIterable.count(scanMetrics.scannedDeleteManifests(), matchingManifests);
      return Iterables.transform(
          matchingManifests,
          manifest ->
              ManifestFiles.readDeleteManifest(manifest, io, specsById)
                  .filterRows(dataFilter)
                  .filterPartitions(partitionFilter)
                  .filterPartitions(partitionSet)
                  .caseSensitive(caseSensitive)
                  .scanMetrics(scanMetrics)
                  .liveEntries());
    }
  }

  // a group of indexed delete files sorted by the sequence number they apply to
  private static class DeleteFileGroup {
    private final long[] seqs;
    private final IndexedDeleteFile[] files;

    DeleteFileGroup(IndexedDeleteFile[] files) {
      this.seqs = Arrays.stream(files).mapToLong(IndexedDeleteFile::applySequenceNumber).toArray();
      this.files = files;
    }

    DeleteFileGroup(long[] seqs, IndexedDeleteFile[] files) {
      this.seqs = seqs;
      this.files = files;
    }

    public DeleteFile[] filter(long seq) {
      int start = findStartIndex(seq);

      if (start >= files.length) {
        return NO_DELETES;
      }

      DeleteFile[] matchingFiles = new DeleteFile[files.length - start];

      for (int index = start; index < files.length; index++) {
        matchingFiles[index - start] = files[index].wrapped();
      }

      return matchingFiles;
    }

    public Stream<IndexedDeleteFile> limit(long seq) {
      int start = findStartIndex(seq);
      return Arrays.stream(files, start, files.length);
    }

    private int findStartIndex(long seq) {
      int pos = Arrays.binarySearch(seqs, seq);
      int start;
      if (pos < 0) {
        // the sequence number was not found, where it would be inserted is -(pos + 1)
        start = -(pos + 1);
      } else {
        // the sequence number was found, but may not be the first
        // find the first delete file with the given sequence number by decrementing the position
        start = pos;
        while (start > 0 && seqs[start - 1] >= seq) {
          start -= 1;
        }
      }

      return start;
    }

    public Iterable<DeleteFile> referencedDeleteFiles() {
      return Arrays.stream(files).map(IndexedDeleteFile::wrapped).collect(Collectors.toList());
    }
  }

  // a delete file wrapper that caches the converted boundaries for faster boundary checks
  // this class is not meant to be exposed beyond the delete file index
  private static class IndexedDeleteFile {
    private final PartitionSpec spec;
    private final DeleteFile wrapped;
    private final long applySequenceNumber;
    private volatile Map<Integer, Object> convertedLowerBounds = null;
    private volatile Map<Integer, Object> convertedUpperBounds = null;

    IndexedDeleteFile(PartitionSpec spec, DeleteFile file, long applySequenceNumber) {
      this.spec = spec;
      this.wrapped = file;
      this.applySequenceNumber = applySequenceNumber;
    }

    IndexedDeleteFile(PartitionSpec spec, DeleteFile file) {
      this.spec = spec;
      this.wrapped = file;

      if (file.content() == FileContent.EQUALITY_DELETES) {
        this.applySequenceNumber = file.dataSequenceNumber() - 1;
      } else {
        this.applySequenceNumber = file.dataSequenceNumber();
      }
    }

    public PartitionSpec spec() {
      return spec;
    }

    public DeleteFile wrapped() {
      return wrapped;
    }

    public long applySequenceNumber() {
      return applySequenceNumber;
    }

    public FileContent content() {
      return wrapped.content();
    }

    public List<Integer> equalityFieldIds() {
      return wrapped.equalityFieldIds();
    }

    public Map<Integer, Long> valueCounts() {
      return wrapped.valueCounts();
    }

    public Map<Integer, Long> nullValueCounts() {
      return wrapped.nullValueCounts();
    }

    public Map<Integer, Long> nanValueCounts() {
      return wrapped.nanValueCounts();
    }

    public boolean hasNoLowerOrUpperBounds() {
      return wrapped.lowerBounds() == null || wrapped.upperBounds() == null;
    }

    public boolean hasLowerAndUpperBounds() {
      return wrapped.lowerBounds() != null && wrapped.upperBounds() != null;
    }

    @SuppressWarnings("unchecked")
    public <T> T lowerBound(int id) {
      return (T) lowerBounds().get(id);
    }

    private Map<Integer, Object> lowerBounds() {
      if (convertedLowerBounds == null) {
        synchronized (this) {
          if (convertedLowerBounds == null) {
            this.convertedLowerBounds = convertBounds(wrapped.lowerBounds());
          }
        }
      }

      return convertedLowerBounds;
    }

    @SuppressWarnings("unchecked")
    public <T> T upperBound(int id) {
      return (T) upperBounds().get(id);
    }

    private Map<Integer, Object> upperBounds() {
      if (convertedUpperBounds == null) {
        synchronized (this) {
          if (convertedUpperBounds == null) {
            this.convertedUpperBounds = convertBounds(wrapped.upperBounds());
          }
        }
      }

      return convertedUpperBounds;
    }

    private Map<Integer, Object> convertBounds(Map<Integer, ByteBuffer> bounds) {
      Map<Integer, Object> converted = Maps.newHashMap();

      if (bounds != null) {
        if (wrapped.content() == FileContent.POSITION_DELETES) {
          Type pathType = MetadataColumns.DELETE_FILE_PATH.type();
          int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
          ByteBuffer bound = bounds.get(pathId);
          if (bound != null) {
            converted.put(pathId, Conversions.fromByteBuffer(pathType, bound));
          }

        } else {
          for (int id : equalityFieldIds()) {
            Type type = spec.schema().findField(id).type();
            if (type.isPrimitiveType()) {
              ByteBuffer bound = bounds.get(id);
              if (bound != null) {
                converted.put(id, Conversions.fromByteBuffer(type, bound));
              }
            }
          }
        }
      }

      return converted;
    }
  }
}
