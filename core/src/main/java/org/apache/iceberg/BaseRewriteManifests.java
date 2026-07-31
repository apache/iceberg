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

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;

public class BaseRewriteManifests extends SnapshotProducer<RewriteManifests>
    implements RewriteManifests {
  private static final Object ROW_ID_ASSIGNMENT_CLUSTER_KEY = new Object();

  private final String tableName;
  private final Map<Integer, PartitionSpec> specsById;
  private final long manifestTargetSizeBytes;

  private final Set<ManifestFile> deletedManifests = Sets.newHashSet();
  private final List<ManifestFile> addedManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAddedManifests = Lists.newArrayList();

  private final Collection<ManifestFile> keptManifests = new ConcurrentLinkedQueue<>();
  private final Collection<ManifestFile> newManifests = new ConcurrentLinkedQueue<>();
  private final Set<ManifestFile> rewrittenManifests = Sets.newConcurrentHashSet();
  private final Map<Object, WriterWrapper> writers = Maps.newConcurrentMap();

  private final AtomicLong entryCount = new AtomicLong(0);
  private final Collection<RowIdRange> assignedRowIdRanges = new ConcurrentLinkedQueue<>();

  private Function<DataFile, Object> clusterByFunc;
  private Function<DataFile, Long> firstRowIdForFile;
  private Predicate<ManifestFile> predicate;
  private Long expectedFirstRowId;
  private Long nextRowIdExclusive;
  private Long expectedSnapshotId;

  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

  BaseRewriteManifests(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
    this.specsById = ops().current().specsById();
    this.manifestTargetSizeBytes =
        ops()
            .current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected RewriteManifests self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteManifests set(String property, String value) {
    summaryBuilder.set(property, value);
    return this;
  }

  @Override
  protected Map<String, String> summary() {
    int createdManifestsCount =
        newManifests.size() + addedManifests.size() + rewrittenAddedManifests.size();
    summaryBuilder.set(
        SnapshotSummary.CREATED_MANIFESTS_COUNT, String.valueOf(createdManifestsCount));
    summaryBuilder.set(SnapshotSummary.KEPT_MANIFESTS_COUNT, String.valueOf(keptManifests.size()));
    summaryBuilder.set(
        SnapshotSummary.REPLACED_MANIFESTS_COUNT,
        String.valueOf(rewrittenManifests.size() + deletedManifests.size()));
    summaryBuilder.set(
        SnapshotSummary.PROCESSED_MANIFEST_ENTRY_COUNT, String.valueOf(entryCount.get()));
    summaryBuilder.setPartitionSummaryLimit(
        0); // do not include partition summaries because data did not change
    return summaryBuilder.build();
  }

  @Override
  public RewriteManifests clusterBy(Function<DataFile, Object> func) {
    this.clusterByFunc = func;
    return this;
  }

  @Override
  public RewriteManifests assignFirstRowIds(
      Function<DataFile, Long> firstRowIdForFile,
      long expectedFirstRowId,
      long nextRowIdExclusive) {
    Preconditions.checkArgument(firstRowIdForFile != null, "First row ID function cannot be null");
    Preconditions.checkArgument(
        ops().current().formatVersion() >= 3,
        "Cannot assign first row IDs to a table with format version %s",
        ops().current().formatVersion());
    Preconditions.checkArgument(
        expectedFirstRowId >= 0,
        "Expected first row ID must be non-negative: %s",
        expectedFirstRowId);
    Preconditions.checkArgument(
        nextRowIdExclusive >= expectedFirstRowId,
        "Next row ID %s must not be less than expected first row ID %s",
        nextRowIdExclusive,
        expectedFirstRowId);
    Preconditions.checkState(
        !hasRowIdAssignment(), "First row ID assignment is already configured");
    Preconditions.checkState(
        predicate == null
            && deletedManifests.isEmpty()
            && addedManifests.isEmpty()
            && rewrittenAddedManifests.isEmpty(),
        "First row ID assignment cannot be combined with manifest selection or replacement");

    this.firstRowIdForFile = firstRowIdForFile;
    this.expectedFirstRowId = expectedFirstRowId;
    this.nextRowIdExclusive = nextRowIdExclusive;
    Snapshot currentSnapshot = ops().current().currentSnapshot();
    this.expectedSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;

    if (clusterByFunc == null) {
      this.clusterByFunc = ignored -> ROW_ID_ASSIGNMENT_CLUSTER_KEY;
    }

    return this;
  }

  @Override
  public RewriteManifests rewriteIf(Predicate<ManifestFile> pred) {
    Preconditions.checkState(
        !hasRowIdAssignment(),
        "Manifest selection cannot be combined with first row ID assignment");
    this.predicate = pred;
    return this;
  }

  @Override
  public RewriteManifests deleteManifest(ManifestFile manifest) {
    Preconditions.checkState(
        !hasRowIdAssignment(),
        "Manifest replacement cannot be combined with first row ID assignment");
    deletedManifests.add(manifest);
    return this;
  }

  @Override
  public RewriteManifests addManifest(ManifestFile manifest) {
    Preconditions.checkState(
        !hasRowIdAssignment(),
        "Manifest replacement cannot be combined with first row ID assignment");
    Preconditions.checkArgument(!manifest.hasAddedFiles(), "Cannot add manifest with added files");
    Preconditions.checkArgument(
        !manifest.hasDeletedFiles(), "Cannot add manifest with deleted files");
    Preconditions.checkArgument(
        manifest.snapshotId() == null || manifest.snapshotId() == -1,
        "Snapshot id must be assigned during commit");
    Preconditions.checkArgument(
        manifest.sequenceNumber() == -1, "Sequence must be assigned during commit");

    if (canInheritSnapshotId() && manifest.snapshotId() == null) {
      addedManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAddedManifests.add(copiedManifest);
    }

    return this;
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    TableMetadata current = ops().current();
    InputFile toCopy = ops().io().newInputFile(manifest);
    EncryptedOutputFile newFile = newManifestOutputFile();
    return ManifestFiles.copyRewriteManifest(
        current.formatVersion(),
        manifest.partitionSpecId(),
        manifest.firstRowId(),
        toCopy,
        specsById,
        newFile,
        snapshotId(),
        summaryBuilder);
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    List<ManifestFile> currentManifests = base.currentSnapshot().allManifests(ops().io());
    Set<ManifestFile> currentManifestSet = ImmutableSet.copyOf(currentManifests);

    validateDeletedManifests(currentManifestSet, base.currentSnapshot().snapshotId());

    if (requiresRewrite(currentManifestSet)) {
      performRewrite(currentManifests);
    } else {
      keepActiveManifests(currentManifests);
    }

    validateFilesCounts();
    validateAssignedRowIds();

    Iterable<ManifestFile> newManifestsWithMetadata =
        Iterables.transform(
            Iterables.concat(newManifests, addedManifests, rewrittenAddedManifests),
            manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());

    // put new manifests at the beginning
    List<ManifestFile> apply = Lists.newArrayList();
    Iterables.addAll(apply, newManifestsWithMetadata);
    apply.addAll(keptManifests);

    return apply;
  }

  @Override
  public Object updateEvent() {
    long snapshotId = snapshotId();
    Snapshot snapshot = ops().current().snapshot(snapshotId);
    long sequenceNumber = snapshot.sequenceNumber();
    return new CreateSnapshotEvent(
        tableName, operation(), snapshotId, sequenceNumber, snapshot.summary());
  }

  private boolean requiresRewrite(Set<ManifestFile> currentManifests) {
    if (clusterByFunc == null) {
      // manifests are deleted and added directly so don't perform a rewrite
      return false;
    }

    if (rewrittenManifests.isEmpty()) {
      // nothing yet processed so perform a full rewrite
      return true;
    }

    // if any processed manifest is not in the current manifest list, perform a full rewrite
    return rewrittenManifests.stream().anyMatch(manifest -> !currentManifests.contains(manifest));
  }

  private void keepActiveManifests(List<ManifestFile> currentManifests) {
    // keep any existing manifests as-is that were not processed
    keptManifests.clear();
    currentManifests.stream()
        .filter(
            manifest ->
                !rewrittenManifests.contains(manifest) && !deletedManifests.contains(manifest))
        .forEach(keptManifests::add);
  }

  private void reset() {
    deleteUncommitted(newManifests, ImmutableSet.of(), true /* clear new manifests */);
    entryCount.set(0);
    assignedRowIdRanges.clear();
    keptManifests.clear();
    rewrittenManifests.clear();
    writers.clear();
  }

  private void performRewrite(List<ManifestFile> currentManifests) {
    reset();

    List<ManifestFile> remainingManifests =
        currentManifests.stream()
            .filter(manifest -> !deletedManifests.contains(manifest))
            .collect(Collectors.toList());

    try {
      Tasks.foreach(remainingManifests)
          .executeWith(workerPool())
          .run(
              manifest -> {
                if (containsDeletes(manifest) || !matchesPredicate(manifest)) {
                  keptManifests.add(manifest);
                } else {
                  rewrittenManifests.add(manifest);
                  try (ManifestReader<DataFile> reader =
                      ManifestFiles.read(manifest, ops().io(), ops().current().specsById())
                          .select(Collections.singletonList("*"))) {
                    reader
                        .liveEntries()
                        .forEach(
                            entry ->
                                appendEntry(
                                    entry,
                                    clusterByFunc.apply(entry.file()),
                                    manifest.partitionSpecId()));

                  } catch (IOException x) {
                    throw new RuntimeIOException(x);
                  }
                }
              });
    } finally {
      Tasks.foreach(writers.values()).executeWith(workerPool()).run(WriterWrapper::close);
    }
  }

  private boolean containsDeletes(ManifestFile manifest) {
    return manifest.content() == ManifestContent.DELETES;
  }

  private boolean matchesPredicate(ManifestFile manifest) {
    return predicate == null || predicate.test(manifest);
  }

  private void validateDeletedManifests(
      Set<ManifestFile> currentManifests, long currentSnapshotID) {
    // directly deleted manifests must be still present in the current snapshot
    deletedManifests.stream()
        .filter(manifest -> !currentManifests.contains(manifest))
        .findAny()
        .ifPresent(
            manifest -> {
              throw new ValidationException(
                  "Deleted manifest %s could not be found in the latest snapshot %d",
                  manifest.path(), currentSnapshotID);
            });
  }

  private void validateFilesCounts() {
    Iterable<ManifestFile> createdManifests =
        Iterables.concat(newManifests, addedManifests, rewrittenAddedManifests);
    int createdManifestsFilesCount = activeFilesCount(createdManifests);

    Iterable<ManifestFile> replacedManifests =
        Iterables.concat(rewrittenManifests, deletedManifests);
    int replacedManifestsFilesCount = activeFilesCount(replacedManifests);

    if (createdManifestsFilesCount != replacedManifestsFilesCount) {
      throw new ValidationException(
          "Replaced and created manifests must have the same number of active files: %d (new), %d (old)",
          createdManifestsFilesCount, replacedManifestsFilesCount);
    }
  }

  private int activeFilesCount(Iterable<ManifestFile> manifests) {
    int activeFilesCount = 0;

    for (ManifestFile manifest : manifests) {
      Preconditions.checkNotNull(
          manifest.addedFilesCount(), "Missing file counts in %s", manifest.path());
      Preconditions.checkNotNull(
          manifest.existingFilesCount(), "Missing file counts in %s", manifest.path());
      activeFilesCount += manifest.addedFilesCount();
      activeFilesCount += manifest.existingFilesCount();
    }

    return activeFilesCount;
  }

  private void appendEntry(ManifestEntry<DataFile> entry, Object key, int partitionSpecId) {
    Preconditions.checkNotNull(entry, "Manifest entry cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    WriterWrapper writer = getWriter(key, partitionSpecId);
    if (hasRowIdAssignment()) {
      writer.addEntryWithAssignedRowId(
          entry, copyDataFileWithAssignedFirstRowId(entry.file(), partitionSpecId));
    } else {
      writer.addEntry(entry);
    }

    entryCount.incrementAndGet();
  }

  private DataFile copyDataFileWithAssignedFirstRowId(DataFile file, int partitionSpecId) {
    Long assignedFirstRowId = firstRowIdForFile.apply(file);
    ValidationException.check(
        assignedFirstRowId != null, "No first row ID for data file: %s", file.location());

    long firstRowId = assignedFirstRowId;
    long lastRowIdExclusive;
    try {
      lastRowIdExclusive = Math.addExact(firstRowId, file.recordCount());
    } catch (ArithmeticException e) {
      throw new ValidationException(e, "Row ID range overflows for data file: %s", file.location());
    }

    ValidationException.check(
        firstRowId >= expectedFirstRowId && lastRowIdExclusive <= nextRowIdExclusive,
        "Row ID range [%s, %s) for data file %s is outside reserved range [%s, %s)",
        firstRowId,
        lastRowIdExclusive,
        file.location(),
        expectedFirstRowId,
        nextRowIdExclusive);

    assignedRowIdRanges.add(new RowIdRange(firstRowId, lastRowIdExclusive, file.location()));
    PartitionSpec spec =
        Preconditions.checkNotNull(
            specsById.get(partitionSpecId), "Cannot find partition spec: %s", partitionSpecId);
    return DataFiles.builder(spec).copy(file).withFirstRowId(firstRowId).build();
  }

  private void validateAssignedRowIds() {
    if (!hasRowIdAssignment()) {
      return;
    }

    List<RowIdRange> ranges = Lists.newArrayList(assignedRowIdRanges);
    ranges.sort(Comparator.comparingLong(range -> range.firstRowId));

    RowIdRange previous = null;
    for (RowIdRange current : ranges) {
      if (previous != null && current.firstRowId < previous.lastRowIdExclusive) {
        throw new ValidationException(
            "Overlapping row ID ranges for data files %s [%s, %s) and %s [%s, %s)",
            previous.filePath,
            previous.firstRowId,
            previous.lastRowIdExclusive,
            current.filePath,
            current.firstRowId,
            current.lastRowIdExclusive);
      }

      if (previous == null || current.lastRowIdExclusive > previous.lastRowIdExclusive) {
        previous = current;
      }
    }
  }

  @Override
  protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {
    if (!hasRowIdAssignment()) {
      return;
    }

    Long currentSnapshotId = snapshot != null ? snapshot.snapshotId() : null;
    ValidationException.check(
        Objects.equals(expectedSnapshotId, currentSnapshotId),
        "Cannot assign first row IDs after the table snapshot changed: expected %s, found %s",
        expectedSnapshotId,
        currentSnapshotId);
    ValidationException.check(
        currentMetadata.nextRowId() == expectedFirstRowId,
        "Cannot assign first row IDs after table next-row-id changed: expected %s, found %s",
        expectedFirstRowId,
        currentMetadata.nextRowId());
  }

  @Override
  protected long assignedRows(TableMetadata base, long manifestListNextRowId) {
    if (!hasRowIdAssignment()) {
      return super.assignedRows(base, manifestListNextRowId);
    }

    return Math.subtractExact(nextRowIdExclusive, expectedFirstRowId);
  }

  private boolean hasRowIdAssignment() {
    return firstRowIdForFile != null;
  }

  private WriterWrapper getWriter(Object key, int partitionSpecId) {
    return writers.computeIfAbsent(
        Pair.of(key, partitionSpecId), k -> new WriterWrapper(specsById.get(partitionSpecId)));
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    deleteUncommitted(newManifests, committed, false);
    // clean up only rewrittenAddedManifests as they are always owned by the table
    // don't clean up addedManifests as they are added to the manifest list and are not compacted
    deleteUncommitted(rewrittenAddedManifests, committed, false);
  }

  long getManifestTargetSizeBytes() {
    return manifestTargetSizeBytes;
  }

  class WriterWrapper {
    private final PartitionSpec spec;
    private ManifestWriter<DataFile> writer;

    WriterWrapper(PartitionSpec spec) {
      this.spec = spec;
    }

    synchronized void addEntry(ManifestEntry<DataFile> entry) {
      prepareWriter();
      writer.existing(entry);
    }

    synchronized void addEntryWithAssignedRowId(ManifestEntry<DataFile> entry, DataFile file) {
      prepareWriter();
      writer.existing(
          file,
          Preconditions.checkNotNull(
              entry.snapshotId(), "Missing snapshot ID for data file: %s", file.location()),
          Preconditions.checkNotNull(
              entry.dataSequenceNumber(),
              "Missing data sequence number for data file: %s",
              file.location()),
          entry.fileSequenceNumber());
    }

    private void prepareWriter() {
      if (writer == null) {
        writer = newManifestWriter(spec);
      } else if (writer.length() >= getManifestTargetSizeBytes()) {
        close();
        writer = newManifestWriter(spec);
      }
    }

    synchronized void close() {
      if (writer != null) {
        try {
          writer.close();
          newManifests.add(writer.toManifestFile());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      }
    }
  }

  private static class RowIdRange {
    private final long firstRowId;
    private final long lastRowIdExclusive;
    private final String filePath;

    private RowIdRange(long firstRowId, long lastRowIdExclusive, String filePath) {
      this.firstRowId = firstRowId;
      this.lastRowIdExclusive = lastRowIdExclusive;
      this.filePath = filePath;
    }
  }
}
