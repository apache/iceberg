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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.PARTITION_STATS_ENABLED;
import static org.apache.iceberg.TableProperties.PARTITION_STATS_ENABLED_DEFAULT;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.Timer.Timed;
import org.apache.iceberg.partition.stats.PartitionStatsUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnnecessaryAnonymousClass")
abstract class SnapshotProducer<ThisT> implements SnapshotUpdate<ThisT> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProducer.class);
  static final Set<ManifestFile> EMPTY_SET = Sets.newHashSet();

  /** Default callback used to delete files. */
  private final Consumer<String> defaultDelete =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          ops.io().deleteFile(file);
        }
      };

  /** Cache used to enrich ManifestFile instances that are written to a ManifestListWriter. */
  private final LoadingCache<ManifestFile, ManifestFile> manifestsWithMetadata;

  private final TableOperations ops;
  private final String commitUUID = UUID.randomUUID().toString();
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private final AtomicInteger attempt = new AtomicInteger(0);
  private final List<String> manifestLists = Lists.newArrayList();
  private final long targetManifestSizeBytes;
  private MetricsReporter reporter = LoggingMetricsReporter.instance();
  private volatile Long snapshotId = null;
  private TableMetadata base;
  private boolean stageOnly = false;
  private Consumer<String> deleteFunc = defaultDelete;

  private ExecutorService workerPool = ThreadPools.getWorkerPool();
  private String targetBranch = SnapshotRef.MAIN_BRANCH;
  private CommitMetrics commitMetrics;
  private final boolean writePartitionStats;

  protected SnapshotProducer(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.manifestsWithMetadata =
        Caffeine.newBuilder()
            .build(
                file -> {
                  if (file.snapshotId() != null) {
                    return file;
                  }
                  return addMetadata(ops, file);
                });
    this.targetManifestSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    writePartitionStats =
        ops.current().spec().isPartitioned()
            && PropertyUtil.propertyAsBoolean(
                base.properties(), PARTITION_STATS_ENABLED, PARTITION_STATS_ENABLED_DEFAULT);
  }

  protected abstract ThisT self();

  @Override
  public ThisT stageOnly() {
    this.stageOnly = true;
    return self();
  }

  @Override
  public ThisT scanManifestsWith(ExecutorService executorService) {
    this.workerPool = executorService;
    return self();
  }

  protected CommitMetrics commitMetrics() {
    if (commitMetrics == null) {
      this.commitMetrics = CommitMetrics.of(new DefaultMetricsContext());
    }

    return commitMetrics;
  }

  protected ThisT reportWith(MetricsReporter newReporter) {
    this.reporter = newReporter;
    return self();
  }

  /**
   * A setter for the target branch on which snapshot producer operation should be performed
   *
   * @param branch to set as target branch
   */
  protected void targetBranch(String branch) {
    Preconditions.checkArgument(branch != null, "Invalid branch name: null");
    boolean refExists = base.ref(branch) != null;
    Preconditions.checkArgument(
        !refExists || base.ref(branch).isBranch(),
        "%s is a tag, not a branch. Tags cannot be targets for producing snapshots",
        branch);
    this.targetBranch = branch;
  }

  protected String targetBranch() {
    return targetBranch;
  }

  protected ExecutorService workerPool() {
    return this.workerPool;
  }

  @Override
  public ThisT deleteWith(Consumer<String> deleteCallback) {
    Preconditions.checkArgument(
        this.deleteFunc == defaultDelete, "Cannot set delete callback more than once");
    this.deleteFunc = deleteCallback;
    return self();
  }

  /**
   * Clean up any uncommitted manifests that were created.
   *
   * <p>Manifests may not be committed if apply is called more because a commit conflict has
   * occurred. Implementations may keep around manifests because the same changes will be made by
   * both apply calls. This method instructs the implementation to clean up those manifests and
   * passes the paths of the manifests that were actually committed.
   *
   * @param committed a set of manifest paths that were actually committed
   */
  protected abstract void cleanUncommitted(Set<ManifestFile> committed);

  /**
   * A string that describes the action that produced the new snapshot.
   *
   * @return a string operation
   */
  protected abstract String operation();

  /**
   * Validate the current metadata.
   *
   * <p>Child operations can override this to add custom validation.
   *
   * @param currentMetadata current table metadata to validate
   * @param snapshot ending snapshot on the lineage which is being validated
   */
  protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {}

  /**
   * Apply the update's changes to the given metadata and snapshot. Return the new manifest list.
   *
   * @param metadataToUpdate the base table metadata to apply changes to
   * @param snapshot snapshot to apply the changes to
   * @return a manifest list for the new snapshot.
   */
  protected abstract List<ManifestFile> apply(TableMetadata metadataToUpdate, Snapshot snapshot);

  /**
   * Update stats for each partition along with previous values and write new stats file for the
   * current snapshot.
   *
   * @return the stats file location
   */
  protected abstract String writeUpdatedPartitionStats(long snapshotCreatedTimeInMillis);

  protected CloseableIterable<Partition> partitionStatsEntriesFromParentSnapshot() {
    if (base.currentSnapshot() == null) {
      return CloseableIterable.empty();
    }

    String oldPartitionStatsFileLocation = base.currentSnapshot().partitionStatsFileLocation();
    if (oldPartitionStatsFileLocation == null) {
      return CloseableIterable.empty();
    }

    return PartitionStatsUtil.readPartitionStatsFile(
        Partition.icebergSchema(Partitioning.partitionType(base.specsById().values())),
        ops.io().newInputFile(oldPartitionStatsFileLocation));
  }

  protected void writePartitionStatsEntries(
      Iterable<Partition> partitionStatsEntries, OutputFile file) {
    PartitionStatsUtil.writePartitionStatsFile(
        partitionStatsEntries.iterator(), file, ops.current().specsById().values());
  }

  protected void updatePartitionStatsMapWithParentEntries(
      long snapshotCreatedTimeInMillis, PartitionsTable.PartitionMap partitionMap) {
    // update the snapshot ID and snapshot created time for the current snapshot
    // as this information was not available at the place of partition creation.
    // see PartitionStatsMap#updatePartitionMap()
    partitionMap
        .all()
        .forEach(
            partition -> {
              partition.setLastUpdatedAt(snapshotCreatedTimeInMillis * 1000L);
              partition.setLastUpdatedSnapshotId(snapshotId());
            });

    // get entries from base snapshot and add to current snapshot's entries
    try (CloseableIterable<Partition> recordIterator = partitionStatsEntriesFromParentSnapshot()) {
      recordIterator.forEach(
          partition -> partitionMap.get(partition.partitionData()).add(partition));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public boolean writePartitionStats() {
    return writePartitionStats;
  }

  @Override
  public Snapshot apply() {
    refresh();
    Snapshot parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch);

    long sequenceNumber = base.nextSequenceNumber();
    Long parentSnapshotId = parentSnapshot == null ? null : parentSnapshot.snapshotId();

    validate(base, parentSnapshot);
    List<ManifestFile> manifests = apply(base, parentSnapshot);

    OutputFile manifestList = manifestListPath();

    try (ManifestListWriter writer =
        ManifestLists.write(
            ops.current().formatVersion(),
            manifestList,
            snapshotId(),
            parentSnapshotId,
            sequenceNumber)) {

      // keep track of the manifest lists created
      manifestLists.add(manifestList.location());

      ManifestFile[] manifestFiles = new ManifestFile[manifests.size()];

      Tasks.range(manifestFiles.length)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(workerPool)
          .run(index -> manifestFiles[index] = manifestsWithMetadata.get(manifests.get(index)));

      writer.addAll(Arrays.asList(manifestFiles));

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file");
    }

    long snapshotCreatedTimeInMillis = System.currentTimeMillis();

    Map<String, String> additionalSummary = Maps.newHashMap();
    // Update Partition Stats for new Snapshot
    if (writePartitionStats) {
      String partitionStatsLocation = writeUpdatedPartitionStats(snapshotCreatedTimeInMillis);
      if (partitionStatsLocation != null) {
        additionalSummary.put(
            SnapshotSummary.PARTITION_STATS_FILE_LOCATION, partitionStatsLocation);
      }
    }

    return new BaseSnapshot(
        sequenceNumber,
        snapshotId(),
        parentSnapshotId,
        snapshotCreatedTimeInMillis,
        operation(),
        summary(base, additionalSummary),
        base.currentSchemaId(),
        manifestList.location());
  }

  protected abstract Map<String, String> summary();

  /** Returns the snapshot summary from the implementation and updates totals. */
  private Map<String, String> summary(
      TableMetadata previous, Map<String, String> additionalSummary) {
    Map<String, String> summary = summary();

    if (summary == null) {
      return ImmutableMap.of();
    }

    Map<String, String> previousSummary;
    SnapshotRef previousBranchHead = previous.ref(targetBranch);
    if (previousBranchHead != null) {
      if (previous.snapshot(previousBranchHead.snapshotId()).summary() != null) {
        previousSummary = previous.snapshot(previousBranchHead.snapshotId()).summary();
      } else {
        // previous snapshot had no summary, use an empty summary
        previousSummary = ImmutableMap.of();
      }
    } else {
      // if there was no previous snapshot, default the summary to start totals at 0
      ImmutableMap.Builder<String, String> summaryBuilder = ImmutableMap.builder();
      summaryBuilder
          .put(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
          .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")
          .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
          .put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0");
      previousSummary = summaryBuilder.build();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    // copy all summary properties from the implementation
    builder.putAll(summary);

    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_RECORDS_PROP,
        summary,
        SnapshotSummary.ADDED_RECORDS_PROP,
        SnapshotSummary.DELETED_RECORDS_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_FILE_SIZE_PROP,
        summary,
        SnapshotSummary.ADDED_FILE_SIZE_PROP,
        SnapshotSummary.REMOVED_FILE_SIZE_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DATA_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_FILES_PROP,
        SnapshotSummary.DELETED_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DELETE_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_DELETE_FILES_PROP,
        SnapshotSummary.REMOVED_DELETE_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_POS_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_POS_DELETES_PROP,
        SnapshotSummary.REMOVED_POS_DELETES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_EQ_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_EQ_DELETES_PROP,
        SnapshotSummary.REMOVED_EQ_DELETES_PROP);

    if (!additionalSummary.isEmpty()) {
      builder.putAll(additionalSummary);
    }

    return builder.build();
  }

  protected TableMetadata current() {
    return base;
  }

  protected TableMetadata refresh() {
    this.base = ops.refresh();
    return base;
  }

  @Override
  public void commit() {
    // this is always set to the latest commit attempt's snapshot id.
    AtomicLong newSnapshotId = new AtomicLong(-1L);
    Timed totalDuration = commitMetrics().totalDuration().start();
    try {
      Tasks.foreach(ops)
          .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .countAttempts(commitMetrics().attempts())
          .run(
              taskOps -> {
                Snapshot newSnapshot = apply();
                newSnapshotId.set(newSnapshot.snapshotId());
                TableMetadata.Builder update = TableMetadata.buildFrom(base);
                if (base.snapshot(newSnapshot.snapshotId()) != null) {
                  // this is a rollback operation
                  update.setBranchSnapshot(newSnapshot.snapshotId(), targetBranch);
                } else if (stageOnly) {
                  update.addSnapshot(newSnapshot);
                } else {
                  update.setBranchSnapshot(newSnapshot, targetBranch);
                }

                if (newSnapshot.partitionStatsFileLocation() != null) {
                  PartitionStatisticsFile partitionStatisticsFile =
                      ImmutableGenericPartitionStatisticsFile.builder()
                          .snapshotId(newSnapshotId.get())
                          .path(newSnapshot.partitionStatsFileLocation())
                          .maxDataSequenceNumber(newSnapshot.sequenceNumber())
                          .build();
                  update.setPartitionStatistics(newSnapshotId.get(), partitionStatisticsFile);
                }

                TableMetadata updated = update.build();
                if (updated.changes().isEmpty()) {
                  // do not commit if the metadata has not changed. for example, this may happen
                  // when setting the current
                  // snapshot to an ID that is already current. note that this check uses identity.
                  return;
                }

                // if the table UUID is missing, add it here. the UUID will be re-created each time
                // this operation retries
                // to ensure that if a concurrent operation assigns the UUID, this operation will
                // not fail.
                taskOps.commit(base, updated.withUUID());
              });

    } catch (CommitStateUnknownException commitStateUnknownException) {
      throw commitStateUnknownException;
    } catch (RuntimeException e) {
      Exceptions.suppressAndThrow(e, this::cleanAll);
    }

    try {
      LOG.info("Committed snapshot {} ({})", newSnapshotId.get(), getClass().getSimpleName());

      // at this point, the commit must have succeeded. after a refresh, the snapshot is loaded by
      // id in case another commit was added between this commit and the refresh.
      Snapshot saved = ops.refresh().snapshot(newSnapshotId.get());
      if (saved != null) {
        cleanUncommitted(Sets.newHashSet(saved.allManifests(ops.io())));
        // also clean up unused manifest lists created by multiple attempts
        for (String manifestList : manifestLists) {
          if (!saved.manifestListLocation().equals(manifestList)) {
            deleteFile(manifestList);
          }
        }
      } else {
        // saved may not be present if the latest metadata couldn't be loaded due to eventual
        // consistency problems in refresh. in that case, don't clean up.
        LOG.warn("Failed to load committed snapshot, skipping manifest clean-up");
      }

    } catch (Throwable e) {
      LOG.warn(
          "Failed to load committed table metadata or during cleanup, skipping further cleanup", e);
    }

    totalDuration.stop();

    try {
      notifyListeners();
    } catch (Throwable e) {
      LOG.warn("Failed to notify event listeners", e);
    }
  }

  private void notifyListeners() {
    try {
      Object event = updateEvent();
      if (event != null) {
        Listeners.notifyAll(event);

        if (event instanceof CreateSnapshotEvent) {
          CreateSnapshotEvent createSnapshotEvent = (CreateSnapshotEvent) event;

          reporter.report(
              ImmutableCommitReport.builder()
                  .tableName(createSnapshotEvent.tableName())
                  .snapshotId(createSnapshotEvent.snapshotId())
                  .operation(createSnapshotEvent.operation())
                  .sequenceNumber(createSnapshotEvent.sequenceNumber())
                  .metadata(EnvironmentContext.get())
                  .commitMetrics(
                      CommitMetricsResult.from(commitMetrics(), createSnapshotEvent.summary()))
                  .build());
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to notify listeners", e);
    }
  }

  protected void cleanAll() {
    for (String manifestList : manifestLists) {
      deleteFile(manifestList);
    }
    manifestLists.clear();
    cleanUncommitted(EMPTY_SET);
  }

  protected void deleteFile(String path) {
    deleteFunc.accept(path);
  }

  protected OutputFile manifestListPath() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.AVRO.addExtension(
                    String.format(
                        "snap-%d-%d-%s", snapshotId(), attempt.incrementAndGet(), commitUUID))));
  }

  protected OutputFile newManifestOutput() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.AVRO.addExtension(commitUUID + "-m" + manifestCount.getAndIncrement())));
  }

  protected OutputFile newPartitionStatsFile() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.PARQUET.addExtension(
                    String.format("partition-stats-%d", snapshotId()))));
  }

  protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec spec) {
    return ManifestFiles.write(
        ops.current().formatVersion(), spec, newManifestOutput(), snapshotId());
  }

  protected ManifestWriter<DeleteFile> newDeleteManifestWriter(PartitionSpec spec) {
    return ManifestFiles.writeDeleteManifest(
        ops.current().formatVersion(), spec, newManifestOutput(), snapshotId());
  }

  protected RollingManifestWriter<DataFile> newRollingManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(() -> newManifestWriter(spec), targetManifestSizeBytes);
  }

  protected RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(
        () -> newDeleteManifestWriter(spec), targetManifestSizeBytes);
  }

  protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
    return ManifestFiles.read(manifest, ops.io(), ops.current().specsById());
  }

  protected ManifestReader<DeleteFile> newDeleteManifestReader(ManifestFile manifest) {
    return ManifestFiles.readDeleteManifest(manifest, ops.io(), ops.current().specsById());
  }

  protected long snapshotId() {
    if (snapshotId == null) {
      synchronized (this) {
        while (snapshotId == null || ops.current().snapshot(snapshotId) != null) {
          this.snapshotId = ops.newSnapshotId();
        }
      }
    }
    return snapshotId;
  }

  private static ManifestFile addMetadata(TableOperations ops, ManifestFile manifest) {
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, ops.io(), ops.current().specsById())) {
      PartitionSummary stats = new PartitionSummary(ops.current().spec(manifest.partitionSpecId()));
      int addedFiles = 0;
      long addedRows = 0L;
      int existingFiles = 0;
      long existingRows = 0L;
      int deletedFiles = 0;
      long deletedRows = 0L;

      Long snapshotId = null;
      long maxSnapshotId = Long.MIN_VALUE;
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        if (entry.snapshotId() > maxSnapshotId) {
          maxSnapshotId = entry.snapshotId();
        }

        switch (entry.status()) {
          case ADDED:
            addedFiles += 1;
            addedRows += entry.file().recordCount();
            if (snapshotId == null) {
              snapshotId = entry.snapshotId();
            }
            break;
          case EXISTING:
            existingFiles += 1;
            existingRows += entry.file().recordCount();
            break;
          case DELETED:
            deletedFiles += 1;
            deletedRows += entry.file().recordCount();
            if (snapshotId == null) {
              snapshotId = entry.snapshotId();
            }
            break;
        }

        stats.update(entry.file().partition());
      }

      if (snapshotId == null) {
        // if no files were added or deleted, use the largest snapshot ID in the manifest
        snapshotId = maxSnapshotId;
      }

      return new GenericManifestFile(
          manifest.path(),
          manifest.length(),
          manifest.partitionSpecId(),
          ManifestContent.DATA,
          manifest.sequenceNumber(),
          manifest.minSequenceNumber(),
          snapshotId,
          addedFiles,
          addedRows,
          existingFiles,
          existingRows,
          deletedFiles,
          deletedRows,
          stats.summaries(),
          null);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read manifest: %s", manifest.path());
    }
  }

  private static void updateTotal(
      ImmutableMap.Builder<String, String> summaryBuilder,
      Map<String, String> previousSummary,
      String totalProperty,
      Map<String, String> currentSummary,
      String addedProperty,
      String deletedProperty) {
    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
      try {
        long newTotal = Long.parseLong(totalStr);

        String addedStr = currentSummary.get(addedProperty);
        if (newTotal >= 0 && addedStr != null) {
          newTotal += Long.parseLong(addedStr);
        }

        String deletedStr = currentSummary.get(deletedProperty);
        if (newTotal >= 0 && deletedStr != null) {
          newTotal -= Long.parseLong(deletedStr);
        }

        if (newTotal >= 0) {
          summaryBuilder.put(totalProperty, String.valueOf(newTotal));
        }

      } catch (NumberFormatException e) {
        // ignore and do not add total
      }
    }
  }
}
