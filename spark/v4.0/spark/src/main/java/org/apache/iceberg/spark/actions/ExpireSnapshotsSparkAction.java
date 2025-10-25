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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.ImmutableExpireSnapshots;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that performs the same operation as {@link org.apache.iceberg.ExpireSnapshots} but uses
 * Spark to determine the delta in files between the pre and post-expiration table metadata. All of
 * the same restrictions of {@link org.apache.iceberg.ExpireSnapshots} also apply to this action.
 *
 * <p>This action first leverages {@link org.apache.iceberg.ExpireSnapshots} to expire snapshots and
 * then uses metadata tables to find files that can be safely deleted. This is done by anti-joining
 * two Datasets that contain all manifest and content files before and after the expiration. The
 * snapshot expiration will be fully committed before any deletes are issued.
 *
 * <p>This operation performs a shuffle so the parallelism can be controlled through
 * 'spark.sql.shuffle.partitions'.
 *
 * <p>Deletes are still performed locally after retrieving the results from the Spark executors.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class ExpireSnapshotsSparkAction extends BaseSparkAction<ExpireSnapshotsSparkAction>
    implements ExpireSnapshots {

  public static final String STREAM_RESULTS = "stream-results";
  public static final boolean STREAM_RESULTS_DEFAULT = false;
  public static final String LOG_EXPIRE_FILES = "log-expire-files";
  public static final boolean LOG_EXPIRE_FILES_DEFAULT = false;
  private static final int LOG_BATCH_SIZE = 10000;

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsSparkAction.class);

  private final Table table;
  private final TableOperations ops;

  private final Set<Long> expiredSnapshotIds = Sets.newHashSet();
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;
  private Dataset<FileInfo> expiredFileDS = null;
  private Boolean cleanExpiredMetadata = null;

  ExpireSnapshotsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected ExpireSnapshotsSparkAction self() {
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction expireSnapshotId(long snapshotId) {
    expiredSnapshotIds.add(snapshotId);
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction retainLast(int numSnapshots) {
    Preconditions.checkArgument(
        1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s",
        numSnapshots);
    this.retainLastValue = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshotsSparkAction cleanExpiredMetadata(boolean clean) {
    this.cleanExpiredMetadata = clean;
    return this;
  }

  /**
   * Expires snapshots and commits the changes to the table, returning a Dataset of files to delete.
   *
   * <p>This does not delete data files. To delete data files, run {@link #execute()}.
   *
   * <p>This may be called before or after {@link #execute()} to return the expired files.
   *
   * @return a Dataset of files that are no longer referenced by the table
   */
  public Dataset<FileInfo> expireFiles() {
    if (expiredFileDS == null) {
      // fetch metadata before expiration
      TableMetadata originalMetadata = ops.current();

      // perform expiration
      org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();

      for (long id : expiredSnapshotIds) {
        expireSnapshots = expireSnapshots.expireSnapshotId(id);
      }

      if (expireOlderThanValue != null) {
        expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
      }

      if (retainLastValue != null) {
        expireSnapshots = expireSnapshots.retainLast(retainLastValue);
      }

      if (cleanExpiredMetadata != null) {
        expireSnapshots.cleanExpiredMetadata(cleanExpiredMetadata);
      }

      // Add option to print additional logging for expireFiles
      boolean logExpireFiles =
          PropertyUtil.propertyAsBoolean(options(), LOG_EXPIRE_FILES, LOG_EXPIRE_FILES_DEFAULT);
      if (logExpireFiles) {
        LOG.info(
            "Table metadata before expiration, Current snapshot ID: {}",
            originalMetadata.currentSnapshot() != null
                ? originalMetadata.currentSnapshot().snapshotId()
                : "none");

        // Log which snapshots will be expired and why
        logSnapshotExpirationReasons(originalMetadata);
      }

      expireSnapshots.cleanExpiredFiles(false).commit();

      // fetch valid files after expiration
      TableMetadata updatedMetadata = ops.refresh();
      Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

      // fetch files referenced by expired snapshots
      Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
      Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

      // determine expired files
      this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);

      if (logExpireFiles) {
        // Enhanced logging for debugging file deletion issues
        logSnapshotDeletionSummary(originalMetadata, deletedSnapshotIds);
        LOG.info(
            "Table metadata after expiration, Current snapshot ID: {}",
            updatedMetadata.currentSnapshot() != null
                ? updatedMetadata.currentSnapshot().snapshotId()
                : "none");
      }
    }

    return expiredFileDS;
  }

  @Override
  public ExpireSnapshots.Result execute() {
    JobGroupInfo info = newJobGroupInfo("EXPIRE-SNAPSHOTS", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();

    if (expireOlderThanValue != null) {
      options.add("older_than=" + expireOlderThanValue);
    }

    if (retainLastValue != null) {
      options.add("retain_last=" + retainLastValue);
    }

    if (!expiredSnapshotIds.isEmpty()) {
      Long first = expiredSnapshotIds.stream().findFirst().get();
      if (expiredSnapshotIds.size() > 1) {
        options.add(
            String.format("snapshot_ids: %s (%s more...)", first, expiredSnapshotIds.size() - 1));
      } else {
        options.add(String.format("snapshot_id: %s", first));
      }
    }

    if (cleanExpiredMetadata != null) {
      options.add("clean_expired_metadata=" + cleanExpiredMetadata);
    }

    return String.format("Expiring snapshots (%s) in %s", COMMA_JOINER.join(options), table.name());
  }

  private ExpireSnapshots.Result doExecute() {
    if (streamResults()) {
      return deleteFiles(expireFiles().toLocalIterator());
    } else {
      return deleteFiles(expireFiles().collectAsList().iterator());
    }
  }

  private boolean streamResults() {
    return PropertyUtil.propertyAsBoolean(options(), STREAM_RESULTS, STREAM_RESULTS_DEFAULT);
  }

  private Dataset<FileInfo> fileDS(TableMetadata metadata) {
    return fileDS(metadata, null);
  }

  private Dataset<FileInfo> fileDS(TableMetadata metadata, Set<Long> snapshotIds) {
    Table staticTable = newStaticTable(metadata, table.io());
    return contentFileDS(staticTable, snapshotIds)
        .union(manifestDS(staticTable, snapshotIds))
        .union(manifestListDS(staticTable, snapshotIds))
        .union(statisticsFileDS(staticTable, snapshotIds));
  }

  private Set<Long> findExpiredSnapshotIds(
      TableMetadata originalMetadata, TableMetadata updatedMetadata) {
    Set<Long> retainedSnapshots =
        updatedMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    return originalMetadata.snapshots().stream()
        .map(Snapshot::snapshotId)
        .filter(id -> !retainedSnapshots.contains(id))
        .collect(Collectors.toSet());
  }

  private ExpireSnapshots.Result deleteFiles(Iterator<FileInfo> files) {
    DeleteSummary summary;
    if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
      summary = deleteFiles((SupportsBulkOperations) table.io(), files);
    } else {

      if (deleteFunc == null) {
        LOG.info(
            "Table IO {} does not support bulk operations. Using non-bulk deletes.",
            table.io().getClass().getName());
        summary = deleteFiles(deleteExecutorService, table.io()::deleteFile, files);
      } else {
        LOG.info("Custom delete function provided. Using non-bulk deletes");
        summary = deleteFiles(deleteExecutorService, deleteFunc, files);
      }
    }

    LOG.info("Deleted {} total files", summary.totalFilesCount());

    return ImmutableExpireSnapshots.Result.builder()
        .deletedDataFilesCount(summary.dataFilesCount())
        .deletedPositionDeleteFilesCount(summary.positionDeleteFilesCount())
        .deletedEqualityDeleteFilesCount(summary.equalityDeleteFilesCount())
        .deletedManifestsCount(summary.manifestsCount())
        .deletedManifestListsCount(summary.manifestListsCount())
        .deletedStatisticsFilesCount(summary.statisticsFilesCount())
        .build();
  }

  /**
   * Logs which snapshots will be expired and the reasons for expiration. This helps debug issues
   * where more files than expected were deleted.
   */
  private void logSnapshotExpirationReasons(TableMetadata metadata) {
    LOG.info("=== Snapshot Expiration Analysis ===");

    // Determine snapshots that will be kept for retain_last calculation
    Set<Long> snapshotsToKeep = Sets.newHashSet();
    if (retainLastValue != null) {
      List<Snapshot> sortedSnapshots =
          metadata.snapshots().stream()
              .sorted((s1, s2) -> Long.compare(s2.timestampMillis(), s1.timestampMillis()))
              .collect(Collectors.toList());

      sortedSnapshots.stream()
          .limit(retainLastValue)
          .map(Snapshot::snapshotId)
          .forEach(snapshotsToKeep::add);

      LOG.info("Snapshots to keep (retain_last={}): {}", retainLastValue, snapshotsToKeep);
    }

    // Analyze each snapshot and log expiration reason
    for (Snapshot snapshot : metadata.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      boolean willExpire = false;
      String reason = "";

      // Check explicit snapshot IDs
      if (expiredSnapshotIds.contains(snapshotId)) {
        willExpire = true;
        reason = "explicitly marked for expiration";
      }
      // Check time-based criteria
      else if (expireOlderThanValue != null && snapshot.timestampMillis() < expireOlderThanValue) {
        willExpire = true;
        reason = "older than threshold (" + new java.util.Date(expireOlderThanValue) + ")";
      }
      // Check count-based criteria
      else if (retainLastValue != null && !snapshotsToKeep.contains(snapshotId)) {
        willExpire = true;
        reason = "beyond retain_last limit (" + retainLastValue + ")";
      }

      if (willExpire) {
        LOG.info(
            "WILL EXPIRE snapshot {} created at {} - reason: {}",
            snapshotId,
            new java.util.Date(snapshot.timestampMillis()),
            reason);
      }
    }
  }

  /**
   * Logs detailed information about files that will be deleted, organized by snapshot. This
   * provides the file-to-snapshot mapping requested for debugging.
   */
  private void logSnapshotDeletionSummary(
      TableMetadata originalMetadata, Set<Long> deletedSnapshotIds) {
    LOG.info("=== File Deletion Analysis by Snapshot ===");

    for (Long snapshotId : deletedSnapshotIds) {
      Set<Long> singleSnapshotSet = Sets.newHashSet(snapshotId);
      Dataset<FileInfo> snapshotFiles = fileDS(originalMetadata, singleSnapshotSet);

      // Use memory-efficient streaming to avoid OOM with large snapshots
      long totalFiles = snapshotFiles.count();
      LOG.info("Snapshot {} contributes {} files for deletion:", snapshotId, totalFiles);

      long processedFiles = 0;
      Iterator<FileInfo> fileIterator = snapshotFiles.toLocalIterator();
      while (fileIterator.hasNext()) {
        List<FileInfo> batch = Lists.newArrayListWithCapacity(LOG_BATCH_SIZE);

        // Collect up to BATCH_SIZE files
        for (int i = 0; i < LOG_BATCH_SIZE && fileIterator.hasNext(); i++) {
          batch.add(fileIterator.next());
        }

        // Group files by type for better organization
        final long currentProcessedFiles = processedFiles;
        batch.stream()
            .collect(Collectors.groupingBy(FileInfo::getType))
            .forEach(
                (fileType, fileList) -> {
                  LOG.info(
                      "  {} files of type {} (batch {}-{})",
                      fileList.size(),
                      fileType,
                      currentProcessedFiles + 1,
                      currentProcessedFiles + fileList.size());

                  // Log individual files with enhanced information
                  fileList.forEach(
                      fileInfo ->
                          LOG.info(
                              "    DELETE: {} (from expired snapshot: {}, type: {})",
                              fileInfo.getPath(),
                              snapshotId,
                              fileInfo.getType()));
                });

        processedFiles += batch.size();
      }
    }
  }
}
