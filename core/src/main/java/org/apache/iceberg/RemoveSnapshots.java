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

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ExpireSnapshotUtil;
import org.apache.iceberg.util.ExpireSnapshotUtil.ManifestExpirationChanges;
import org.apache.iceberg.util.ExpireSnapshotUtil.SnapshotExpirationChanges;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private final Set<Long> idsToRetain = Sets.newHashSet();
  private boolean cleanExpiredFiles = true;
  private TableMetadata base;
  private Long expireOlderThan = null;
  private Consumer<String> deleteFunc = defaultDelete;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long expireSnapshotId) {
    LOG.info("Expiring snapshot with id: {}", expireSnapshotId);
    idsToRemove.add(expireSnapshotId);
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    LOG.info("Expiring snapshots older than: {} ({})", new Date(timestampMillis), timestampMillis);
    this.expireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots retainLast(int numSnapshots) {
    Preconditions.checkArgument(1 <= numSnapshots,
            "Number of snapshots to retain must be at least 1, cannot be: %s", numSnapshots);
    idsToRetain.clear();
    List<Long> ancestorIds = SnapshotUtil.ancestorIds(base.currentSnapshot(), base::snapshot);
    if (numSnapshots >= ancestorIds.size()) {
      idsToRetain.addAll(ancestorIds);
    } else {
      idsToRetain.addAll(ancestorIds.subList(0, numSnapshots));
    }

    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public List<Snapshot> apply() {
    TableMetadata updated = internalApply();
    List<Snapshot> removed = Lists.newArrayList(base.snapshots());
    removed.removeAll(updated.snapshots());

    return removed;
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();

    return base.removeSnapshotsIf(snapshot ->
        idsToRemove.contains(snapshot.snapshotId()) ||
        (expireOlderThan != null && snapshot.timestampMillis() < expireOlderThan &&
            !idsToRetain.contains(snapshot.snapshotId())));
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(item -> {
          TableMetadata updated = internalApply();
          // only commit the updated metadata if at least one snapshot was removed
          if (updated.snapshots().size() != base.snapshots().size()) {
            ops.commit(base, updated);
          }
        });
    LOG.info("Committed snapshot changes");

    if (cleanExpiredFiles) {
      cleanExpiredSnapshots();
    } else {
      LOG.info("Cleaning up manifest and data files disabled, leaving them in place");
    }
  }

  private void cleanExpiredSnapshots() {
    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists
    TableMetadata currentMetadata = ops.refresh();
    SnapshotExpirationChanges snapshotChanges = ExpireSnapshotUtil.getExpiredSnapshots(currentMetadata, base);
    snapshotChanges.getExpiredSnapshots().forEach(snapshot -> LOG.info("Expired snapshot: {}", snapshot));

    if (snapshotChanges.getExpiredSnapshotIds().isEmpty()) {
      // if no snapshots were expired, skip cleanup
    } else {
      LOG.info("Cleaning up expired manifests and data files locally.");

      //All Read and Delete Failures are Ignored
      ManifestExpirationChanges changes = ExpireSnapshotUtil.determineManifestChangesFromSnapshotExpiration(
          snapshotChanges.getValidSnapshotIds(), snapshotChanges.getExpiredSnapshotIds(), currentMetadata, base,
          ops.io());

      deleteDataFiles(changes.manifestsToScan(), changes.manifestsToRevert(), snapshotChanges.getValidSnapshotIds());
      deleteMetadataFiles(changes.manifestsToDelete(), changes.manifestListsToDelete());
    }
  }

  private void deleteMetadataFiles(Set<String> manifestsToDelete, Set<String> manifestListsToDelete) {
    LOG.warn("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));
    LOG.warn("Manifests Lists to delete: {}", Joiner.on(", ").join(manifestListsToDelete));

    Tasks.foreach(manifestsToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(deleteFunc::accept);

    Tasks.foreach(manifestListsToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(deleteFunc::accept);
  }

  private void deleteDataFiles(Set<ManifestFile> manifestsToScan, Set<ManifestFile> manifestsToRevert,
                               Set<Long> validIds) {
    Set<String> filesToDelete = findFilesToDelete(manifestsToScan, manifestsToRevert, validIds);
    Tasks.foreach(filesToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Delete failed for data file: {}", file, exc))
        .run(file -> deleteFunc.accept(file));
  }

  private Set<String> findFilesToDelete(Set<ManifestFile> manifestsToScan, Set<ManifestFile> manifestsToRevert,
                                        Set<Long> validIds) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();
    filesToDelete.addAll(ManifestExpirationManager
        .scanManifestsForAbandonedDeletedFiles(manifestsToScan, validIds, ops.current().specsById(), ops.io()));

    filesToDelete.addAll(ManifestExpirationManager
        .scanManifestsForRevertingAddedFiles(manifestsToRevert, ops.current().specsById(), ops.io()));

    return filesToDelete;
  }

}
