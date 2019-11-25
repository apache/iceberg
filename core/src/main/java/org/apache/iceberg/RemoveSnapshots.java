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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.util.internal.ConcurrentSet;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
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
        (expireOlderThan != null && snapshot.timestampMillis() < expireOlderThan));
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

    cleanExpiredSnapshots();
  }

  private void cleanExpiredSnapshots() {
    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists

    TableMetadata current = ops.refresh();

    Set<Long> validIds = Sets.newHashSet();
    for (Snapshot snapshot : current.snapshots()) {
      validIds.add(snapshot.snapshotId());
    }

    Set<Long> expiredIds = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      if (!validIds.contains(snapshotId)) {
        // the snapshot was expired
        LOG.info("Expired snapshot: {}", snapshot);
        expiredIds.add(snapshotId);
      }
    }

    if (expiredIds.isEmpty()) {
      // if no snapshots were expired, skip cleanup
      return;
    }

    LOG.info("Committed snapshot changes; cleaning up expired manifests and data files.");

    cleanExpiredFiles(current.snapshots(), validIds, expiredIds);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void cleanExpiredFiles(List<Snapshot> snapshots, Set<Long> validIds, Set<Long> expiredIds) {
    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds = Sets.newHashSet(SnapshotUtil.ancestorIds(base.currentSnapshot(), base::snapshot));

    Set<String> validManifests = Sets.newHashSet();
    Set<String> manifestsToScan = Sets.newHashSet();
    for (Snapshot snapshot : snapshots) {
      try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot)) {
        for (ManifestFile manifest : manifests) {
          validManifests.add(manifest.path());

          boolean fromValidSnapshots = validIds.contains(manifest.snapshotId());
          boolean isFromAncestor = ancestorIds.contains(manifest.snapshotId());
          if (!fromValidSnapshots && isFromAncestor && manifest.hasDeletedFiles()) {
            manifestsToScan.add(manifest.path());
          }
        }

      } catch (IOException e) {
        throw new RuntimeIOException(e,
            "Failed to close manifest list: %s", snapshot.manifestListLocation());
      }
    }

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Set<String> manifestsToRevert = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      if (!validIds.contains(snapshotId)) {
        // find any manifests that are no longer needed
        try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot)) {
          for (ManifestFile manifest : manifests) {
            if (!validManifests.contains(manifest.path())) {
              manifestsToDelete.add(manifest.path());

              boolean isFromAncestor = ancestorIds.contains(manifest.snapshotId());
              boolean isFromExpiringSnapshot = expiredIds.contains(manifest.snapshotId());

              if (isFromAncestor && manifest.hasDeletedFiles()) {
                // Only delete data files that were deleted in by an expired snapshot if that
                // snapshot is an ancestor of the current table state. Otherwise, a snapshot that
                // deleted files and was rolled back will delete files that could be in the current
                // table state.
                manifestsToScan.add(manifest.path());
              }

              if (!isFromAncestor && isFromExpiringSnapshot && manifest.hasAddedFiles()) {
                // Because the manifest was written by a snapshot that is not an ancestor of the
                // current table state, the files added in this manifest can be removed. The extra
                // check whether the manifest was written by a known snapshot that was expired in
                // this commit ensures that the full ancestor list between when the snapshot was
                // written and this expiration is known and there is no missing history. If history
                // were missing, then the snapshot could be an ancestor of the table state but the
                // ancestor ID set would not contain it and this would be unsafe.
                manifestsToRevert.add(manifest.path());
              }
            }
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e,
              "Failed to close manifest list: %s", snapshot.manifestListLocation());
        }

        // add the manifest list to the delete set, if present
        if (snapshot.manifestListLocation() != null) {
          manifestListsToDelete.add(snapshot.manifestListLocation());
        }
      }
    }

    deleteDataFiles(manifestsToScan, manifestsToRevert, validIds);
    deleteMetadataFiles(manifestsToDelete, manifestListsToDelete);
  }

  private void deleteMetadataFiles(Set<String> manifestsToDelete, Set<String> manifestListsToDelete) {
    LOG.warn("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));

    Tasks.foreach(manifestsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(deleteFunc::accept);

    Tasks.foreach(manifestListsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(deleteFunc::accept);
  }

  private void deleteDataFiles(Set<String> manifestsToScan, Set<String> manifestsToRevert, Set<Long> validIds) {
    Set<String> filesToDelete = findFilesToDelete(manifestsToScan, manifestsToRevert, validIds);
    Tasks.foreach(filesToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Delete failed for data file: {}", file, exc))
        .run(file -> deleteFunc.accept(file));
  }

  private Set<String> findFilesToDelete(
      Set<String> manifestsToScan, Set<String> manifestsToRevert, Set<Long> validIds) {
    Set<String> filesToDelete = new ConcurrentSet<>();
    Tasks.foreach(manifestsToScan)
        .noRetry().suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(manifest -> {
          // the manifest has deletes, scan it to find files to delete
          try (ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest), ops.current().specsById())) {
            for (ManifestEntry entry : reader.entries()) {
              // if the snapshot ID of the DELETE entry is no longer valid, the data can be deleted
              if (entry.status() == ManifestEntry.Status.DELETED &&
                  !validIds.contains(entry.snapshotId())) {
                // use toString to ensure the path will not change (Utf8 is reused)
                filesToDelete.add(entry.file().path().toString());
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
          }
        });

    Tasks.foreach(manifestsToRevert)
        .noRetry().suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get added files: this may cause orphaned data files", exc))
        .run(manifest -> {
          // the manifest has deletes, scan it to find files to delete
          try (ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest), ops.current().specsById())) {
            for (ManifestEntry entry : reader.entries()) {
              // delete any ADDED file from manifests that were reverted
              if (entry.status() == ManifestEntry.Status.ADDED) {
                // use toString to ensure the path will not change (Utf8 is reused)
                filesToDelete.add(entry.file().path().toString());
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: %s", manifest);
          }
        });

    return filesToDelete;
  }

  private static final Schema MANIFEST_PROJECTION = ManifestFile.schema()
      .select("manifest_path", "added_snapshot_id", "deleted_data_files_count");

  private CloseableIterable<ManifestFile> readManifestFiles(Snapshot snapshot) {
    if (snapshot.manifestListLocation() != null) {
      return Avro.read(ops.io().newInputFile(snapshot.manifestListLocation()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .project(MANIFEST_PROJECTION)
          .reuseContainers(true)
          .build();

    } else {
      return CloseableIterable.withNoopClose(snapshot.manifests());
    }
  }
}
