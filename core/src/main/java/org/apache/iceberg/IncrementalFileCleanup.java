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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IncrementalFileCleanup extends FileCleanupStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementalFileCleanup.class);

  IncrementalFileCleanup(
      FileIO fileIO,
      ExecutorService deleteExecutorService,
      ExecutorService planExecutorService,
      Consumer<String> deleteFunc) {
    super(fileIO, deleteExecutorService, planExecutorService, deleteFunc);
  }

  @Override
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "MethodLength"})
  public void cleanFiles(TableMetadata beforeExpiration, TableMetadata afterExpiration) {
    if (afterExpiration.refs().size() > 1) {
      throw new UnsupportedOperationException(
          "Cannot incrementally clean files for tables with more than 1 ref");
    }

    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists

    Set<Long> validIds = Sets.newHashSet();
    for (Snapshot snapshot : afterExpiration.snapshots()) {
      validIds.add(snapshot.snapshotId());
    }

    Set<Long> expiredIds = Sets.newHashSet();
    for (Snapshot snapshot : beforeExpiration.snapshots()) {
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

    SnapshotRef branchToCleanup = Iterables.getFirst(beforeExpiration.refs().values(), null);
    if (branchToCleanup == null) {
      return;
    }

    Snapshot latest = beforeExpiration.snapshot(branchToCleanup.snapshotId());
    List<Snapshot> snapshots = afterExpiration.snapshots();

    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds =
        Sets.newHashSet(SnapshotUtil.ancestorIds(latest, beforeExpiration::snapshot));

    Set<Long> pickedAncestorSnapshotIds = Sets.newHashSet();
    for (long snapshotId : ancestorIds) {
      String sourceSnapshotId =
          beforeExpiration
              .snapshot(snapshotId)
              .summary()
              .get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
      if (sourceSnapshotId != null) {
        // protect any snapshot that was cherry-picked into the current table state
        pickedAncestorSnapshotIds.add(Long.parseLong(sourceSnapshotId));
      }
    }

    // find manifests to clean up that are still referenced by a valid snapshot, but written by an
    // expired snapshot
    Set<String> validManifests = Sets.newHashSet();
    Set<ManifestFile> manifestsToScan = Sets.newHashSet();

    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.
    Tasks.foreach(snapshots)
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed on snapshot {} while reading manifest list: {}",
                    snapshot.snapshotId(),
                    snapshot.manifestListLocation(),
                    exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                for (ManifestFile manifest : manifests) {
                  validManifests.add(manifest.path());

                  long snapshotId = manifest.snapshotId();
                  // whether the manifest was created by a valid snapshot (true) or an expired
                  // snapshot (false)
                  boolean fromValidSnapshots = validIds.contains(snapshotId);
                  // whether the snapshot that created the manifest was an ancestor of the table
                  // state
                  boolean isFromAncestor = ancestorIds.contains(snapshotId);
                  // whether the changes in this snapshot have been picked into the current table
                  // state
                  boolean isPicked = pickedAncestorSnapshotIds.contains(snapshotId);
                  // if the snapshot that wrote this manifest is no longer valid (has expired),
                  // then delete its deleted files. note that this is only for expired snapshots
                  // that are in the
                  // current table state
                  if (!fromValidSnapshots
                      && (isFromAncestor || isPicked)
                      && manifest.hasDeletedFiles()) {
                    manifestsToScan.add(manifest.copy());
                  }
                }

              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format(
                        "Failed to close manifest list: %s", snapshot.manifestListLocation()),
                    e);
              }
            });

    // find manifests to clean up that were only referenced by snapshots that have expired
    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToRevert = Sets.newHashSet();
    Tasks.foreach(beforeExpiration.snapshots())
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed on snapshot {} while reading manifest list: {}",
                    snapshot.snapshotId(),
                    snapshot.manifestListLocation(),
                    exc))
        .run(
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              if (!validIds.contains(snapshotId)) {
                // determine whether the changes in this snapshot are in the current table state
                if (pickedAncestorSnapshotIds.contains(snapshotId)) {
                  // this snapshot was cherry-picked into the current table state, so skip cleaning
                  // it up.
                  // its changes will expire when the picked snapshot expires.
                  // A -- C -- D (source=B)
                  //  `- B <-- this commit
                  return;
                }

                long sourceSnapshotId =
                    PropertyUtil.propertyAsLong(
                        snapshot.summary(), SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, -1);
                if (ancestorIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state.
                  // do not clean up its changes because it would revert data file additions that
                  // are in the current
                  // table.
                  // A -- B -- C
                  //  `- D (source=B) <-- this commit
                  return;
                }

                if (pickedAncestorSnapshotIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state.
                  // do not clean up its changes because it would revert data file additions that
                  // are in the current
                  // table.
                  // A -- C -- E (source=B)
                  //  `- B `- D (source=B) <-- this commit
                  return;
                }

                // find any manifests that are no longer needed
                try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                  for (ManifestFile manifest : manifests) {
                    if (!validManifests.contains(manifest.path())) {
                      manifestsToDelete.add(manifest.path());

                      boolean isFromAncestor = ancestorIds.contains(manifest.snapshotId());
                      boolean isFromExpiringSnapshot = expiredIds.contains(manifest.snapshotId());

                      if (isFromAncestor && manifest.hasDeletedFiles()) {
                        // Only delete data files that were deleted in by an expired snapshot if
                        // that napshot is an ancestor of the current table state. Otherwise, a
                        // snapshot
                        // that deleted files and was rolled back will delete files that could be in
                        // the current
                        // table state.
                        manifestsToScan.add(manifest.copy());
                      }

                      if (!isFromAncestor && isFromExpiringSnapshot && manifest.hasAddedFiles()) {
                        // Because the manifest was written by a snapshot that is not an ancestor of
                        // the current table state, the files added in this manifest can be removed.
                        // The
                        // extra check whether the manifest was written by a known snapshot that was
                        // expired in this commit ensures that the full ancestor list between when
                        // the snapshot
                        // was written and this expiration is known and there is no missing history.
                        // If
                        // history were missing, then the snapshot could be an ancestor of the table
                        // state
                        // but the ancestor ID set would not contain it and this would be unsafe.
                        manifestsToRevert.add(manifest.copy());
                      }
                    }
                  }
                } catch (IOException e) {
                  throw new UncheckedIOException(
                      String.format(
                          "Failed to close manifest list: %s", snapshot.manifestListLocation()),
                      e);
                }

                // add the manifest list to the delete set, if present
                if (snapshot.manifestListLocation() != null) {
                  manifestListsToDelete.add(snapshot.manifestListLocation());
                }
              }
            });

    Set<String> filesToDelete =
        findFilesToDelete(manifestsToScan, manifestsToRevert, validIds, afterExpiration);

    deleteFiles(filesToDelete, "data");
    deleteFiles(manifestsToDelete, "manifest");
    deleteFiles(manifestListsToDelete, "manifest list");

    if (!beforeExpiration.statisticsFiles().isEmpty()) {
      Set<String> expiredStatisticsFilesLocations =
          expiredStatisticsFilesLocations(beforeExpiration, afterExpiration);
      deleteFiles(expiredStatisticsFilesLocations, "statistics files");
    }
  }

  private Set<String> findFilesToDelete(
      Set<ManifestFile> manifestsToScan,
      Set<ManifestFile> manifestsToRevert,
      Set<Long> validIds,
      TableMetadata current) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();
    Tasks.foreach(manifestsToScan)
        .retry(3)
        .suppressFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (item, exc) ->
                LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(
            manifest -> {
              // the manifest has deletes, scan it to find files to delete
              try (ManifestReader<?> reader =
                  ManifestFiles.open(manifest, fileIO, current.specsById())) {
                for (ManifestEntry<?> entry : reader.entries()) {
                  // if the snapshot ID of the DELETE entry is no longer valid, the data can be
                  // deleted
                  if (entry.status() == ManifestEntry.Status.DELETED
                      && !validIds.contains(entry.snapshotId())) {
                    // use toString to ensure the path will not change (Utf8 is reused)
                    filesToDelete.add(entry.file().path().toString());
                  }
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Failed to read manifest file: %s", manifest), e);
              }
            });

    Tasks.foreach(manifestsToRevert)
        .retry(3)
        .suppressFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (item, exc) ->
                LOG.warn("Failed to get added files: this may cause orphaned data files", exc))
        .run(
            manifest -> {
              // the manifest has deletes, scan it to find files to delete
              try (ManifestReader<?> reader =
                  ManifestFiles.open(manifest, fileIO, current.specsById())) {
                for (ManifestEntry<?> entry : reader.entries()) {
                  // delete any ADDED file from manifests that were reverted
                  if (entry.status() == ManifestEntry.Status.ADDED) {
                    // use toString to ensure the path will not change (Utf8 is reused)
                    filesToDelete.add(entry.file().path().toString());
                  }
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Failed to read manifest file: %s", manifest), e);
              }
            });

    return filesToDelete;
  }
}
