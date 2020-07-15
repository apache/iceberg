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

package org.apache.iceberg.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExpireSnapshotUtil {

  //Utility Class No Instantiation Allowed
  private ExpireSnapshotUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotUtil.class);

  private static AncestorIds getAncestorIds(TableMetadata currentTableMetadata) {
    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds = Sets.newHashSet(SnapshotUtil
        .ancestorIds(currentTableMetadata.currentSnapshot(), currentTableMetadata::snapshot));

    Set<Long> pickedAncestorSnapshotIds = Sets.newHashSet();
    for (long snapshotId : ancestorIds) {
      String sourceSnapshotId = currentTableMetadata.snapshot(snapshotId).summary()
          .get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
      if (sourceSnapshotId != null) {
        // protect any snapshot that was cherry-picked into the current table state
        pickedAncestorSnapshotIds.add(Long.parseLong(sourceSnapshotId));
      }
    }

    return new AncestorIds(ancestorIds, pickedAncestorSnapshotIds);
  }

  /**
   * Given a list of currently valid snapshots, extract all the manifests from those snapshots if
   * there is an error while reading manifest lists an incomplete list of manifests will be
   * produced.
   *
   * @param currentTableSnapshots a list of currently valid non-expired snapshots
   * @return all of the manifests of those snapshots
   */
  private static Set<ManifestFile> getValidManifests(
      List<Snapshot> currentTableSnapshots, TableOperations ops) {

    Set<ManifestFile> validManifests = Sets.newHashSet();
    Tasks.foreach(currentTableSnapshots).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}", snapshot.snapshotId(),
                snapshot.manifestListLocation(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot, ops)) {
                for (ManifestFile manifest : manifests) {
                  validManifests.add(manifest);
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Failed to close manifest list: %s",
                        snapshot.manifestListLocation()),
                    e);
              }
            });
    return validManifests;
  }

  /**
   * Find manifests to clean up that are still referenced by a valid snapshot, but written by an
   * expired snapshot.
   *
   * @param validSnapshotIds     A list of the snapshots which are not expired
   * @param currentTableMetadata A reference to the table containing the snapshots
   * @return MetadataFiles which must be scanned to look for files to delete
   */
  private static Set<ManifestFile> validManifestsInExpiredSnapshots(
      Set<Long> validSnapshotIds, TableMetadata currentTableMetadata) {

    AncestorIds ids = getAncestorIds(currentTableMetadata);
    Set<Long> ancestorIds = ids.getAncestorIds();
    Set<Long> pickedAncestorSnapshotIds = ids.getPickedAncestorIds();

    Set<ManifestFile> manifestsToScan = Sets.newHashSet();
    manifestsToScan.forEach(manifest -> {
      long snapshotId = manifest.snapshotId();
      // whether the manifest was created by a valid snapshot (true) or an expired snapshot (false)
      boolean fromValidSnapshots = validSnapshotIds.contains(snapshotId);
      // whether the snapshot that created the manifest was an ancestor of the table state
      boolean isFromAncestor = ancestorIds.contains(snapshotId);
      // whether the changes in this snapshot have been picked into the current table state
      boolean isPicked = pickedAncestorSnapshotIds.contains(snapshotId);
      // if the snapshot that wrote this manifest is no longer valid (has expired),
      // then delete its deleted files. note that this is only for expired snapshots that are in the
      // current table state
      if (!fromValidSnapshots && (isFromAncestor || isPicked) && manifest.hasDeletedFiles()) {
        manifestsToScan.add(manifest.copy());
      }
    });
    return manifestsToScan;
  }

  /**
   * Removes snapshots whose changes impact the current table state leaving only those which may
   * have files that could potentially need to be deleted.
   *
   * @param currentTableMetadata TableMetadata for the table we are expiring from
   * @param validSnapshotIds     Snapshots which are not expired
   * @return A list of those snapshots which may have files that need to be deleted
   */
  private static List<Snapshot> filterOutSnapshotsInTableState(
      Set<Long> validSnapshotIds, TableMetadata currentTableMetadata) {

    AncestorIds ids = getAncestorIds(currentTableMetadata);
    Set<Long> ancestorIds = ids.getAncestorIds();
    Set<Long> pickedAncestorSnapshotIds = ids.getPickedAncestorIds();

    List<Snapshot> currentSnapshots = currentTableMetadata.snapshots();
    return currentSnapshots.stream().filter(snapshot -> {
      long snapshotId = snapshot.snapshotId();
      if (!validSnapshotIds.contains(snapshotId)) {
        // determine whether the changes in this snapshot are in the current table state
        if (pickedAncestorSnapshotIds.contains(snapshotId)) {
          // this snapshot was cherry-picked into the current table state, so skip cleaning it up.
          // its changes will expire when the picked snapshot expires.
          // A -- C -- D (source=B)
          //  `- B <-- this commit
          return false;
        }
        long sourceSnapshotId = PropertyUtil.propertyAsLong(
            snapshot.summary(), SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, -1);
        if (ancestorIds.contains(sourceSnapshotId)) {
          // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
          // changes because it would revert data file additions that are in the current table.
          // A -- B -- C
              //  `- D (source=B) <-- this commit
              return false;
            }

            if (pickedAncestorSnapshotIds.contains(sourceSnapshotId)) {
              // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
              // changes because it would revert data file additions that are in the current table.
              // A -- C -- E (source=B)
              //  `- B `- D (source=B) <-- this commit
              return false;
            }
            return true;
          }
          return false;
        }
    ).collect(Collectors.toList());
  }

  private static ManifestExpirationChanges findExpiredManifestsInUnusedSnapshots(
      List<Snapshot> snapshotsNotInTableState, Set<ManifestFile> validManifests,
      TableMetadata oldMetadata, Set<Long> expiredSnapshotIds, TableOperations ops) {

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToRevert = Sets.newHashSet();
    Set<ManifestFile> manifestsToScan = Sets.newHashSet();
    Set<Long> ancestorIds = getAncestorIds(oldMetadata).getAncestorIds();

    Tasks.foreach(snapshotsNotInTableState).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}",
                snapshot.snapshotId(), snapshot.manifestListLocation(), exc))
        .run(snapshot -> {
          // find any manifests that are no longer needed
          try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot, ops)) {
            for (ManifestFile manifest : manifests) {
              if (!validManifests.contains(manifest)) {
                manifestsToDelete.add(manifest.path());

                boolean isFromAncestor = ancestorIds.contains(manifest.snapshotId());
                boolean isFromExpiringSnapshot = expiredSnapshotIds.contains(manifest.snapshotId());

                if (isFromAncestor && manifest.hasDeletedFiles()) {
                  // Only delete data files that were deleted in by an expired snapshot if that
                  // snapshot is an ancestor of the current table state. Otherwise, a snapshot that
                  // deleted files and was rolled back will delete files that could be in the current
                  // table state.
                  manifestsToScan.add(manifest.copy());
                }

                if (!isFromAncestor && isFromExpiringSnapshot && manifest.hasAddedFiles()) {
                  // Because the manifest was written by a snapshot that is not an ancestor of the
                  // current table state, the files added in this manifest can be removed. The extra
                  // check whether the manifest was written by a known snapshot that was expired in
                  // this commit ensures that the full ancestor list between when the snapshot was
                  // written and this expiration is known and there is no missing history. If history
                  // were missing, then the snapshot could be an ancestor of the table state but the
                  // ancestor ID set would not contain it and this would be unsafe.
                  manifestsToRevert.add(manifest.copy());
                }
              }
            }
            // add the manifest list to the delete set, if present
            if (snapshot.manifestListLocation() != null) {
              manifestListsToDelete.add(snapshot.manifestListLocation());
            }
          } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Failed to close manifest list: %s", snapshot.manifestListLocation()),
                e);
          }
        });
    return new ManifestExpirationChanges(manifestsToScan, manifestsToRevert, manifestsToDelete,
        manifestListsToDelete);
  }

  private static final Schema MANIFEST_PROJECTION = ManifestFile.schema()
      .select("manifest_path", "added_snapshot_id", "deleted_data_files_count");

  private static CloseableIterable<ManifestFile> readManifestFiles(
      Snapshot snapshot, TableOperations ops) {

    if (snapshot.manifestListLocation() != null) {
      return Avro.read(ops.io().newInputFile(snapshot.manifestListLocation()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .classLoader(GenericManifestFile.class.getClassLoader())
          .project(MANIFEST_PROJECTION)
          .reuseContainers(true)
          .build();

    } else {
      return CloseableIterable.withNoopClose(snapshot.allManifests());
    }
  }

  /**
   * Determines the manifest files which need to be inspected because they refer to data files which
   * can be removed after a Snapshot Expiration.
   *
   * @param currentTableSnapshots A list of Snapshots Currently used by the Table
   * @param validIds              The Ids of the Snapshots which have not been expired
   * @param expiredIds            The Ids of the Snapshots which have been expired
   * @param currentTableMetadata  The metadata of the table being expired
   * @param ops                   The Table Operations module for the table in question, required
   *                              for several IO operations
   * @return
   */
  public static ManifestExpirationChanges determineManifestChangesFromSnapshotExpiration(
      List<Snapshot> currentTableSnapshots, Set<Long> validIds, Set<Long> expiredIds,
      TableMetadata currentTableMetadata, TableOperations ops) {

    Set<ManifestFile> validManifests = getValidManifests(currentTableSnapshots, ops);
    Set<ManifestFile> manifestsToScan = validManifestsInExpiredSnapshots(validIds,
        currentTableMetadata);

    List<Snapshot> snapshotsNotChangingTableState = filterOutSnapshotsInTableState(validIds,
        currentTableMetadata);

    // find manifests to clean up that were only referenced by snapshots that have expired
    ManifestExpirationChanges manifestExpirationChanges =
        findExpiredManifestsInUnusedSnapshots(snapshotsNotChangingTableState, validManifests,
            currentTableMetadata, expiredIds, ops);

    manifestExpirationChanges.getManifestsToScan().addAll(manifestsToScan);
    return manifestExpirationChanges;
  }

  public static SnapshotExpirationChanges getExpiredSnapshots(
      TableOperations ops, TableMetadata originalMetadata) {

    TableMetadata current = ops.refresh();

    Set<Long> validIds = Sets.newHashSet();
    for (Snapshot snapshot : current.snapshots()) {
      validIds.add(snapshot.snapshotId());
    }

    Set<Long> expiredIds = Sets.newHashSet();
    for (Snapshot snapshot : originalMetadata.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      if (!validIds.contains(snapshotId)) {
        // the snapshot was expired
        LOG.info("Expired snapshot: {}", snapshot);
        expiredIds.add(snapshotId);
      }
    }

    return new SnapshotExpirationChanges(current.snapshots(), validIds, expiredIds);
  }

  public static class SnapshotExpirationChanges {

    private final List<Snapshot> currentSnapshots;
    private final Set<Long> validSnapshotIds;
    private final Set<Long> expiredSnapshotIds;

    public SnapshotExpirationChanges(
        List<Snapshot> currentSnapshots, Set<Long> validSnapshotIds, Set<Long> expiredSnapshotIds) {

      this.currentSnapshots = currentSnapshots;
      this.validSnapshotIds = validSnapshotIds;
      this.expiredSnapshotIds = expiredSnapshotIds;
    }

    public List<Snapshot> getCurrentSnapshots() {
      return currentSnapshots;
    }

    public Set<Long> getValidSnapshotIds() {
      return validSnapshotIds;
    }

    public Set<Long> getExpiredSnapshotIds() {
      return expiredSnapshotIds;
    }
  }

  public static class ManifestExpirationChanges {

    private final Set<ManifestFile> manifestsToScan;
    private final Set<ManifestFile> manifestsToRevert;
    private final Set<String> manifestsToDelete;
    private final Set<String> manifestListsToDelete;

    private ManifestExpirationChanges(
        Set<ManifestFile> manifestsToScan, Set<ManifestFile> manifestsToRevert,
        Set<String> manifestsToDelete, Set<String> manifestListsToDelete) {

      this.manifestsToScan = manifestsToScan;
      this.manifestsToRevert = manifestsToRevert;
      this.manifestsToDelete = manifestsToDelete;
      this.manifestListsToDelete = manifestListsToDelete;
    }


    public Set<ManifestFile> getManifestsToScan() {
      return manifestsToScan;
    }

    public Set<ManifestFile> getManifestsToRevert() {
      return manifestsToRevert;
    }

    public Set<String> getManifestsToDelete() {
      return manifestsToDelete;
    }

    public Set<String> getManifestListsToDelete() {
      return manifestListsToDelete;
    }
  }

  private static class AncestorIds {

    private final Set<Long> ancestorIds;
    private final Set<Long> pickedAncestorIds;

    private AncestorIds(Set<Long> ancestorIds, Set<Long> pickedAncestorIds) {
      this.ancestorIds = ancestorIds;
      this.pickedAncestorIds = pickedAncestorIds;
    }

    public Set<Long> getPickedAncestorIds() {
      return pickedAncestorIds;
    }

    public Set<Long> getAncestorIds() {
      return ancestorIds;
    }
  }
}
