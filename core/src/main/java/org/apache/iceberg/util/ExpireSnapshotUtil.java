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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExpireSnapshotUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotUtil.class);

  //Utility Class No Instantiation Allowed
  private ExpireSnapshotUtil() {}

  /**
   * Determines the manifest files which need to be inspected because they refer to data files which
   * can be removed after a Snapshot Expiration.
   *
   * Our goal is to determine which manifest files we actually need to read through because they
   * may refer to files which are no longer accessible from any valid snapshot and do not effect
   * the current table.
   *
   * For this we need to look through
   *   1. Snapshots which have not expired but contain manifests from expired snapshots
   *   2. Snapshots which have expired and contain manifests referring to now orphaned files
   *
   * @param snapshotChanges       Information about which Snapshots are still valid and which have been expired
   * @param original              The table metadata from before the snapshot expiration
   * @param io                    FileIO for reading manifest info
   * @return Wrapper around which manifests contain references to possibly abandoned files
   */
  public static ManifestExpirationChanges determineManifestChangesFromSnapshotExpiration(
      SnapshotExpirationChanges snapshotChanges, TableMetadata original, FileIO io) {

    AncestorInfo ancestorInfo = getAncestorInfo(original);

    Set<Snapshot> currentSnapshots = snapshotChanges.getValidSnapshots();
    Set<Snapshot> expiredSnapshots = snapshotChanges.getExpiredSnapshots();
    Set<Long> validIds = snapshotChanges.getValidSnapshotIds();
    Set<Long> expiredIds = snapshotChanges.getExpiredSnapshotIds();
    Set<Long> ancestorIds = ancestorInfo.getAncestorIds();

    //Snapshots which are not expired but refer to manifests from expired snapshots
    Set<ManifestFile> validManifests = getValidManifests(currentSnapshots, io);
    Set<ManifestFile> manifestsToScan = findValidManifestsInExpiredSnapshots(validManifests, ancestorInfo, validIds);

    //Snapshots which are expired and do not effect the current table
    List<Snapshot> snapshotsNotInTableState = findSnapshotsNotInTableState(expiredSnapshots, ancestorInfo);
    ManifestExpirationChanges manifestExpirationChanges =
        findExpiredManifestsInUnusedSnapshots(snapshotsNotInTableState, validManifests, ancestorIds, expiredIds, io);

    manifestExpirationChanges.manifestsToScan().addAll(manifestsToScan);
    return manifestExpirationChanges;
  }

  /**
   * Compares the Snapshots from the two TableMetadata objects and identifies the snapshots
   * still in use and those no longer in use
   * @param current Metadata from a table after an expiration of snapshots
   * @param original Metadata from the table before expiration of snapshots
   * @return Information about which Snapshots have Expired and which are Still Valid
   */
  public static SnapshotExpirationChanges getExpiredSnapshots(TableMetadata current, TableMetadata original) {

    Set<Snapshot> validSnapshots = Sets.newHashSet();
    for (Snapshot snapshot : current.snapshots()) {
      validSnapshots.add(snapshot);
    }

    Set<Snapshot> expiredSnapshots = Sets.newHashSet();
    for (Snapshot snapshot : original.snapshots()) {
      if (!validSnapshots.contains(snapshot)) {
        // This snapshot is no longer in the current metadata
        expiredSnapshots.add(snapshot);
      }
    }

    return new SnapshotExpirationChanges(validSnapshots, expiredSnapshots);
  }

  private static AncestorInfo getAncestorInfo(TableMetadata original) {
    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds = Sets.newHashSet(SnapshotUtil.ancestorIds(original.currentSnapshot(), original::snapshot));
    Set<Long> pickedAncestorSnapshotIds = Sets.newHashSet();
    for (long snapshotId : ancestorIds) {
      String sourceSnapshotId = original.snapshot(snapshotId).summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
      if (sourceSnapshotId != null) {
        // protect any snapshot that was cherry-picked into the current table state
        pickedAncestorSnapshotIds.add(Long.parseLong(sourceSnapshotId));
      }
    }

    return new AncestorInfo(ancestorIds, pickedAncestorSnapshotIds);
  }

  /**
   * Given a list of currently valid snapshots, extract all the manifests from those snapshots. If
   * there is an error while reading manifest lists an incomplete list of manifests will be
   * produced.
   *
   * @param currentSnapshots a list of currently valid non-expired snapshots
   * @return all of the manifests of those snapshots
   */
  private static Set<ManifestFile> getValidManifests(Set<Snapshot> currentSnapshots, FileIO io) {

    Set<ManifestFile> validManifests = Sets.newHashSet();
    Tasks.foreach(currentSnapshots).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}", snapshot.snapshotId(),
                snapshot.manifestListLocation(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot, io)) {
                for (ManifestFile manifest : manifests) {
                  validManifests.add(manifest);
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Failed to close manifest list: %s", snapshot.manifestListLocation()), e);
              }
            });
    return validManifests;
  }

  /**
   * Find manifests to clean up that are still referenced by a valid snapshot, but written by an
   * expired snapshot.
   *
   * @param validManifests Manfiests that are part of the current table state
   * @param ancestors Information about snapshot ancestors both cherry picked and and normal
   * @param validIds The ids of Snapshots that are currently valid
   *
   * @return MetadataFiles which must be scanned to look for files to delete
   */
  private static Set<ManifestFile> findValidManifestsInExpiredSnapshots(
      Set<ManifestFile> validManifests, AncestorInfo ancestors, Set<Long> validIds) {

    Set<ManifestFile> manifestsToScan = Sets.newHashSet();
    validManifests.forEach(manifest -> {
      long snapshotId = manifest.snapshotId();
      // whether the manifest was created by a valid snapshot (true) or an expired snapshot (false)
      boolean fromValidSnapshots = validIds.contains(snapshotId);
      // whether the snapshot that created the manifest was an ancestor of the table state
      boolean isFromAncestor = ancestors.getAncestorIds().contains(snapshotId);
      // whether the changes in this snapshot have been picked into the current table state
      boolean isPicked = ancestors.getPickedAncestorIds().contains(snapshotId);
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
   * @param expiredSnapshots Snapshots which have been expired from the table
   * @param ancestors Information about snapshot ancestors both cherry picked and and normal
   * @return A list of those snapshots which may have files that need to be deleted
   */
  private static List<Snapshot> findSnapshotsNotInTableState(Set<Snapshot> expiredSnapshots, AncestorInfo ancestors) {
    return expiredSnapshots.stream().filter(snapshot -> {
      long snapshotId = snapshot.snapshotId();
      if (ancestors.getPickedAncestorIds().contains(snapshotId)) {
        // this snapshot was cherry-picked into the current table state, so skip cleaning it up.
        // its changes will expire when the picked snapshot expires.
        // A -- C -- D (source=B)
        //  `- B <-- this commit
        return false;
      }

      long sourceSnapshotId = PropertyUtil.propertyAsLong(
          snapshot.summary(), SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, -1);
      if (ancestors.getAncestorIds().contains(sourceSnapshotId)) {
        // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
        // changes because it would revert data file additions that are in the current table.
        // A -- B -- C
        //  `- D (source=B) <-- this commit
        return false;
      }

      if (ancestors.getPickedAncestorIds().contains(sourceSnapshotId)) {
        // this commit was cherry-picked from a commit that is in the current table state. do not clean up its
        // changes because it would revert data file additions that are in the current table.
        // A -- C -- E (source=B)
        //  `- B `- D (source=B) <-- this commit
        return false;
      }
      return true;
    }).collect(Collectors.toList());
  }

  /**
   *
   * @param snapshotsNotInTableState Snapshots which may contain manifests that must be scanned because
   *                                 their contents may not be needed
   * @param validManifests Manifests which must be kept
   * @param expiredSnapshotIds Snapshots which are no longer needed
   * @param ancestorIds Ids of ancestors of the original table metadata
   * @param io For inspecting Manifests
   * @return
   */
  private static ManifestExpirationChanges findExpiredManifestsInUnusedSnapshots(
      List<Snapshot> snapshotsNotInTableState, Set<ManifestFile> validManifests, Set<Long> ancestorIds,
      Set<Long> expiredSnapshotIds, FileIO io) {

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToRevert = Sets.newHashSet();
    Set<ManifestFile> manifestsToScan = Sets.newHashSet();

    Tasks.foreach(snapshotsNotInTableState).retry(3).suppressFailureWhenFinished()
        .onFailure((snapshot, exc) ->
            LOG.warn("Failed on snapshot {} while reading manifest list: {}",
                snapshot.snapshotId(), snapshot.manifestListLocation(), exc))
        .run(snapshot -> {
          // find any manifests that are no longer needed
          try (CloseableIterable<ManifestFile> manifests = readManifestFiles(snapshot, io)) {
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

  private static CloseableIterable<ManifestFile> readManifestFiles(Snapshot snapshot, FileIO io) {

    if (snapshot.manifestListLocation() != null) {
      return Avro.read(io.newInputFile(snapshot.manifestListLocation()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .classLoader(GenericManifestFile.class.getClassLoader())
          .project(MANIFEST_PROJECTION)
          .reuseContainers(true)
          .build();

    } else {
      return CloseableIterable.withNoopClose(snapshot.allManifests());
    }
  }

  public static class SnapshotExpirationChanges {

    private final Set<Snapshot> validSnapshots;
    private final Set<Snapshot> expiredSnapshots;

    public SnapshotExpirationChanges(Set<Snapshot> validSnapshots, Set<Snapshot> expiredSnapshots) {
      this.validSnapshots = validSnapshots;
      this.expiredSnapshots = expiredSnapshots;
    }

    public Set<Long> getValidSnapshotIds() {
      return validSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    }

    public Set<Long> getExpiredSnapshotIds() {
      return expiredSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    }

    public Set<Snapshot> getValidSnapshots() {
      return validSnapshots;
    }

    public Set<Snapshot> getExpiredSnapshots() {
      return expiredSnapshots;
    }
  }

  public static class AncestorInfo {
    private final Set<Long> ancestorIds;
    private final Set<Long> pickedAncestorIds;

    public AncestorInfo(Set<Long> ancestorIds, Set<Long> pickedAncestorIds) {
      this.ancestorIds = ancestorIds;
      this.pickedAncestorIds = pickedAncestorIds;
    }

    public Set<Long> getAncestorIds() {
      return ancestorIds;
    }

    public Set<Long> getPickedAncestorIds() {
      return pickedAncestorIds;
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

    public Set<ManifestFile> manifestsToScan() {
      return manifestsToScan;
    }

    public Set<ManifestFile> manifestsToRevert() {
      return manifestsToRevert;
    }

    public Set<String> manifestsToDelete() {
      return manifestsToDelete;
    }

    public Set<String> manifestListsToDelete() {
      return manifestListsToDelete;
    }
  }
}
