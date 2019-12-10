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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * In an audit workflow, new data is written to an orphan snapshot that is not committed as the table's
 * current state until it is audited. After auditing a change, it may need to be applied or cherry-picked
 * on top of the latest snapshot instead of the one that was current when the audited changes were created.
 *
 * This class adds support for cherry-picking the changes from an orphan snapshot by applying them to
 * the current snapshot. The output of the operation is a new snapshot with the changes from cherry-picked
 * snapshot.
 *
 * Cherry-picking should apply the exact set of changes that were done in the original commit.
 *  - All added files should be added to the new version.
 *  - Todo: If files were deleted, then those files must still exist in the data set.
 *  - Does not support Overwrite operations currently. Overwrites are considered as conflicts.
 *
 */
class CherryPickFromSnapshot extends MergingSnapshotProducer<AppendFiles> implements CherryPick {
  private final TableOperations ops;
  private TableMetadata base = null;
  private Long cherryPickSnapshotId = null;
  private final AtomicInteger manifestCount = new AtomicInteger(0);

  CherryPickFromSnapshot(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.base = ops.refresh();
  }

  @Override
  protected AppendFiles self() {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * We only cherry pick for appends right now
   * @return
   */
  @Override
  protected String operation() {
    return DataOperations.APPEND;
  }

  @Override
  public CherryPickFromSnapshot fromSnapshotId(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot cherry pick unknown snapshot id: %s", snapshotId);

    this.cherryPickSnapshotId = snapshotId;
    return this;
  }

  /**
   * Apply the pending changes and return the uncommitted changes for validation.
   * <p>
   * This does not result in a permanent update.
   *
   * @return the uncommitted changes that would be committed by calling {@link #commit()}
   * @throws ValidationException      If the pending changes cannot be applied to the current metadata
   * @throws IllegalArgumentException If the pending changes are conflicting or invalid
   */
  @Override
  public Snapshot apply() {
    ValidationException.check(
        cherryPickSnapshotId != null,
        "Cannot cherry pick unknown version: call fromSnapshotId");

    Snapshot cherryPickSnapshot = base.snapshot(cherryPickSnapshotId);
    // only append operations are currently supported
    if (!cherryPickSnapshot.operation().equals(DataOperations.APPEND)) {
      throw new UnsupportedOperationException("Can cherry pick only append operations");
    }

    Snapshot currentSnapshot = base.currentSnapshot();
    Long parentSnapshotId = base.currentSnapshot() != null ?
        base.currentSnapshot().snapshotId() : null;

    // Todo:
    //  - Check if files to be deleted exist in current snapshot,
    //    ignore those files or reject incoming snapshot entirely?
    //  - Check if there are overwrites, ignore those files or reject incoming snapshot entirely?

    // create manifest file by picking Appends from cherry-pick snapshot
    List<ManifestFile> newManifestFiles = Lists.newArrayList();
    long outputSnapshotId = snapshotId();
    try {
      ManifestFile newManifestFile = createManifestFromAppends(cherryPickSnapshot.manifests(), outputSnapshotId);
      newManifestFiles.add(newManifestFile);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to create new manifest from cherry pick snapshot");
    }
    List<ManifestFile> manifestsForNewSnapshot = Lists.newArrayList(Iterables.concat(
        newManifestFiles, currentSnapshot.manifests()));

    // write out the manifest list
    OutputFile manifestList = createManifestList(parentSnapshotId, outputSnapshotId, manifestsForNewSnapshot);

    // create a fresh snapshot with changes from cherry pick snapshot and
    Snapshot outputSnapshot = new BaseSnapshot(ops.io(),
        outputSnapshotId, parentSnapshotId, System.currentTimeMillis(), cherryPickSnapshot.operation(),
        summary(base), ops.io().newInputFile(manifestList.location()));
    TableMetadata updated = base.addStagedSnapshot(outputSnapshot);
    ops.commit(base, updated);

    return outputSnapshot;
  }

  private OutputFile createManifestList(
      Long parentSnapshotId,
      long outputSnapshotId,
      List<ManifestFile> manifestsForNewSnapshot) {
    OutputFile manifestList = manifestListPath();
    try (ManifestListWriter listWriter = new ManifestListWriter(
        manifestList, outputSnapshotId, parentSnapshotId)) {

      ManifestFile[] manifestFiles = new ManifestFile[manifestsForNewSnapshot.size()];
      Tasks.range(manifestFiles.length)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(ThreadPools.getWorkerPool())
          .run(index ->
              manifestFiles[index] = manifestsForNewSnapshot.get(index));

      listWriter.addAll(Arrays.asList(manifestFiles));

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file");
    }
    return manifestList;
  }

  /**
   * Apply the pending changes and commit.
   * <p>
   * Changes are committed by calling the underlying table's commit method.
   * <p>
   * Once the commit is successful, the updated table will be refreshed.
   *
   * @throws ValidationException   If the update cannot be applied to the current table metadata.
   * @throws CommitFailedException If the update cannot be committed due to conflicts.
   */
  @Override
  public void commit() {
    // Todo: Need to add retry
    Snapshot outputSnapshot = apply();
    base = ops.refresh();
    ops.commit(base, base.cherrypickFrom(outputSnapshot));
  }

  /**
   * Looks for manifest entries that have append files from cherry-pick snapshot
   * and creates a new ManifestFile with a new snapshot
   */
  private ManifestFile createManifestFromAppends(List<ManifestFile> inputManifests, long outputSnapshotId)
      throws IOException {

    OutputFile out = manifestPath(manifestCount.getAndIncrement());
    // create a manifest writer with new snapshot id
    ManifestWriter writer = new ManifestWriter(ops.current().spec(), out, outputSnapshotId);
    try {
      for (ManifestFile manifest : inputManifests) {
        try (ManifestReader reader = ManifestReader.read(
            ops.io().newInputFile(manifest.path()), ops.current().specsById())) {
          for (ManifestEntry entry : reader.entries()) {
            if (entry.status() == ManifestEntry.Status.ADDED && entry.snapshotId() == cherryPickSnapshotId) {
              // add only manifests added in this snapshot
              writer.addEntry(entry);
            }
          }
        }
      }
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

}
