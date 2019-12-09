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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
 * the current snapshot.
 *
 * Cherry-picking should apply the exact set of changes that were done in the original commit.
 *  - If files were deleted, then those files must still exist in the data set.
 *  - All added files should be added to the new version.
 *  - Does not support Overwrite operations currently. Overwrites are considered as conflicts.
 *
 */
class CherryPickFromSnapshot extends MergingSnapshotProducer<AppendFiles> implements CherryPick {
  private final TableOperations ops;
  private TableMetadata base = null;
  private Long cherryPickSnapshotId = null;

  CherryPickFromSnapshot(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.base = ops.refresh();
  }

  @Override
  protected AppendFiles self() {
    throw new UnsupportedOperationException("Not supported");
  }

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

    // Fetch latest table metadata after checking for any recent updates
    this.base = ops.refresh();
    Snapshot cherryPickSnapshot = base.snapshot(cherryPickSnapshotId);
    Snapshot currentSnapshot = base.currentSnapshot();
    Long parentSnapshotId = base.currentSnapshot() != null ?
        base.currentSnapshot().snapshotId() : null;

    // Get target snapshot to cherry-pick from and pick out
    // deletes and appends from it to apply over current snapshot
    Iterable<DataFile> addedFiles = cherryPickSnapshot.addedFiles();
    Iterable<DataFile> deletedFiles = cherryPickSnapshot.deletedFiles();
    List<ManifestFile> manifests = cherryPickSnapshot.manifests();
    Iterable<ManifestFile> manifestsInCherryPickSnapshot = Iterables.filter(
        manifests,
        // only keep manifests that have live data files or that were written by this commit
        manifest -> manifest.hasAddedFiles() ||
            manifest.hasDeletedFiles() ||
            manifest.hasExistingFiles() ||
            manifest.snapshotId() == cherryPickSnapshotId);

    // Todo Checks:
    //  - Check if files to be deleted exist in current snapshot,
    //    ignore those files or reject incoming snapshot entirely?
    //  - Check if there are overwrites, ignore those files or reject incoming snapshot entirely?

    // only append operations are currently supported
    if (!(cherryPickSnapshot.operation().equals(DataOperations.APPEND) ||
            cherryPickSnapshot.operation().equals(DataOperations.DELETE))) {
      throw new UnsupportedOperationException("Can cherry pick only append and delete operations");
    }

    OutputFile manifestList = manifestListPath();
    try (ManifestListWriter writer = new ManifestListWriter(
        manifestList, snapshotId(), parentSnapshotId)) {

      // keep track of the manifest lists created
      // manifestLists.add(manifestList.location());

      ManifestFile[] manifestFiles = new ManifestFile[manifests.size()];

      Tasks.range(manifestFiles.length)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(ThreadPools.getWorkerPool())
          .run(index ->
              manifestFiles[index] = manifests.get(index));

      writer.addAll(Arrays.asList(manifestFiles));

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file");
    }

    // create a fresh snapshot with changes from cherry pick snapshot and
    return new BaseSnapshot(ops.io(),
        ops.newSnapshotId(), parentSnapshotId, System.currentTimeMillis(), cherryPickSnapshot.operation(),
        summary(base), ops.io().newInputFile(manifestList.location()));
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
    base = ops.refresh(); // refresh
    ops.commit(base, base.cherrypickFrom(apply()));
  }
}
