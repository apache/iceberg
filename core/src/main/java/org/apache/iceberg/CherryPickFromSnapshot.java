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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;

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
class CherryPickFromSnapshot extends MergingSnapshotProducer<CherryPick> implements CherryPick {
  private final TableOperations ops;
  private TableMetadata base;
  private Long cherryPickSnapshotId = null;

  CherryPickFromSnapshot(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.base = ops.refresh();
  }

  @Override
  protected CherryPick self() {
    return this;
  }

  /**
   * We only cherry pick for appends right now
   * @return
   */
  @Override
  protected String operation() {
    Snapshot cherryPickSnapshot = base.snapshot(cherryPickSnapshotId);
    return cherryPickSnapshot.operation();
  }

  @Override
  public CherryPickFromSnapshot cherrypick(long snapshotId) {
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
    ValidationException.check(cherryPickSnapshotId != null,
        "Cannot cherry pick unknown version: call cherrypick");

    Snapshot cherryPickSnapshot = base.snapshot(cherryPickSnapshotId);
    String wapId = stagedWapId(cherryPickSnapshot);
    ValidationException.check(!base.isWapIdPublished(Long.parseLong(wapId)),
        "Duplicate request to cherry pick wap id that was published already: %s", wapId);

    // only append operations are currently supported
    if (!cherryPickSnapshot.operation().equals(DataOperations.APPEND)) {
      throw new UnsupportedOperationException("Can cherry pick only append operations");
    }

    // Todo:
    //  - Check if files to be deleted exist in current snapshot,
    //    ignore those files or reject incoming snapshot entirely?
    //  - Check if there are overwrites, ignore those files or reject incoming snapshot entirely?

    for (DataFile addedFile : cherryPickSnapshot.addedFiles()) {
      add(addedFile);
    }
    set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
    Snapshot outputSnapshot = super.apply();
    TableMetadata updated = base.addStagedSnapshot(outputSnapshot);
    ops.commit(base, updated);
    return outputSnapshot;
  }

  private static String stagedWapId(Snapshot snapshot) {
    return snapshot.summary() != null ? snapshot.summary().getOrDefault("wap.id", null) : null;
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
}
