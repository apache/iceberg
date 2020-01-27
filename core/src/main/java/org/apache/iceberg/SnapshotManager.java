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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.WapUtil;

public class SnapshotManager extends MergingSnapshotProducer<ManageSnapshots> implements ManageSnapshots {

  private SnapshotManagerOperation operation;
  private TableMetadata base;
  private Long targetSnapshotId = null;
  private Table table;

  enum SnapshotManagerOperation {
    CHERRYPICK,
    ROLLBACK
  }

  SnapshotManager(TableOperations ops, Table table) {
    super(ops);
    this.base = ops.current();
    this.table = table;
  }

  @Override
  protected ManageSnapshots self() {
    return this;
  }

  @Override
  protected String operation() {
    Snapshot manageSnapshot = base.snapshot(targetSnapshotId);
    return manageSnapshot.operation();
  }

  @Override
  public ManageSnapshots cherrypick(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot cherry pick unknown snapshot id: %s", snapshotId);

    operation = SnapshotManagerOperation.CHERRYPICK;
    this.targetSnapshotId = snapshotId;

    // Pick modifications from the snapshot
    Snapshot cherryPickSnapshot = base.snapshot(this.targetSnapshotId);
    // only append operations are currently supported
    if (!cherryPickSnapshot.operation().equals(DataOperations.APPEND)) {
      throw new UnsupportedOperationException("Can cherry pick only append operations");
    }

    for (DataFile addedFile : cherryPickSnapshot.addedFiles()) {
      add(addedFile);
    }
    // this property is set on target snapshot that will get published
    String wapId = WapUtil.stagedWapId(cherryPickSnapshot);
    boolean isWapWorkflow =  wapId != null && !wapId.isEmpty();
    if (isWapWorkflow) {
      set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
    }
    // link the snapshot about to be published on commit with the picked snapshot
    set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(this.targetSnapshotId));

    return this;
  }

  @Override
  public ManageSnapshots setCurrentSnapshot(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);

    operation = SnapshotManagerOperation.ROLLBACK;
    this.targetSnapshotId = snapshotId;

    return this;
  }

  @Override
  public ManageSnapshots rollbackToTime(long timestampMillis) {
    operation = SnapshotManagerOperation.ROLLBACK;
    // find the latest snapshot by timestamp older than timestampMillis
    Snapshot snapshot = SnapshotUtil.findLatestSnapshotOlderThan(table, timestampMillis);
    Preconditions.checkArgument(snapshot != null,
        "Cannot roll back, no valid snapshot older than: %s", timestampMillis);
    this.targetSnapshotId = snapshot.snapshotId();

    return this;
  }

  @Override
  public ManageSnapshots rollbackTo(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);

    ValidationException.check(SnapshotUtil.isCurrentAncestor(table, snapshotId),
        "Cannot roll back to snapshot, not an ancestor of the current state: %s", snapshotId);
    return setCurrentSnapshot(snapshotId);
  }

  @Override
  public Snapshot apply() {
    if (targetSnapshotId == null) {
      // if no target snapshot was configured then NOOP by returning current state
      return table.currentSnapshot();
    }
    switch (operation) {
      case CHERRYPICK:
        WapUtil.validateNonAncestor(this.table, this.targetSnapshotId);
        WapUtil.validateWapPublish(this.table, this.targetSnapshotId);
        return super.apply();
      case ROLLBACK:
        return base.snapshot(targetSnapshotId);
      default:
        throw new ValidationException("Invalid SnapshotManagerOperation, " +
            "only cherrypick, rollback are supported");
    }
  }
}
