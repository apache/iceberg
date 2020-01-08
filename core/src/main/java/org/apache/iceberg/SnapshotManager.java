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

public class SnapshotManager extends MergingSnapshotProducer<ManageSnapshots> implements ManageSnapshots {

  private final TableOperations ops;
  private SnapshotManagerOperation operation;
  private TableMetadata base;
  private Long targetSnapshotId = null;

  enum SnapshotManagerOperation {
    CHERRYPICK,
    ROLLBACK
  }

  SnapshotManager(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.base = ops.current();
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
    return this;
  }

  @Override
  public ManageSnapshots rollback(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);

    operation = SnapshotManagerOperation.ROLLBACK;
    this.targetSnapshotId = snapshotId;
    return this;
  }

  @Override
  public ManageSnapshots rollbackAtTime(long timestampMillis) {
    long snapshotId = 0;
    long snapshotTimestamp = 0;
    operation = SnapshotManagerOperation.ROLLBACK;
    // find the latest snapshot by timestamp older than timestampMillis
    for (Snapshot snapshot : base.snapshots()) {
      if (snapshot.timestampMillis() < timestampMillis &&
          snapshot.timestampMillis() > snapshotTimestamp) {
        snapshotId = snapshot.snapshotId();
        snapshotTimestamp = snapshot.timestampMillis();
      }
    }

    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back, no valid snapshot older than: %s", timestampMillis);

    this.targetSnapshotId = snapshotId;

    return this;
  }

  @Override
  public Snapshot apply() {
    // common checks
    ValidationException.check(targetSnapshotId != null,
        "Cannot run operations on an unknown version: call rollback, rollbackAtTime or cherrypick");
    ValidationException.check(operation != null,
        "Need to define operation on snapshot: call rollback, rollbackAtTime or cherrypick");

    if (operation == SnapshotManagerOperation.CHERRYPICK) {

      Snapshot cherryPickSnapshot = base.snapshot(this.targetSnapshotId);
      String wapId = stagedWapId(cherryPickSnapshot);
      ValidationException.check(!base.isWapIdPublished(wapId),
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
      // this property is set on target snapshot that will get published
      set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);

      return super.apply();
    } else {
      return base.snapshot(targetSnapshotId);
    }
  }

  private static String stagedWapId(Snapshot snapshot) {
    return snapshot.summary() != null ? snapshot.summary().getOrDefault("wap.id", null) : null;
  }

}
