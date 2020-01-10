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
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;

public class SnapshotManager extends MergingSnapshotProducer<ManageSnapshots> implements ManageSnapshots {

  private final TableOperations ops;
  private SnapshotManagerOperation operation;
  private TableMetadata base;
  private Long targetSnapshotId = null;
  private Set<Long> snapshotsAlreadyCherrypicked;

  enum SnapshotManagerOperation {
    CHERRYPICK,
    ROLLBACK
  }

  SnapshotManager(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.base = ops.current();
    this.snapshotsAlreadyCherrypicked = new HashSet<>();
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
  public ManageSnapshots rollbackToTime(long timestampMillis) {
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

    switch (operation) {
      case CHERRYPICK:
        Snapshot cherryPickSnapshot = base.snapshot(this.targetSnapshotId);
        String wapId = stagedWapId(cherryPickSnapshot);
        boolean isWapWorkflow =  wapId != null && !"".equals(wapId);
        if (isWapWorkflow) {
          ValidationException.check(!base.isWapIdPublished(wapId),
              "Duplicate request to cherry pick wap id that was published already: %s", wapId);
        }
        // only append operations are currently supported
        if (!cherryPickSnapshot.operation().equals(DataOperations.APPEND)) {
          throw new UnsupportedOperationException("Can cherry pick only append operations");
        }
        // this is to ensure we add files only once for each targetSnapshotId, to protect from
        // duplicate additions if commit retries the same cherrypick operation on failure.
        if (!snapshotsAlreadyCherrypicked.contains(targetSnapshotId)) {
          for (DataFile addedFile : cherryPickSnapshot.addedFiles()) {
            add(addedFile);
          }
          // this property is set on target snapshot that will get published
          if (isWapWorkflow) {
            set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
          }
          snapshotsAlreadyCherrypicked.add(targetSnapshotId);
        }

        return super.apply();
      case ROLLBACK:
        return base.snapshot(targetSnapshotId);
      default:
        throw new ValidationException("Invalid SnapshotManagerOperation, only cherrypick, rollback are supported");
    }
  }

  private static String stagedWapId(Snapshot snapshot) {
    return snapshot.summary() != null ? snapshot.summary().getOrDefault("wap.id", null) : null;
  }

}
