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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.WapUtil;

/**
 * Cherry-picks or fast-forwards the current state to a snapshot.
 *
 * <p>This update is not exposed though the Table API. Instead, it is a package-private part of the
 * Transaction API intended for use in {@link ManageSnapshots}.
 */
class CherryPickOperation extends MergingSnapshotProducer<CherryPickOperation> {

  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;
  private Snapshot cherrypickSnapshot = null;
  private boolean requireFastForward = false;
  private PartitionSet replacedPartitions = null;

  CherryPickOperation(String tableName, TableOperations ops) {
    super(tableName, ops);
    this.io = ops.io();
    this.specsById = ops.current().specsById();
  }

  @Override
  protected CherryPickOperation self() {
    return this;
  }

  @Override
  protected String operation() {
    // snapshotOperation is used by SnapshotProducer when building and writing a new snapshot for
    // cherrypick
    Preconditions.checkNotNull(cherrypickSnapshot, "[BUG] Detected uninitialized operation");
    return cherrypickSnapshot.operation();
  }

  public CherryPickOperation cherrypick(long snapshotId) {
    TableMetadata current = current();
    this.cherrypickSnapshot = current.snapshot(snapshotId);
    ValidationException.check(
        cherrypickSnapshot != null, "Cannot cherry-pick unknown snapshot ID: %s", snapshotId);

    if (cherrypickSnapshot.operation().equals(DataOperations.APPEND)) {
      // this property is set on target snapshot that will get published
      String wapId = WapUtil.validateWapPublish(current, snapshotId);
      if (wapId != null) {
        set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
      }

      // link the snapshot about to be published on commit with the picked snapshot
      set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(snapshotId));

      // Pick modifications from the snapshot
      for (DataFile addedFile : cherrypickSnapshot.addedDataFiles(io)) {
        add(addedFile);
      }

    } else if (cherrypickSnapshot.operation().equals(DataOperations.OVERWRITE)
        && PropertyUtil.propertyAsBoolean(
            cherrypickSnapshot.summary(), SnapshotSummary.REPLACE_PARTITIONS_PROP, false)) {
      // the operation was ReplacePartitions. this can be cherry-picked iff the partitions have not
      // been modified.
      // detecting modification requires finding the new files since the parent was committed, so
      // the parent must be an
      // ancestor of the current state, or null if the overwrite was based on an empty table.
      ValidationException.check(
          cherrypickSnapshot.parentId() == null
              || isCurrentAncestor(current, cherrypickSnapshot.parentId()),
          "Cannot cherry-pick overwrite not based on an ancestor of the current state: %s",
          snapshotId);

      // this property is set on target snapshot that will get published
      String wapId = WapUtil.validateWapPublish(current, snapshotId);
      if (wapId != null) {
        set(SnapshotSummary.PUBLISHED_WAP_ID_PROP, wapId);
      }

      // link the snapshot about to be published on commit with the picked snapshot
      set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(snapshotId));

      // check that all deleted files are still in the table
      failMissingDeletePaths();

      // copy adds from the picked snapshot
      this.replacedPartitions = PartitionSet.create(specsById);
      for (DataFile addedFile : cherrypickSnapshot.addedDataFiles(io)) {
        add(addedFile);
        replacedPartitions.add(addedFile.specId(), addedFile.partition());
      }

      // copy deletes from the picked snapshot
      for (DataFile deletedFile : cherrypickSnapshot.removedDataFiles(io)) {
        delete(deletedFile);
      }

    } else {
      // attempt to fast-forward
      ValidationException.check(
          isFastForward(current),
          "Cannot cherry-pick snapshot %s: not append, dynamic overwrite, or fast-forward",
          cherrypickSnapshot.snapshotId());
      this.requireFastForward = true;
    }

    return this;
  }

  @Override
  public Object updateEvent() {
    if (cherrypickSnapshot == null) {
      // NOOP operation, no snapshot created
      return null;
    }

    TableMetadata tableMetadata = refresh();
    long snapshotId = tableMetadata.currentSnapshot().snapshotId();
    if (cherrypickSnapshot.snapshotId() == snapshotId) {
      // No new snapshot is created for fast-forward
      return null;
    } else {
      // New snapshot created, we rely on super class to fire a CreateSnapshotEvent
      return super.updateEvent();
    }
  }

  @Override
  protected void validate(TableMetadata base, Snapshot snapshot) {
    // this is only called after apply() passes off to super, but check fast-forward status just in
    // case
    if (!isFastForward(base)) {
      validateNonAncestor(base, cherrypickSnapshot.snapshotId());
      validateReplacedPartitions(base, cherrypickSnapshot.parentId(), replacedPartitions, io);
      WapUtil.validateWapPublish(base, cherrypickSnapshot.snapshotId());
    }
  }

  private boolean isFastForward(TableMetadata base) {
    if (base.currentSnapshot() != null) {
      // can fast-forward if the cherry-picked snapshot's parent is the current snapshot
      return cherrypickSnapshot.parentId() != null
          && base.currentSnapshot().snapshotId() == cherrypickSnapshot.parentId();
    } else {
      // ... or if the parent and current snapshot are both null
      return cherrypickSnapshot.parentId() == null;
    }
  }

  @Override
  public Snapshot apply() {
    TableMetadata base = refresh();

    if (cherrypickSnapshot == null) {
      // if no target snapshot was configured then NOOP by returning current state
      return base.currentSnapshot();
    }

    boolean isFastForward = isFastForward(base);
    if (requireFastForward || isFastForward) {
      ValidationException.check(
          isFastForward,
          "Cannot cherry-pick snapshot %s: not append, dynamic overwrite, or fast-forward",
          cherrypickSnapshot.snapshotId());
      return base.snapshot(cherrypickSnapshot.snapshotId());
    } else {
      // validate(TableMetadata) is called in apply(TableMetadata) after this apply refreshes the
      // table state
      return super.apply();
    }
  }

  private static void validateNonAncestor(TableMetadata meta, long snapshotId) {
    if (isCurrentAncestor(meta, snapshotId)) {
      throw new CherrypickAncestorCommitException(snapshotId);
    }

    Long ancestorId = lookupAncestorBySourceSnapshot(meta, snapshotId);
    if (ancestorId != null) {
      throw new CherrypickAncestorCommitException(snapshotId, ancestorId);
    }
  }

  private static void validateReplacedPartitions(
      TableMetadata meta, Long parentId, PartitionSet replacedPartitions, FileIO io) {
    if (replacedPartitions != null && meta.currentSnapshot() != null) {
      ValidationException.check(
          parentId == null || isCurrentAncestor(meta, parentId),
          "Cannot cherry-pick overwrite, based on non-ancestor of the current state: %s",
          parentId);
      List<DataFile> newFiles =
          SnapshotUtil.newFiles(parentId, meta.currentSnapshot().snapshotId(), meta::snapshot, io);
      for (DataFile newFile : newFiles) {
        ValidationException.check(
            !replacedPartitions.contains(newFile.specId(), newFile.partition()),
            "Cannot cherry-pick replace partitions with changed partition: %s",
            newFile.partition());
      }
    }
  }

  private static Long lookupAncestorBySourceSnapshot(TableMetadata meta, long snapshotId) {
    String snapshotIdStr = String.valueOf(snapshotId);
    for (long ancestorId : currentAncestors(meta)) {
      Map<String, String> summary = meta.snapshot(ancestorId).summary();
      if (summary != null
          && snapshotIdStr.equals(summary.get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))) {
        return ancestorId;
      }
    }

    return null;
  }

  private static List<Long> currentAncestors(TableMetadata meta) {
    return SnapshotUtil.ancestorIds(meta.currentSnapshot(), meta::snapshot);
  }

  private static boolean isCurrentAncestor(TableMetadata meta, long snapshotId) {
    return currentAncestors(meta).contains(snapshotId);
  }
}
