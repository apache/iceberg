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

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * API for managing snapshots. Allows rolling table data back to a stated at an older table {@link
 * Snapshot snapshot}. Rollback:
 *
 * <p>This API does not allow conflicting calls to {@link #setCurrentSnapshot(long)} and {@link
 * #rollbackToTime(long)}.
 *
 * <p>When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}. Cherrypick:
 *
 * <p>In an audit workflow, new data is written to an orphan {@link Snapshot snapshot} that is not
 * committed as the table's current state until it is audited. After auditing a change, it may need
 * to be applied or cherry-picked on top of the latest snapshot instead of the one that was current
 * when the audited changes were created. This class adds support for cherry-picking the changes
 * from an orphan snapshot by applying them to the current snapshot. The output of the operation is
 * a new snapshot with the changes from cherry-picked snapshot.
 *
 * <p>
 */
public interface ManageSnapshots extends PendingUpdate<Snapshot> {

  /**
   * Roll this table's data back to a specific {@link Snapshot} identified by id.
   *
   * @param snapshotId long id of the snapshot to roll back table data to
   * @return this for method chaining
   * @throws IllegalArgumentException If the table has no snapshot with the given id
   */
  ManageSnapshots setCurrentSnapshot(long snapshotId);

  /**
   * Roll this table's data back to the last {@link Snapshot} before the given timestamp.
   *
   * @param timestampMillis a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   * @throws IllegalArgumentException If the table has no old snapshot before the given timestamp
   */
  ManageSnapshots rollbackToTime(long timestampMillis);

  /**
   * Rollback table's state to a specific {@link Snapshot} identified by id.
   *
   * @param snapshotId long id of snapshot id to roll back table to. Must be an ancestor of the
   *     current snapshot
   * @throws IllegalArgumentException If the table has no snapshot with the given id
   * @throws ValidationException If given snapshot id is not an ancestor of the current state
   */
  ManageSnapshots rollbackTo(long snapshotId);

  /**
   * Apply supported changes in given snapshot and create a new snapshot which will be set as the
   * current snapshot on commit.
   *
   * @param snapshotId a snapshotId whose changes to apply
   * @return this for method chaining
   * @throws IllegalArgumentException If the table has no snapshot with the given id
   * @throws DuplicateWAPCommitException In case of a WAP workflow and if the table has a duplicate
   *     commit with same wapId
   */
  ManageSnapshots cherrypick(long snapshotId);

  /**
   * Create a new branch. The branch will point to current snapshot if the current snapshot is not
   * NULL. Otherwise, the branch will point to a newly created empty snapshot.
   *
   * @param name branch name
   * @return this for method chaining
   * @throws IllegalArgumentException if a branch with the given name already exists
   */
  default ManageSnapshots createBranch(String name) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement createBranch(String)");
  }

  /**
   * Create a new branch pointing to the given snapshot id.
   *
   * @param name branch name
   * @param snapshotId id of the snapshot which will be the head of the branch
   * @return this for method chaining
   * @throws IllegalArgumentException if a branch with the given name already exists
   */
  ManageSnapshots createBranch(String name, long snapshotId);

  /**
   * Create a new tag pointing to the given snapshot id
   *
   * @param name tag name
   * @param snapshotId snapshotId for the head of the new branch.
   * @return this for method chaining
   * @throws IllegalArgumentException if a tag with the given name already exists
   */
  ManageSnapshots createTag(String name, long snapshotId);

  /**
   * Remove a branch by name
   *
   * @param name branch name
   * @return this for method chaining
   * @throws IllegalArgumentException if the branch does not exist
   */
  ManageSnapshots removeBranch(String name);

  /**
   * Rename a branch
   *
   * @param name name of branch to rename
   * @param newName the desired new name of the branch
   * @throws IllegalArgumentException if the branch to rename does not exist or if there is already
   *     a branch with the same name as the desired new name.
   */
  ManageSnapshots renameBranch(String name, String newName);

  /**
   * Remove the tag with the given name.
   *
   * @param name tag name
   * @return this for method chaining
   * @throws IllegalArgumentException if the branch does not exist
   */
  ManageSnapshots removeTag(String name);

  /**
   * Replaces the tag with the given name to point to the specified snapshot.
   *
   * @param name Tag to replace
   * @param snapshotId new snapshot id for the given tag
   * @return this for method chaining
   */
  ManageSnapshots replaceTag(String name, long snapshotId);

  /**
   * Replaces the branch with the given name to point to the specified snapshot
   *
   * @param name Branch to replace
   * @param snapshotId new snapshot id for the given branch
   * @return this for method chaining
   */
  ManageSnapshots replaceBranch(String name, long snapshotId);

  /**
   * Replaces the {@code from} branch to point to the {@code to} snapshot. The {@code to} will
   * remain unchanged, and {@code from} branch will retain its retention properties.
   *
   * @param from Branch to replace
   * @param to The branch {@code from} should be replaced with
   * @return this for method chaining
   */
  ManageSnapshots replaceBranch(String from, String to);

  /**
   * Performs a fast-forward of {@code from} up to the {@code to} snapshot if {@code from} is an
   * ancestor of {@code to}. The {@code to} will remain unchanged, and {@code from} will retain its
   * retention properties.
   *
   * @param from Branch to fast-forward
   * @param to Ref for the {@code from} branch to be fast forwarded to
   * @return this for method chaining
   * @throws IllegalArgumentException if {@code from} is not an ancestor of {@code to}
   */
  ManageSnapshots fastForwardBranch(String from, String to);

  /**
   * Updates the minimum number of snapshots to keep for a branch.
   *
   * @param branchName branch name
   * @param minSnapshotsToKeep minimum number of snapshots to retain on the branch
   * @return this for method chaining
   * @throws IllegalArgumentException if the branch does not exist
   */
  ManageSnapshots setMinSnapshotsToKeep(String branchName, int minSnapshotsToKeep);

  /**
   * Updates the max snapshot age for a branch.
   *
   * @param branchName branch name
   * @param maxSnapshotAgeMs maximum snapshot age in milliseconds to retain on branch
   * @return this for method chaining
   * @throws IllegalArgumentException if the branch does not exist
   */
  ManageSnapshots setMaxSnapshotAgeMs(String branchName, long maxSnapshotAgeMs);

  /**
   * Updates the retention policy for a reference.
   *
   * @param name branch name
   * @param maxRefAgeMs retention age in milliseconds of the tag reference itself
   * @return this for method chaining
   * @throws IllegalArgumentException if the reference does not exist
   */
  ManageSnapshots setMaxRefAgeMs(String name, long maxRefAgeMs);
}
