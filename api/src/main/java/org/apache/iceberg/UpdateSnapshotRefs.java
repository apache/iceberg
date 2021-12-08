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

import java.util.Map;
import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * API for snapshot reference evolution.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdateSnapshotRefs extends PendingUpdate<Map<String, SnapshotRef>> {

  /**
   * Assign a tag to a snapshot.
   *
   * @param name tag name
   * @param snapshotId ID of the snapshot to tag
   * @return this for method chaining
   * @throws IllegalArgumentException if ref with the same name already exists
   */
  UpdateSnapshotRefs tag(String name, long snapshotId);

  /**
   * Create a branch and set a snapshot as the branch head.
   *
   * @param name branch name
   * @param snapshotId ID of the snapshot to set as the branch head
   * @return this for method chaining
   * @throws IllegalArgumentException if ref with the same name already exists
   */
  UpdateSnapshotRefs branch(String name, long snapshotId);

  /**
   * Remove a tag or branch reference.
   *
   * @param name reference name
   * @return this for method chaining
   * @throws IllegalArgumentException if the reference does not exist
   */
  UpdateSnapshotRefs remove(String name);

  /**
   * Rename a tag or branch reference.
   * Any configuration associated with the ref does not change.
   *
   * @param from ref to rename
   * @param to renamed ref
   * @throws IllegalArgumentException if from ref does not exist or to ref already exists
   */
  UpdateSnapshotRefs rename(String from, String to);

  /**
   * Set the maximum age of the tag or branch reference while expiring snapshots
   *
   * @param name reference name
   * @param ageMs maximum age in millisecond
   * @return this for method chaining
   * @throws IllegalArgumentException if the reference does not exist
   */
  UpdateSnapshotRefs setLifetime(String name, long ageMs);

  /**
   * Set the maximum age of snapshots in a branch while expiring snapshots
   *
   * @param name branch name
   * @param ageMs maximum age in millisecond
   * @return this for method chaining
   * @throws IllegalArgumentException if the reference does not exist
   */
  UpdateSnapshotRefs setBranchSnapshotLifetime(String name, long ageMs);

  /**
   * Set the minimum number of snapshots to keep in a branch while expiring snapshots
   *
   * @param name branch name
   * @param numToKeep minimum number of snapshots to keep
   * @return this for method chaining
   * @throws IllegalArgumentException if the reference does not exist
   */
  UpdateSnapshotRefs setMinSnapshotsInBranch(String name, int numToKeep);

}
