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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * ToDo: Add SetSnapshotOperation operations such as setCurrentSnapshot, rollBackTime, rollbackTo to
 * this class so that we can support those operations for refs.
 */
class UpdateSnapshotReferencesOperation extends BasePendingUpdate<Map<String, SnapshotRef>>
    implements PendingUpdate<Map<String, SnapshotRef>> {

  private final TableOperations ops;
  private final Map<String, SnapshotRef> updatedRefs;
  private final TableMetadata base;

  UpdateSnapshotReferencesOperation(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.updatedRefs = Maps.newHashMap(base.refs());
  }

  @Override
  public Map<String, SnapshotRef> apply() {
    return updatedRefs;
  }

  @Override
  public void commit() {
    TableMetadata updated = internalApply();
    validate(base);
    ops.commit(base, updated);
  }

  public UpdateSnapshotReferencesOperation createBranch(String name, long snapshotId) {
    Preconditions.checkNotNull(name, "Branch name cannot be null");
    SnapshotRef branch = SnapshotRef.branchBuilder(snapshotId).build();
    SnapshotRef existingRef = updatedRefs.put(name, branch);
    Preconditions.checkArgument(existingRef == null, "Ref %s already exists", name);
    return this;
  }

  public UpdateSnapshotReferencesOperation createTag(String name, long snapshotId) {
    Preconditions.checkNotNull(name, "Tag name cannot be null");
    SnapshotRef tag = SnapshotRef.tagBuilder(snapshotId).build();
    SnapshotRef existingRef = updatedRefs.put(name, tag);
    Preconditions.checkArgument(existingRef == null, "Ref %s already exists", name);
    return this;
  }

  public UpdateSnapshotReferencesOperation removeBranch(String name) {
    Preconditions.checkNotNull(name, "Branch name cannot be null");
    Preconditions.checkArgument(!name.equals(SnapshotRef.MAIN_BRANCH), "Cannot remove main branch");
    SnapshotRef ref = updatedRefs.remove(name);
    Preconditions.checkArgument(ref != null, "Branch does not exist: %s", name);
    Preconditions.checkArgument(ref.isBranch(), "Ref %s is a tag not a branch", name);
    return this;
  }

  public UpdateSnapshotReferencesOperation removeTag(String name) {
    Preconditions.checkNotNull(name, "Tag name cannot be null");
    SnapshotRef ref = updatedRefs.remove(name);
    Preconditions.checkArgument(ref != null, "Tag does not exist: %s", name);
    Preconditions.checkArgument(ref.isTag(), "Ref %s is a branch not a tag", name);
    return this;
  }

  public UpdateSnapshotReferencesOperation renameBranch(String name, String newName) {
    Preconditions.checkNotNull(name, "Branch to rename cannot be null");
    Preconditions.checkNotNull(newName, "New branch name cannot be null");
    Preconditions.checkArgument(!name.equals(SnapshotRef.MAIN_BRANCH), "Cannot rename main branch");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Branch does not exist: %s", name);
    Preconditions.checkArgument(ref.isBranch(), "Ref %s is a tag not a branch", name);
    SnapshotRef existing = updatedRefs.put(newName, ref);
    Preconditions.checkArgument(existing == null, "Ref %s already exists", newName);
    updatedRefs.remove(name, ref);
    return this;
  }

  public UpdateSnapshotReferencesOperation replaceBranch(String name, long snapshotId) {
    Preconditions.checkNotNull(name, "Branch name cannot be null");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Branch does not exist: %s", name);
    Preconditions.checkArgument(ref.isBranch(), "Ref %s is a tag not a branch", name);
    SnapshotRef updatedRef = SnapshotRef.builderFrom(ref, snapshotId).build();
    updatedRefs.put(name, updatedRef);
    return this;
  }

  public UpdateSnapshotReferencesOperation replaceBranch(String from, String to) {
    return replaceBranch(from, to, false);
  }

  public UpdateSnapshotReferencesOperation fastForward(String from, String to) {
    return replaceBranch(from, to, true);
  }

  private UpdateSnapshotReferencesOperation replaceBranch(
      String from, String to, boolean fastForward) {
    Preconditions.checkNotNull(from, "Branch to update cannot be null");
    Preconditions.checkNotNull(to, "Destination ref cannot be null");
    SnapshotRef branchToUpdate = updatedRefs.get(from);
    SnapshotRef toRef = updatedRefs.get(to);
    Preconditions.checkArgument(toRef != null, "Ref does not exist: %s", to);
    if (branchToUpdate == null) {
      return createBranch(from, toRef.snapshotId());
    }

    Preconditions.checkArgument(branchToUpdate.isBranch(), "Ref %s is a tag not a branch", from);

    // Nothing to replace
    if (toRef.snapshotId() == branchToUpdate.snapshotId()) {
      return this;
    }

    SnapshotRef updatedRef = SnapshotRef.builderFrom(branchToUpdate, toRef.snapshotId()).build();

    if (fastForward) {
      boolean targetIsAncestor =
          SnapshotUtil.isAncestorOf(
              toRef.snapshotId(), branchToUpdate.snapshotId(), base::snapshot);
      Preconditions.checkArgument(
          targetIsAncestor, "Cannot fast-forward: %s is not an ancestor of %s", from, to);
    }

    updatedRefs.put(from, updatedRef);
    return this;
  }

  public UpdateSnapshotReferencesOperation replaceTag(String name, long snapshotId) {
    Preconditions.checkNotNull(name, "Tag name cannot be null");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Tag does not exist: %s", name);
    Preconditions.checkArgument(ref.isTag(), "Ref %s is a branch not a tag", name);
    SnapshotRef updatedRef = SnapshotRef.builderFrom(ref, snapshotId).build();
    updatedRefs.put(name, updatedRef);
    return this;
  }

  public UpdateSnapshotReferencesOperation setMinSnapshotsToKeep(
      String name, int minSnapshotsToKeep) {
    Preconditions.checkNotNull(name, "Branch name cannot be null");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Branch does not exist: %s", name);
    SnapshotRef updateBranch =
        SnapshotRef.builderFrom(ref).minSnapshotsToKeep(minSnapshotsToKeep).build();
    updatedRefs.put(name, updateBranch);
    return this;
  }

  public UpdateSnapshotReferencesOperation setMaxSnapshotAgeMs(String name, long maxSnapshotAgeMs) {
    Preconditions.checkNotNull(name, "Branch name cannot be null");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Branch does not exist: %s", name);
    SnapshotRef updateBranch =
        SnapshotRef.builderFrom(ref).maxSnapshotAgeMs(maxSnapshotAgeMs).build();
    updatedRefs.put(name, updateBranch);
    return this;
  }

  public UpdateSnapshotReferencesOperation setMaxRefAgeMs(String name, long maxRefAgeMs) {
    Preconditions.checkNotNull(name, "Reference name cannot be null");
    SnapshotRef ref = updatedRefs.get(name);
    Preconditions.checkArgument(ref != null, "Ref does not exist: %s", name);
    SnapshotRef updatedRef = SnapshotRef.builderFrom(ref).maxRefAgeMs(maxRefAgeMs).build();
    updatedRefs.put(name, updatedRef);
    return this;
  }

  private TableMetadata internalApply() {
    TableMetadata.Builder updatedBuilder = TableMetadata.buildFrom(base);
    // Identify references which have been removed
    Map<String, SnapshotRef> currRefs = base.refs();
    for (Map.Entry<String, SnapshotRef> currRefEntry : currRefs.entrySet()) {
      if (!updatedRefs.containsKey(currRefEntry.getKey())) {
        updatedBuilder.removeRef(currRefEntry.getKey());
      }
    }

    // Identify references which have been created or updated.
    for (Map.Entry<String, SnapshotRef> newRefEntry : updatedRefs.entrySet()) {
      final String name = newRefEntry.getKey();
      SnapshotRef currRef = currRefs.get(name);
      SnapshotRef updatedRef = updatedRefs.get(name);
      if (currRef == null || !currRef.equals(updatedRef)) {
        updatedBuilder.setRef(name, updatedRef);
      }
    }

    return updatedBuilder.build();
  }
}
