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

public class BaseUpdateSnapshotRefs implements UpdateSnapshotRefs {

  private final TableOperations ops;
  private final TableMetadata base;
  private final Map<String, SnapshotRef> refs;

  BaseUpdateSnapshotRefs(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.refs = Maps.newHashMap(base.refs());
  }

  @Override
  public UpdateSnapshotRefs tag(String name, long snapshotId) {
    Preconditions.checkArgument(name != null, "Tag name must not be null");
    Preconditions.checkArgument(base.snapshot(snapshotId) != null, "Cannot find snapshot with ID: %s", snapshotId);
    Preconditions.checkArgument(!refs.containsKey(name), "Cannot tag snapshot, ref already exists: %s", name);

    refs.put(name, SnapshotRef.builderForTag(snapshotId).build());
    return this;
  }

  @Override
  public UpdateSnapshotRefs branch(String name, long snapshotId) {
    Preconditions.checkArgument(name != null, "Branch name must not be null");
    Preconditions.checkArgument(base.snapshot(snapshotId) != null, "Cannot find snapshot with ID: %s", snapshotId);
    Preconditions.checkArgument(!refs.containsKey(name), "Cannot create branch, ref already exists: %s", name);

    refs.put(name, SnapshotRef.builderForBranch(snapshotId).build());
    return this;
  }

  @Override
  public UpdateSnapshotRefs remove(String name) {
    Preconditions.checkArgument(name != null, "Ref name must not be null");
    Preconditions.checkArgument(refs.containsKey(name), "Cannot find ref to remove: %s", name);
    Preconditions.checkArgument(!SnapshotRef.MAIN_BRANCH.equals(name), "Main branch must not be removed");

    refs.remove(name);
    return this;
  }

  @Override
  public UpdateSnapshotRefs rename(String from, String to) {
    Preconditions.checkArgument(from != null && to != null, "Names must not be null");
    Preconditions.checkArgument(refs.containsKey(from), "Cannot find ref to rename from: %s", from);
    Preconditions.checkArgument(!refs.containsKey(to), "Cannot rename to an existing ref: %s", to);

    refs.put(to, refs.remove(from));
    return this;
  }

  @Override
  public UpdateSnapshotRefs setLifetime(String name, long ageMs) {
    Preconditions.checkArgument(name != null, "Branch name must not be null");
    Preconditions.checkArgument(ageMs > 0, "Lifetime must be positive");
    Preconditions.checkArgument(refs.containsKey(name), "Cannot find ref to set lifetime: %s", name);
    Preconditions.checkArgument(!SnapshotRef.MAIN_BRANCH.equals(name), "Main branch is retained forever");

    SnapshotRef oldRef = refs.get(name);
    refs.put(name, SnapshotRef.builderFrom(oldRef).maxRefAgeMs(ageMs).build());
    return this;
  }

  @Override
  public UpdateSnapshotRefs setBranchSnapshotLifetime(String name, long ageMs) {
    Preconditions.checkArgument(name != null, "Branch name must not be null");
    Preconditions.checkArgument(ageMs > 0, "Lifetime must be positive");
    Preconditions.checkArgument(refs.containsKey(name), "Cannot find ref to set branch snapshot lifetime: %s", name);

    SnapshotRef oldRef = refs.get(name);
    Preconditions.checkArgument(oldRef.type() == SnapshotRefType.BRANCH,
        "Branch snapshot lifetime cannot be set for %s of type %s", name, oldRef.type());

    refs.put(name, SnapshotRef.builderFrom(oldRef).maxSnapshotAgeMs(ageMs).build());
    return this;
  }

  @Override
  public UpdateSnapshotRefs setMinSnapshotsInBranch(String name, int numToKeep) {
    Preconditions.checkArgument(name != null, "Branch name must not be null");
    Preconditions.checkArgument(numToKeep > 0, "Snapshots to keep number must be positive");
    Preconditions.checkArgument(refs.containsKey(name), "Cannot find ref to set snapshots in branch: %s", name);

    SnapshotRef oldRef = refs.get(name);
    Preconditions.checkArgument(oldRef.type() == SnapshotRefType.BRANCH,
        "Branch snapshots to keep cannot be set for %s of type %s", name, oldRef.type());

    refs.put(name, SnapshotRef.builderFrom(oldRef).minSnapshotsToKeep(numToKeep).build());
    return this;
  }

  @Override
  public Map<String, SnapshotRef> apply() {
    return refs;
  }

  @Override
  public void commit() {
    TableMetadata updated = base.replaceRefs(apply());
    ops.commit(base, updated);
  }
}
