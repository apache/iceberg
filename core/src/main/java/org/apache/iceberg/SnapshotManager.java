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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class SnapshotManager implements ManageSnapshots {

  private final boolean isExternalTransaction;
  private final BaseTransaction transaction;
  private UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation;

  SnapshotManager(String tableName, TableOperations ops) {
    Preconditions.checkState(
        ops.current() != null, "Cannot manage snapshots: table %s does not exist", tableName);
    this.transaction =
        new BaseTransaction(tableName, ops, BaseTransaction.TransactionType.SIMPLE, ops.refresh());
    this.isExternalTransaction = false;
  }

  SnapshotManager(BaseTransaction transaction) {
    Preconditions.checkArgument(transaction != null, "Invalid input transaction: null");
    this.transaction = transaction;
    this.isExternalTransaction = true;
  }

  @Override
  public ManageSnapshots cherrypick(long snapshotId) {
    commitIfRefUpdatesExist();
    transaction.cherryPick().cherrypick(snapshotId).commit();
    return this;
  }

  @Override
  public ManageSnapshots setCurrentSnapshot(long snapshotId) {
    commitIfRefUpdatesExist();
    transaction.setBranchSnapshot().setCurrentSnapshot(snapshotId).commit();
    return this;
  }

  @Override
  public ManageSnapshots rollbackToTime(long timestampMillis) {
    commitIfRefUpdatesExist();
    transaction.setBranchSnapshot().rollbackToTime(timestampMillis).commit();
    return this;
  }

  @Override
  public ManageSnapshots rollbackTo(long snapshotId) {
    commitIfRefUpdatesExist();
    transaction.setBranchSnapshot().rollbackTo(snapshotId).commit();
    return this;
  }

  @Override
  public ManageSnapshots createBranch(String name, long snapshotId) {
    updateSnapshotReferencesOperation().createBranch(name, snapshotId);
    return this;
  }

  @Override
  public ManageSnapshots createTag(String name, long snapshotId) {
    updateSnapshotReferencesOperation().createTag(name, snapshotId);
    return this;
  }

  @Override
  public ManageSnapshots removeBranch(String name) {
    updateSnapshotReferencesOperation().removeBranch(name);
    return this;
  }

  @Override
  public ManageSnapshots removeTag(String name) {
    updateSnapshotReferencesOperation().removeTag(name);
    return this;
  }

  @Override
  public ManageSnapshots setMinSnapshotsToKeep(String name, int minSnapshotsToKeep) {
    updateSnapshotReferencesOperation().setMinSnapshotsToKeep(name, minSnapshotsToKeep);
    return this;
  }

  @Override
  public ManageSnapshots setMaxSnapshotAgeMs(String name, long maxSnapshotAgeMs) {
    updateSnapshotReferencesOperation().setMaxSnapshotAgeMs(name, maxSnapshotAgeMs);
    return this;
  }

  @Override
  public ManageSnapshots setMaxRefAgeMs(String name, long maxRefAgeMs) {
    updateSnapshotReferencesOperation().setMaxRefAgeMs(name, maxRefAgeMs);
    return this;
  }

  @Override
  public ManageSnapshots replaceTag(String name, long snapshotId) {
    updateSnapshotReferencesOperation().replaceTag(name, snapshotId);
    return this;
  }

  @Override
  public ManageSnapshots replaceBranch(String name, long snapshotId) {
    updateSnapshotReferencesOperation().replaceBranch(name, snapshotId);
    return this;
  }

  @Override
  public ManageSnapshots replaceBranch(String name, String source) {
    updateSnapshotReferencesOperation().replaceBranch(name, source);
    return this;
  }

  @Override
  public ManageSnapshots fastForwardBranch(String name, String source) {
    updateSnapshotReferencesOperation().fastForward(name, source);
    return this;
  }

  @Override
  public ManageSnapshots renameBranch(String name, String newName) {
    updateSnapshotReferencesOperation().renameBranch(name, newName);
    return this;
  }

  private UpdateSnapshotReferencesOperation updateSnapshotReferencesOperation() {
    if (updateSnapshotReferencesOperation == null) {
      this.updateSnapshotReferencesOperation = transaction.updateSnapshotReferencesOperation();
    }

    return updateSnapshotReferencesOperation;
  }

  private void commitIfRefUpdatesExist() {
    if (updateSnapshotReferencesOperation != null) {
      updateSnapshotReferencesOperation.commit();
      updateSnapshotReferencesOperation = null;
    }
  }

  @Override
  public Snapshot apply() {
    return transaction.table().currentSnapshot();
  }

  @Override
  public void commit() {
    commitIfRefUpdatesExist();
    if (!isExternalTransaction) {
      transaction.commitTransaction();
    }
  }
}
