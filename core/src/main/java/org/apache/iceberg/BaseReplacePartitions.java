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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.util.PartitionSet;

public class BaseReplacePartitions extends MergingSnapshotProducer<ReplacePartitions>
    implements ReplacePartitions {

  private final PartitionSet replacedPartitions;
  private Long startingSnapshotId;
  private boolean validateConflictingData = false;
  private boolean validateConflictingDeletes = false;

  BaseReplacePartitions(String tableName, TableOperations ops) {
    super(tableName, ops);
    set(SnapshotSummary.REPLACE_PARTITIONS_PROP, "true");
    replacedPartitions = PartitionSet.create(ops.current().specsById());
  }

  @Override
  protected ReplacePartitions self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  public ReplacePartitions addFile(DataFile file) {
    dropPartition(file.specId(), file.partition());
    replacedPartitions.add(file.specId(), file.partition());
    add(file);
    return this;
  }

  @Override
  public ReplacePartitions validateAppendOnly() {
    failAnyDelete();
    return this;
  }

  @Override
  public ReplacePartitions validateFromSnapshot(long newStartingSnapshotId) {
    this.startingSnapshotId = newStartingSnapshotId;
    return this;
  }

  @Override
  public ReplacePartitions validateNoConflictingDeletes() {
    this.validateConflictingDeletes = true;
    return this;
  }

  @Override
  public ReplacePartitions validateNoConflictingData() {
    this.validateConflictingData = true;
    return this;
  }

  @Override
  public BaseReplacePartitions toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  public void validate(TableMetadata currentMetadata, Snapshot parent) {
    if (validateConflictingData) {
      if (dataSpec().isUnpartitioned()) {
        validateAddedDataFiles(
            currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
      } else {
        validateAddedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
      }
    }

    if (validateConflictingDeletes) {
      if (dataSpec().isUnpartitioned()) {
        validateDeletedDataFiles(
            currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
        validateNoNewDeleteFiles(
            currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
      } else {
        validateDeletedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
        validateNoNewDeleteFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
      }
    }
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    if (dataSpec().fields().size() <= 0) {
      // replace all data in an unpartitioned table
      deleteByRowFilter(Expressions.alwaysTrue());
    }

    try {
      return super.apply(base, snapshot);
    } catch (ManifestFilterManager.DeleteException e) {
      throw new ValidationException(
          "Cannot commit file that conflicts with existing partition: %s", e.partition());
    }
  }
}
