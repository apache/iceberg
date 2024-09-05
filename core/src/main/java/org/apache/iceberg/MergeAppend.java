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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Append implementation that produces a minimal number of manifest files.
 *
 * <p>This implementation will attempt to commit 5 times before throwing {@link
 * CommitFailedException}.
 */
class MergeAppend extends MergingSnapshotProducer<AppendFiles> implements AppendFiles {
  MergeAppend(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected AppendFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.APPEND;
  }

  @Override
  protected Long startingSnapshotId() {
    return null;
  }

  @Override
  public MergeAppend appendFile(DataFile file) {
    add(file);
    return this;
  }

  @Override
  public MergeAppend toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  public AppendFiles appendManifest(ManifestFile manifest) {
    Preconditions.checkArgument(
        !manifest.hasExistingFiles(), "Cannot append manifest with existing files");
    Preconditions.checkArgument(
        !manifest.hasDeletedFiles(), "Cannot append manifest with deleted files");
    Preconditions.checkArgument(
        manifest.snapshotId() == null || manifest.snapshotId() == -1,
        "Snapshot id must be assigned during commit");
    Preconditions.checkArgument(
        manifest.sequenceNumber() == -1, "Sequence must be assigned during commit");
    add(manifest);
    return this;
  }
}
