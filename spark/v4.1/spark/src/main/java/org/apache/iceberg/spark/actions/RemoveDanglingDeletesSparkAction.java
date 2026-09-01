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
package org.apache.iceberg.spark.actions;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutableRemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.DanglingDeleteFileUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any live data files.
 */
class RemoveDanglingDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeletesSparkAction>
    implements RemoveDanglingDeleteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveDanglingDeletesSparkAction.class);

  private final Table table;
  private String branch = SnapshotRef.MAIN_BRANCH;

  protected RemoveDanglingDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeletesSparkAction self() {
    return this;
  }

  public RemoveDanglingDeletesSparkAction toBranch(String targetBranch) {
    Preconditions.checkArgument(targetBranch != null, "Invalid branch name: null");
    this.branch = targetBranch;
    return this;
  }

  @Override
  public Result execute() {
    Preconditions.checkArgument(
        table.snapshot(branch) != null,
        "Cannot remove dangling delete files from branch %s: branch does not exist",
        branch);

    String desc = String.format("Removing dangling delete files in %s", table.name());
    return withJobGroupInfo(newJobGroupInfo("REMOVE-DELETES", desc), this::doExecute);
  }

  Result doExecute() {
    Snapshot snapshot = table.snapshot(branch);
    RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(snapshot.snapshotId());
    DeleteFileSet danglingDeletes = DanglingDeleteFileUtil.findDanglingDeletes(table, snapshot);

    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.debug("Removing dangling delete file {}", deleteFile.location());
      rewriteFiles.deleteFile(deleteFile);
    }

    if (!danglingDeletes.isEmpty()) {
      commit(rewriteFiles.toBranch(branch));
    }

    return ImmutableRemoveDanglingDeleteFiles.Result.builder()
        .removedDeleteFiles(danglingDeletes)
        .build();
  }
}
