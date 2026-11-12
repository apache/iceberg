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

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.FileCleanupStrategy.DeleteSummary;
import org.apache.iceberg.FileCleanupStrategy.DeletedFileType;
import org.apache.iceberg.actions.ImmutableExpireSnapshots;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Expires snapshots and deletes the files they no longer reference, all in the calling process.
 *
 * <p>This runs the same commit and file cleanup as {@link Table#expireSnapshots()} and also returns
 * the number of deleted files by type.
 *
 * <p>When a function is set with {@link #deleteWith(Consumer)}, a file counts as deleted once that
 * function returns without throwing.
 */
public class ExpireSnapshotsAction implements org.apache.iceberg.actions.ExpireSnapshots {
  private final RemoveSnapshots removeSnapshots;

  /**
   * Creates an action for a table loaded from a catalog.
   *
   * <p>Tables of a {@link Transaction} are not supported, because this action deletes files before
   * the transaction commits.
   *
   * @param table the table to expire snapshots from
   */
  public ExpireSnapshotsAction(Table table) {
    this(new RemoveSnapshots(operations(table)));
  }

  ExpireSnapshotsAction(RemoveSnapshots removeSnapshots) {
    this.removeSnapshots = removeSnapshots;
  }

  private static TableOperations operations(Table table) {
    Preconditions.checkArgument(
        table instanceof BaseTable,
        "Cannot expire snapshots of table %s with unsupported type: %s",
        table.name(),
        table.getClass().getName());
    return ((BaseTable) table).operations();
  }

  @Override
  public ExpireSnapshotsAction expireSnapshotId(long snapshotId) {
    removeSnapshots.expireSnapshotId(snapshotId);
    return this;
  }

  @Override
  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    removeSnapshots.expireOlderThan(timestampMillis);
    return this;
  }

  @Override
  public ExpireSnapshotsAction retainLast(int numSnapshots) {
    removeSnapshots.retainLast(numSnapshots);
    return this;
  }

  @Override
  public ExpireSnapshotsAction deleteWith(Consumer<String> deleteFunc) {
    removeSnapshots.deleteWith(deleteFunc);
    return this;
  }

  @Override
  public ExpireSnapshotsAction executeDeleteWith(ExecutorService executorService) {
    removeSnapshots.executeDeleteWith(executorService);
    return this;
  }

  @Override
  public ExpireSnapshotsAction cleanExpiredMetadata(boolean clean) {
    removeSnapshots.cleanExpiredMetadata(clean);
    return this;
  }

  /**
   * Passes an executor service that reads manifests to find the files to delete.
   *
   * @param executorService the executor service to use for planning
   * @return this for method chaining
   */
  public ExpireSnapshotsAction planWith(ExecutorService executorService) {
    removeSnapshots.planWith(executorService);
    return this;
  }

  @Override
  public Result execute() {
    DeleteSummary summary = removeSnapshots.commitAndCleanUp();
    return ImmutableExpireSnapshots.Result.builder()
        .deletedDataFilesCount(summary.count(DeletedFileType.DATA))
        .deletedPositionDeleteFilesCount(summary.count(DeletedFileType.POSITION_DELETES))
        .deletedEqualityDeleteFilesCount(summary.count(DeletedFileType.EQUALITY_DELETES))
        .deletedManifestsCount(summary.count(DeletedFileType.MANIFEST))
        .deletedManifestListsCount(summary.count(DeletedFileType.MANIFEST_LIST))
        .deletedStatisticsFilesCount(summary.count(DeletedFileType.STATISTICS_FILES))
        .build();
  }
}
