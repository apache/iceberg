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
package org.apache.iceberg.flink.actions;

import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;

public class ExpireSnapshotsAction
    extends BaseTableMaintenanceAction<ExpireSnapshotsAction, ExpireSnapshots.Builder> {

  public ExpireSnapshotsAction(
      StreamExecutionEnvironment env, TableLoader tableLoader, long triggerTimestamp) {
    super(env, tableLoader, triggerTimestamp, ExpireSnapshots::builder);
  }

  public ExpireSnapshotsAction(StreamExecutionEnvironment env, TableLoader tableLoader) {
    super(env, tableLoader, ExpireSnapshots::builder);
  }

  /**
   * The snapshots older than this age will be removed.
   *
   * @param newMaxSnapshotAge of the snapshots to be removed
   */
  public ExpireSnapshotsAction maxSnapshotAge(Duration newMaxSnapshotAge) {
    builder().maxSnapshotAge(newMaxSnapshotAge);
    return this;
  }

  /**
   * The minimum number of {@link Snapshot}s to retain. For more details description see {@link
   * org.apache.iceberg.ExpireSnapshots#retainLast(int)}.
   *
   * @param newNumSnapshots number of snapshots to retain
   */
  public ExpireSnapshotsAction retainLast(int newNumSnapshots) {
    builder().retainLast(newNumSnapshots);
    return this;
  }

  /**
   * The worker pool size used to calculate the files to delete. If not set, the shared worker pool
   * is used.
   *
   * @param newPlanningWorkerPoolSize for planning files to delete
   */
  public ExpireSnapshotsAction planningWorkerPoolSize(Integer newPlanningWorkerPoolSize) {
    builder().planningWorkerPoolSize(newPlanningWorkerPoolSize);
    return this;
  }

  /**
   * Size of the batch used to deleting the files.
   *
   * @param newDeleteBatchSize used for deleting
   */
  public ExpireSnapshotsAction deleteBatchSize(int newDeleteBatchSize) {
    builder().deleteBatchSize(newDeleteBatchSize);
    return this;
  }
}
