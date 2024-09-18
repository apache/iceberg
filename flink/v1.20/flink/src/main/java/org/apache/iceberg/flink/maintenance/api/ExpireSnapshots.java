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
package org.apache.iceberg.flink.maintenance.api;

import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.flink.maintenance.operator.DeleteFilesProcessor;
import org.apache.iceberg.flink.maintenance.operator.ExpireSnapshotsProcessor;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Deletes expired snapshots and the corresponding files. */
public class ExpireSnapshots {
  private static final int DELETE_BATCH_SIZE_DEFAULT = 1000;
  private static final String EXECUTOR_OPERATOR_NAME = "Expire Snapshot";
  @VisibleForTesting static final String DELETE_FILES_OPERATOR_NAME = "Delete file";

  private ExpireSnapshots() {
    // Do not instantiate directly
  }

  /** Creates the builder for creating a stream which expires snapshots for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<ExpireSnapshots.Builder> {
    private Duration maxSnapshotAge = null;
    private Integer numSnapshots = null;
    private int planningWorkerPoolSize = SystemConfigs.WORKER_THREAD_POOL_SIZE.value();
    private int deleteBatchSize = DELETE_BATCH_SIZE_DEFAULT;
    private int deleteParallelism = 1;

    /**
     * The snapshots older than this age will be removed.
     *
     * @param newMaxSnapshotAge of the snapshots to be removed
     */
    public Builder maxSnapshotAge(Duration newMaxSnapshotAge) {
      this.maxSnapshotAge = newMaxSnapshotAge;
      return this;
    }

    /**
     * The minimum number of {@link Snapshot}s to retain. For more details description see {@link
     * org.apache.iceberg.ExpireSnapshots#retainLast(int)}.
     *
     * @param newNumSnapshots number of snapshots to retain
     */
    public Builder retainLast(int newNumSnapshots) {
      this.numSnapshots = newNumSnapshots;
      return this;
    }

    /**
     * The worker pool size used to calculate the files to delete.
     *
     * @param newPlanningWorkerPoolSize for planning files to delete
     */
    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * Size of the batch used to deleting the files.
     *
     * @param newDeleteBatchSize used for deleting
     */
    public Builder deleteBatchSize(int newDeleteBatchSize) {
      this.deleteBatchSize = newDeleteBatchSize;
      return this;
    }

    /**
     * The number of subtasks which are doing the deletes.
     *
     * @param newDeleteParallelism used for deleting
     */
    public Builder deleteParallelism(int newDeleteParallelism) {
      this.deleteParallelism = newDeleteParallelism;
      return this;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      Preconditions.checkNotNull(tableLoader(), "TableLoader should not be null");

      SingleOutputStreamOperator<TaskResult> result =
          trigger
              .process(
                  new ExpireSnapshotsProcessor(
                      tableLoader(),
                      maxSnapshotAge == null ? null : maxSnapshotAge.toMillis(),
                      numSnapshots,
                      planningWorkerPoolSize))
              .name(EXECUTOR_OPERATOR_NAME)
              .uid(EXECUTOR_OPERATOR_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      result
          .getSideOutput(ExpireSnapshotsProcessor.DELETE_STREAM)
          .rebalance()
          .transform(
              DELETE_FILES_OPERATOR_NAME,
              TypeInformation.of(Void.class),
              new DeleteFilesProcessor(name(), tableLoader(), deleteBatchSize))
          .name(DELETE_FILES_OPERATOR_NAME)
          .uid(DELETE_FILES_OPERATOR_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(deleteParallelism);

      // Ignore the file deletion result and return the DataStream<TaskResult> directly
      return result;
    }
  }
}
