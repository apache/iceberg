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
package org.apache.iceberg.flink.maintenance.stream;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.iceberg.flink.maintenance.operator.AsyncDeleteFiles;
import org.apache.iceberg.flink.maintenance.operator.ExpireSnapshotsProcessor;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class ExpireSnapshots {
  private static final long DELETE_INITIAL_DELAY_MS = 10L;
  private static final long DELETE_MAX_RETRY_DELAY_MS = 1000L;
  private static final double DELETE_BACKOFF_MULTIPLIER = 1.5;
  private static final long DELETE_TIMEOUT_MS = 10000L;
  private static final int DELETE_PLANNING_WORKER_POOL_SIZE_DEFAULT = 10;
  private static final int DELETE_ATTEMPT_NUM = 10;
  private static final int DELETE_WORKER_POOL_SIZE_DEFAULT = 10;
  private static final String EXECUTOR_TASK_NAME = "ES Executor";
  @VisibleForTesting static final String DELETE_FILES_TASK_NAME = "Delete file";

  private ExpireSnapshots() {
    // Do not instantiate directly
  }

  /** Creates the builder for creating a stream which expires snapshots for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<ExpireSnapshots.Builder> {
    private Duration minAge = null;
    private Integer retainLast = null;
    private int planningWorkerPoolSize = DELETE_PLANNING_WORKER_POOL_SIZE_DEFAULT;
    private int deleteAttemptNum = DELETE_ATTEMPT_NUM;
    private int deleteWorkerPoolSize = DELETE_WORKER_POOL_SIZE_DEFAULT;

    /**
     * The snapshots newer than this age will not be removed.
     *
     * @param newMinAge of the files to be removed
     * @return for chained calls
     */
    public Builder minAge(Duration newMinAge) {
      this.minAge = newMinAge;
      return this;
    }

    /**
     * The minimum {@link org.apache.iceberg.Snapshot}s to retain. For more details description see
     * {@link org.apache.iceberg.ExpireSnapshots#retainLast(int)}.
     *
     * @param newRetainLast number of snapshots to retain
     * @return for chained calls
     */
    public Builder retainLast(int newRetainLast) {
      this.retainLast = newRetainLast;
      return this;
    }

    /**
     * The worker pool size used to calculate the files to delete.
     *
     * @param newPlanningWorkerPoolSize for planning files to delete
     * @return for chained calls
     */
    public Builder planningWorkerPoolSize(int newPlanningWorkerPoolSize) {
      this.planningWorkerPoolSize = newPlanningWorkerPoolSize;
      return this;
    }

    /**
     * The number of retries on the failed delete attempts.
     *
     * @param newDeleteAttemptNum number of retries
     * @return for chained calls
     */
    public Builder deleteAttemptNum(int newDeleteAttemptNum) {
      this.deleteAttemptNum = newDeleteAttemptNum;
      return this;
    }

    /**
     * The worker pool size used for deleting files.
     *
     * @param newDeleteWorkerPoolSize for scanning
     * @return for chained calls
     */
    public Builder deleteWorkerPoolSize(int newDeleteWorkerPoolSize) {
      this.deleteWorkerPoolSize = newDeleteWorkerPoolSize;
      return this;
    }

    @Override
    DataStream<TaskResult> buildInternal(DataStream<Trigger> trigger) {
      Preconditions.checkNotNull(tableLoader(), "TableLoader should not be null");

      SingleOutputStreamOperator<TaskResult> result =
          trigger
              .process(
                  new ExpireSnapshotsProcessor(
                      tableLoader(),
                      minAge == null ? null : minAge.toMillis(),
                      retainLast,
                      planningWorkerPoolSize))
              .name(EXECUTOR_TASK_NAME)
              .uid(uidPrefix() + "-expire-snapshots")
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      AsyncRetryStrategy<Boolean> retryStrategy =
          new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Boolean>(
                  deleteAttemptNum,
                  DELETE_INITIAL_DELAY_MS,
                  DELETE_MAX_RETRY_DELAY_MS,
                  DELETE_BACKOFF_MULTIPLIER)
              .ifResult(AsyncDeleteFiles.FAILED_PREDICATE)
              .build();

      AsyncDataStream.unorderedWaitWithRetry(
              result.getSideOutput(ExpireSnapshotsProcessor.DELETE_STREAM).rebalance(),
              new AsyncDeleteFiles(name(), tableLoader(), deleteWorkerPoolSize),
              DELETE_TIMEOUT_MS,
              TimeUnit.MILLISECONDS,
              deleteWorkerPoolSize,
              retryStrategy)
          .name(DELETE_FILES_TASK_NAME)
          .uid(uidPrefix() + "-delete-expired-files")
          .slotSharingGroup(slotSharingGroup())
          .setParallelism(parallelism());

      // Deleting the files is asynchronous, so we ignore the results when calculating the return
      // value
      return result;
    }
  }
}
