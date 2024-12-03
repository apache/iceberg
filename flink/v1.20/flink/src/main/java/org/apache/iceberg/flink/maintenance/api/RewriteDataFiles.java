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

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteCommitter;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteExecutor;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner;
import org.apache.iceberg.flink.maintenance.operator.TaskResultAggregator;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Creates the data file rewriter data stream. Which runs a single iteration of the task for every
 * {@link Trigger} event.
 */
public class RewriteDataFiles {
  static final String PLANNER_TASK_NAME = "RDF Planner";
  static final String REWRITE_TASK_NAME = "Rewrite";
  static final String COMMIT_TASK_NAME = "Rewrite commit";
  static final String AGGREGATOR_TASK_NAME = "Rewrite aggregator";

  private RewriteDataFiles() {}

  /** Creates the builder for a stream which rewrites data files for the table. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<RewriteDataFiles.Builder> {
    private boolean partialProgressEnabled = false;
    private int partialProgressMaxCommits = 10;
    private final Map<String, String> rewriteOptions = Maps.newHashMapWithExpectedSize(6);
    private long maxRewriteBytes = Long.MAX_VALUE;

    /**
     * Allows committing compacted data files in batches. For more details description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_ENABLED}.
     *
     * @param newPartialProgressEnabled to enable partial commits
     */
    public Builder partialProgressEnabled(boolean newPartialProgressEnabled) {
      this.partialProgressEnabled = newPartialProgressEnabled;
      return this;
    }

    /**
     * Configures the size of batches if {@link #partialProgressEnabled}. For more details
     * description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_MAX_COMMITS}.
     *
     * @param newPartialProgressMaxCommits to target number of the commits per run
     */
    public Builder partialProgressMaxCommits(int newPartialProgressMaxCommits) {
      this.partialProgressMaxCommits = newPartialProgressMaxCommits;
      return this;
    }

    /**
     * Configures the maximum byte size of the rewrites for one scheduled compaction. This could be
     * used to limit the resources used by the compaction.
     *
     * @param newMaxRewriteBytes to limit the size of the rewrites
     */
    public Builder maxRewriteBytes(long newMaxRewriteBytes) {
      this.maxRewriteBytes = newMaxRewriteBytes;
      return this;
    }

    /**
     * Configures the target file size. For more details description see {@link
     * org.apache.iceberg.actions.RewriteDataFiles#TARGET_FILE_SIZE_BYTES}.
     *
     * @param targetFileSizeBytes target file size
     */
    public Builder targetFileSizeBytes(long targetFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSizeBytes));
      return this;
    }

    /**
     * Configures the min file size considered for rewriting. For more details description see
     * {@link SizeBasedFileRewriter#MIN_FILE_SIZE_BYTES}.
     *
     * @param minFileSizeBytes min file size
     */
    public Builder minFileSizeBytes(long minFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSizeBytes));
      return this;
    }

    /**
     * Configures the max file size considered for rewriting. For more details description see
     * {@link SizeBasedFileRewriter#MAX_FILE_SIZE_BYTES}.
     *
     * @param maxFileSizeBytes max file size
     */
    public Builder maxFileSizeBytes(long maxFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSizeBytes));
      return this;
    }

    /**
     * Configures the minimum file number after a rewrite is always initiated. For more details
     * description see {@link SizeBasedFileRewriter#MIN_INPUT_FILES}.
     *
     * @param minInputFiles min file number
     */
    public Builder minInputFiles(int minInputFiles) {
      this.rewriteOptions.put(SizeBasedFileRewriter.MIN_INPUT_FILES, String.valueOf(minInputFiles));
      return this;
    }

    /**
     * Configures the minimum delete file number for a file after a rewrite is always initiated. For
     * more details description see {@link SizeBasedDataRewriter#DELETE_FILE_THRESHOLD}.
     *
     * @param deleteFileThreshold min delete file number
     */
    public Builder deleteFileThreshold(int deleteFileThreshold) {
      this.rewriteOptions.put(
          SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, String.valueOf(deleteFileThreshold));
      return this;
    }

    /**
     * Every other option is overridden, and all the files are rewritten.
     *
     * @param rewriteAll enables a full rewrite
     */
    public Builder rewriteAll(boolean rewriteAll) {
      this.rewriteOptions.put(SizeBasedFileRewriter.REWRITE_ALL, String.valueOf(rewriteAll));
      return this;
    }

    /**
     * Configures the group size for rewriting. For more details description see {@link
     * SizeBasedDataRewriter#MAX_FILE_GROUP_SIZE_BYTES}.
     *
     * @param maxFileGroupSizeBytes file group size for rewrite
     */
    public Builder maxFileGroupSizeBytes(long maxFileGroupSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewriter.MAX_FILE_GROUP_SIZE_BYTES, String.valueOf(maxFileGroupSizeBytes));
      return this;
    }

    /**
     * The input is a {@link DataStream} with {@link Trigger} events and every event should be
     * immediately followed by a {@link org.apache.flink.streaming.api.watermark.Watermark} with the
     * same timestamp as the event.
     *
     * <p>The output is a {@link DataStream} with the {@link TaskResult} of the run followed by the
     * {@link org.apache.flink.streaming.api.watermark.Watermark}.
     */
    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      SingleOutputStreamOperator<DataFileRewritePlanner.PlannedGroup> planned =
          trigger
              .process(
                  new DataFileRewritePlanner(
                      tableName(),
                      taskName(),
                      index(),
                      tableLoader(),
                      partialProgressEnabled ? partialProgressMaxCommits : 1,
                      maxRewriteBytes,
                      rewriteOptions))
              .name(operatorName(PLANNER_TASK_NAME))
              .uid(PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<DataFileRewriteExecutor.ExecutedGroup> rewritten =
          planned
              .rebalance()
              .process(new DataFileRewriteExecutor(tableName(), taskName(), index()))
              .name(operatorName(REWRITE_TASK_NAME))
              .uid(REWRITE_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .setParallelism(parallelism());

      SingleOutputStreamOperator<Trigger> updated =
          rewritten
              .transform(
                  operatorName(COMMIT_TASK_NAME),
                  TypeInformation.of(Trigger.class),
                  new DataFileRewriteCommitter(tableName(), taskName(), index(), tableLoader()))
              .uid(COMMIT_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      return trigger
          .union(updated)
          .connect(
              planned
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .union(
                      rewritten.getSideOutput(TaskResultAggregator.ERROR_STREAM),
                      updated.getSideOutput(TaskResultAggregator.ERROR_STREAM)))
          .transform(
              operatorName(AGGREGATOR_TASK_NAME),
              TypeInformation.of(TaskResult.class),
              new TaskResultAggregator(tableName(), taskName(), index()))
          .uid(AGGREGATOR_TASK_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }
}
