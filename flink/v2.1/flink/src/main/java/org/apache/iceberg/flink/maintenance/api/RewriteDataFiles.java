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
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteCommitter;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner;
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
    private boolean partialProgressEnabled =
        org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT;
    private int partialProgressMaxCommits =
        org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT;
    private final Map<String, String> rewriteOptions = Maps.newHashMapWithExpectedSize(6);
    private long maxRewriteBytes = Long.MAX_VALUE;
    private Expression filter = Expressions.alwaysTrue();

    @Override
    String maintenanceTaskName() {
      return "RewriteDataFiles";
    }

    /**
     * Allows committing compacted data files in batches. See {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_ENABLED} for more details.
     *
     * @param newPartialProgressEnabled to enable partial commits
     */
    public Builder partialProgressEnabled(boolean newPartialProgressEnabled) {
      this.partialProgressEnabled = newPartialProgressEnabled;
      return this;
    }

    /**
     * Configures the size of batches if {@link #partialProgressEnabled}. See {@link
     * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_MAX_COMMITS} for more details.
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
     * Configures the target file size. See {@link
     * org.apache.iceberg.actions.RewriteDataFiles#TARGET_FILE_SIZE_BYTES} for more details.
     *
     * @param targetFileSizeBytes target file size
     */
    public Builder targetFileSizeBytes(long targetFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSizeBytes));
      return this;
    }

    /**
     * Configures the min file size considered for rewriting. See {@link
     * SizeBasedFileRewritePlanner#MIN_FILE_SIZE_BYTES} for more details.
     *
     * @param minFileSizeBytes min file size
     */
    public Builder minFileSizeBytes(long minFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSizeBytes));
      return this;
    }

    /**
     * Configures the max file size considered for rewriting. See {@link
     * SizeBasedFileRewritePlanner#MAX_FILE_SIZE_BYTES} for more details.
     *
     * @param maxFileSizeBytes max file size
     */
    public Builder maxFileSizeBytes(long maxFileSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSizeBytes));
      return this;
    }

    /**
     * Configures the minimum file number after a rewrite is always initiated. See description see
     * {@link SizeBasedFileRewritePlanner#MIN_INPUT_FILES} for more details.
     *
     * @param minInputFiles min file number
     */
    public Builder minInputFiles(int minInputFiles) {
      this.rewriteOptions.put(
          SizeBasedFileRewritePlanner.MIN_INPUT_FILES, String.valueOf(minInputFiles));
      return this;
    }

    /**
     * Configures the minimum delete file number for a file after a rewrite is always initiated. See
     * {@link BinPackRewriteFilePlanner#DELETE_FILE_THRESHOLD} for more details.
     *
     * @param deleteFileThreshold min delete file number
     */
    public Builder deleteFileThreshold(int deleteFileThreshold) {
      this.rewriteOptions.put(
          BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, String.valueOf(deleteFileThreshold));
      return this;
    }

    /**
     * Overrides other options and forces rewriting of all provided files.
     *
     * @param rewriteAll enables a full rewrite
     */
    public Builder rewriteAll(boolean rewriteAll) {
      this.rewriteOptions.put(SizeBasedFileRewritePlanner.REWRITE_ALL, String.valueOf(rewriteAll));
      return this;
    }

    /**
     * Configures the group size for rewriting. See {@link
     * SizeBasedFileRewritePlanner#MAX_FILE_GROUP_SIZE_BYTES} for more details.
     *
     * @param maxFileGroupSizeBytes file group size for rewrite
     */
    public Builder maxFileGroupSizeBytes(long maxFileGroupSizeBytes) {
      this.rewriteOptions.put(
          SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES,
          String.valueOf(maxFileGroupSizeBytes));
      return this;
    }

    /**
     * Configures max files to rewrite. See {@link BinPackRewriteFilePlanner#MAX_FILES_TO_REWRITE}
     * for more details.
     *
     * @param maxFilesToRewrite maximum files to rewrite
     */
    public Builder maxFilesToRewrite(int maxFilesToRewrite) {
      this.rewriteOptions.put(
          BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE, String.valueOf(maxFilesToRewrite));
      return this;
    }

    /**
     * A user provided filter for determining which files will be considered by the rewrite
     * strategy.
     *
     * @param newFilter the filter expression to apply
     * @return this for method chaining
     */
    public Builder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    /**
     * Configures the properties for the rewriter.
     *
     * @param rewriteDataFilesConfig properties for the rewriter
     */
    public Builder config(RewriteDataFilesConfig rewriteDataFilesConfig) {

      // Config about the rewriter
      this.partialProgressEnabled(rewriteDataFilesConfig.partialProgressEnable())
          .partialProgressMaxCommits(rewriteDataFilesConfig.partialProgressMaxCommits())
          .maxRewriteBytes(rewriteDataFilesConfig.maxRewriteBytes())
          // Config about the schedule
          .scheduleOnCommitCount(rewriteDataFilesConfig.scheduleOnCommitCount())
          .scheduleOnDataFileCount(rewriteDataFilesConfig.scheduleOnDataFileCount())
          .scheduleOnDataFileSize(rewriteDataFilesConfig.scheduleOnDataFileSize())
          .scheduleOnInterval(
              Duration.ofSeconds(rewriteDataFilesConfig.scheduleOnIntervalSecond()));

      // override the rewrite options
      this.rewriteOptions.putAll(rewriteDataFilesConfig.properties());

      return this;
    }

    /**
     * The input is a {@link DataStream} with {@link Trigger} events and every event should be
     * immediately followed by a {@link Watermark} with the same timestamp as the event.
     *
     * <p>The output is a {@link DataStream} with the {@link TaskResult} of the run followed by the
     * {@link Watermark}.
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
                      rewriteOptions,
                      filter))
              .name(operatorName(PLANNER_TASK_NAME))
              .uid(PLANNER_TASK_NAME + uidSuffix())
              .slotSharingGroup(slotSharingGroup())
              .forceNonParallel();

      SingleOutputStreamOperator<DataFileRewriteRunner.ExecutedGroup> rewritten =
          planned
              .rebalance()
              .process(new DataFileRewriteRunner(tableName(), taskName(), index()))
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
