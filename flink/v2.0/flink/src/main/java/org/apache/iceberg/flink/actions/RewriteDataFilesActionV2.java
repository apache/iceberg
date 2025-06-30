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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;

public class RewriteDataFilesActionV2
    extends BaseTableMaintenanceAction<RewriteDataFilesActionV2, RewriteDataFiles.Builder> {

  public RewriteDataFilesActionV2(
      StreamExecutionEnvironment env, TableLoader tableLoader, long triggerTimestamp) {
    super(env, tableLoader, triggerTimestamp, RewriteDataFiles::builder);
  }

  public RewriteDataFilesActionV2(StreamExecutionEnvironment env, TableLoader tableLoader) {
    super(env, tableLoader, RewriteDataFiles::builder);
  }

  /**
   * Allows committing compacted data files in batches. See {@link
   * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_ENABLED} for more details.
   *
   * @param newPartialProgressEnabled to enable partial commits
   */
  public RewriteDataFilesActionV2 partialProgressEnabled(boolean newPartialProgressEnabled) {
    builder().partialProgressEnabled(newPartialProgressEnabled);
    return this;
  }

  /**
   * Configures the size of batches if {@link #partialProgressEnabled}. See {@link
   * org.apache.iceberg.actions.RewriteDataFiles#PARTIAL_PROGRESS_MAX_COMMITS} for more details.
   *
   * @param newPartialProgressMaxCommits to target number of the commits per run
   */
  public RewriteDataFilesActionV2 partialProgressMaxCommits(int newPartialProgressMaxCommits) {
    builder().partialProgressMaxCommits(newPartialProgressMaxCommits);
    return this;
  }

  /**
   * Configures the maximum byte size of the rewrites for one scheduled compaction. This could be
   * used to limit the resources used by the compaction.
   *
   * @param newMaxRewriteBytes to limit the size of the rewrites
   */
  public RewriteDataFilesActionV2 maxRewriteBytes(long newMaxRewriteBytes) {
    builder().maxRewriteBytes(newMaxRewriteBytes);
    return this;
  }

  /**
   * Configures the target file size. See {@link
   * org.apache.iceberg.actions.RewriteDataFiles#TARGET_FILE_SIZE_BYTES} for more details.
   *
   * @param targetFileSizeBytes target file size
   */
  public RewriteDataFilesActionV2 targetFileSizeBytes(long targetFileSizeBytes) {
    builder().targetFileSizeBytes(targetFileSizeBytes);
    return this;
  }

  /**
   * Configures the min file size considered for rewriting. See {@link
   * SizeBasedFileRewritePlanner#MIN_FILE_SIZE_BYTES} for more details.
   *
   * @param minFileSizeBytes min file size
   */
  public RewriteDataFilesActionV2 minFileSizeBytes(long minFileSizeBytes) {
    builder().minFileSizeBytes(minFileSizeBytes);
    return this;
  }

  /**
   * Configures the max file size considered for rewriting. See {@link
   * SizeBasedFileRewritePlanner#MAX_FILE_SIZE_BYTES} for more details.
   *
   * @param maxFileSizeBytes max file size
   */
  public RewriteDataFilesActionV2 maxFileSizeBytes(long maxFileSizeBytes) {
    builder().maxFileSizeBytes(maxFileSizeBytes);
    return this;
  }

  /**
   * Configures the minimum file number after a rewrite is always initiated. See description see
   * {@link SizeBasedFileRewritePlanner#MIN_INPUT_FILES} for more details.
   *
   * @param minInputFiles min file number
   */
  public RewriteDataFilesActionV2 minInputFiles(int minInputFiles) {
    builder().minInputFiles(minInputFiles);
    return this;
  }

  /**
   * Configures the minimum delete file number for a file after a rewrite is always initiated. See
   * {@link BinPackRewriteFilePlanner#DELETE_FILE_THRESHOLD} for more details.
   *
   * @param deleteFileThreshold min delete file number
   */
  public RewriteDataFilesActionV2 deleteFileThreshold(int deleteFileThreshold) {
    builder().deleteFileThreshold(deleteFileThreshold);
    return this;
  }

  /**
   * Overrides other options and forces rewriting of all provided files.
   *
   * @param rewriteAll enables a full rewrite
   */
  public RewriteDataFilesActionV2 rewriteAll(boolean rewriteAll) {
    builder().rewriteAll(rewriteAll);
    return this;
  }

  /**
   * Configures the group size for rewriting. See {@link
   * SizeBasedFileRewritePlanner#MAX_FILE_GROUP_SIZE_BYTES} for more details.
   *
   * @param maxFileGroupSizeBytes file group size for rewrite
   */
  public RewriteDataFilesActionV2 maxFileGroupSizeBytes(long maxFileGroupSizeBytes) {
    builder().maxFileGroupSizeBytes(maxFileGroupSizeBytes);
    return this;
  }

  /**
   * Configures max files to rewrite. See {@link BinPackRewriteFilePlanner#MAX_FILES_TO_REWRITE} for
   * more details.
   *
   * @param maxFilesToRewrite maximum files to rewrite
   */
  public RewriteDataFilesActionV2 maxFilesToRewrite(int maxFilesToRewrite) {
    builder().maxFilesToRewrite(maxFilesToRewrite);
    return this;
  }

  /**
   * A user provided filter for determining which files will be considered by the rewrite strategy.
   *
   * @param newFilter the filter expression to apply
   * @return this for method chaining
   */
  public RewriteDataFilesActionV2 filter(Expression newFilter) {
    builder().filter(newFilter);
    return this;
  }
}
