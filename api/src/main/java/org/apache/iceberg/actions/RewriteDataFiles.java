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

package org.apache.iceberg.actions;

import java.util.Map;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;

/**
 * An action for rewriting datafiles according to a Compaction Strategy. Generally used for
 * optimizing the sizing and layout of datafiles within a table.
 */
public interface RewriteDataFiles extends Action<RewriteDataFiles, RewriteDataFiles.Result> {

  /**
   * Enable committing groups of files (see max-file-group-size) prior to the entire compaction completing. This will produce additional commits
   * but allow for progress even if some groups fail to commit. The default is false, which produces a single commit
   * when the entire job has completed.
   */
  String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";
  boolean PARTIAL_PROGRESS_ENABLED_DEFAULT = false;

  /**
   * The maximum amount of Iceberg commits that compaction is allowed to produce if partial progress is enabled.
   */
  String PARTIAL_PROGRESS_MAX_COMMITS = "partial-progress.max-commits";
  int PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT = 10;

  /**
   * The entire compaction operation is broken down into pieces based on partitioning and within partitions based
   * on size into groups. These sub-units of compaction are referred to as file groups. The largest amount of data that
   * should be compacted in a single group is controlled by MAX_FILE_GROUP_SIZE_BYTES. When grouping files, the
   * underlying compaction strategy will use this value as to limit the files which will be included in a single file
   * group. A group will be processed by a single framework "action". For example, in Spark this means that each group
   * would be rewritten in its own Spark action. A group will never contain files for multiple output partitions.
   */
  String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";
  long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L; // 100 Gigabytes

  /**
   * The max number of file groups to be simultaneously rewritten by the compaction strategy. The structure and
   * contents of the group is determined by the compaction strategy. Each file group will be rewritten
   * independently and asynchronously.
   **/
  String MAX_CONCURRENT_FILE_GROUP_ACTIONS = "max-concurrent-file-group-actions";
  int MAX_CONCURRENT_FILE_GROUP_ACTIONS_DEFAULT = 1;

  /**
   * The output file size that this compaction strategy will attempt to generate when rewriting files. By default this
   * will use the write.target-size value in the table properties of the table being updated.
   */
  String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  /**
   * The partition spec to use when writing the output data from this operation. By default uses the
   * current table partition spec.
   */
  String PARTITION_SPEC_ID = "partition-spec-id";

  enum CompactionStrategyName {
    BinPack,
    Sort
  }

  /**
   * The name of the compaction strategy to be used when compacting data files. Currently we only support BINPACK and
   * SORT as options.
   *
   * @param strategyName name of the strategy
   * @return this for method chaining
   */
  RewriteDataFiles strategy(CompactionStrategyName strategyName);

  /**
   * A user provided filter for determining which files will be considered by the compaction strategy. This will be used
   * in addition to whatever rules the compaction strategy generates. For example this would be used for providing a
   * restriction to only run compaction on a specific partition.
   *
   * @param expression only entries that pass this filter will be compacted
   * @return this for chaining
   */
  RewriteDataFiles filter(Expression expression);

  /**
   * A pairing of file group information to the result of the rewriting that file group. If the results are null then
   * that particular chunk failed. We should only have failed groups if partial progress is enabled otherwise we will
   * report a total failure for the job.
   */
  interface Result {
    Map<FileGroupInfo, FileGroupResult> resultMap();
  }

  interface FileGroupResult {
    int addedDataFilesCount();

    int rewrittenDataFilesCount();
  }

  /**
   * A description of a file group, when it was processed, and within which partition. For use
   * tracking rewrite operations and for returning results.
   */
  interface FileGroupInfo {

    /**
     * returns which chunk this is out of the total set of chunks for this compaction
     */
    int globalIndex();

    /**
     * returns which chunk this is out of the set of chunks for this partition
     */
    int partitionIndex();

    /**
     * returns which partition this chunk contains files from
     */
    StructLike partition();
  }
}
