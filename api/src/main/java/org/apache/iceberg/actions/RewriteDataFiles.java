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

import java.util.List;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

/**
 * An action for rewriting data files according to a rewrite strategy. Generally used for optimizing
 * the sizing and layout of data files within a table.
 */
@Value.Enclosing
public interface RewriteDataFiles
    extends SnapshotUpdate<RewriteDataFiles, RewriteDataFiles.Result> {

  /**
   * Enable committing groups of files (see max-file-group-size-bytes) prior to the entire rewrite
   * completing. This will produce additional commits but allow for progress even if some groups
   * fail to commit. This setting will not change the correctness of the rewrite operation as file
   * groups can be compacted independently.
   *
   * <p>The default is false, which produces a single commit when the entire job has completed.
   */
  String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";

  boolean PARTIAL_PROGRESS_ENABLED_DEFAULT = false;

  /**
   * The maximum amount of Iceberg commits that this rewrite is allowed to produce if partial
   * progress is enabled. This setting has no effect if partial progress is disabled.
   */
  String PARTIAL_PROGRESS_MAX_COMMITS = "partial-progress.max-commits";

  int PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT = 10;

  /**
   * The entire rewrite operation is broken down into pieces based on partitioning and within
   * partitions based on size into groups. These sub-units of the rewrite are referred to as file
   * groups. The largest amount of data that should be compacted in a single group is controlled by
   * {@link #MAX_FILE_GROUP_SIZE_BYTES}. This helps with breaking down the rewriting of very large
   * partitions which may not be rewritable otherwise due to the resource constraints of the
   * cluster. For example a sort based rewrite may not scale to terabyte sized partitions, those
   * partitions need to be worked on in small subsections to avoid exhaustion of resources.
   *
   * <p>When grouping files, the underlying rewrite strategy will use this value as to limit the
   * files which will be included in a single file group. A group will be processed by a single
   * framework "action". For example, in Spark this means that each group would be rewritten in its
   * own Spark action. A group will never contain files for multiple output partitions.
   */
  String MAX_FILE_GROUP_SIZE_BYTES = "max-file-group-size-bytes";

  long MAX_FILE_GROUP_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L; // 100 Gigabytes

  /**
   * The max number of file groups to be simultaneously rewritten by the rewrite strategy. The
   * structure and contents of the group is determined by the rewrite strategy. Each file group will
   * be rewritten independently and asynchronously.
   */
  String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max-concurrent-file-group-rewrites";

  int MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT = 5;

  /**
   * The output file size that this rewrite strategy will attempt to generate when rewriting files.
   * By default this will use the "write.target-file-size-bytes value" in the table properties of
   * the table being updated.
   */
  String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  /**
   * If the compaction should use the sequence number of the snapshot at compaction start time for
   * new data files, instead of using the sequence number of the newly produced snapshot.
   *
   * <p>This avoids commit conflicts with updates that add newer equality deletes at a higher
   * sequence number.
   *
   * <p>Defaults to true.
   */
  String USE_STARTING_SEQUENCE_NUMBER = "use-starting-sequence-number";

  boolean USE_STARTING_SEQUENCE_NUMBER_DEFAULT = true;

  /**
   * Forces the rewrite job order based on the value.
   *
   * <p>
   *
   * <ul>
   *   <li>If rewrite-job-order=bytes-asc, then rewrite the smallest job groups first.
   *   <li>If rewrite-job-order=bytes-desc, then rewrite the largest job groups first.
   *   <li>If rewrite-job-order=files-asc, then rewrite the job groups with the least files first.
   *   <li>If rewrite-job-order=files-desc, then rewrite the job groups with the most files first.
   *   <li>If rewrite-job-order=none, then rewrite job groups in the order they were planned (no
   *       specific ordering).
   * </ul>
   *
   * <p>Defaults to none.
   */
  String REWRITE_JOB_ORDER = "rewrite-job-order";

  String REWRITE_JOB_ORDER_DEFAULT = RewriteJobOrder.NONE.orderName();

  /**
   * Choose BINPACK as a strategy for this rewrite operation
   *
   * @return this for method chaining
   */
  default RewriteDataFiles binPack() {
    return this;
  }

  /**
   * Choose SORT as a strategy for this rewrite operation using the table's sortOrder
   *
   * @return this for method chaining
   */
  default RewriteDataFiles sort() {
    throw new UnsupportedOperationException(
        "SORT Rewrite Strategy not implemented for this framework");
  }

  /**
   * Choose SORT as a strategy for this rewrite operation and manually specify the sortOrder to use
   *
   * @param sortOrder user defined sortOrder
   * @return this for method chaining
   */
  default RewriteDataFiles sort(SortOrder sortOrder) {
    throw new UnsupportedOperationException(
        "SORT Rewrite Strategy not implemented for this framework");
  }

  /**
   * Choose Z-ORDER as a strategy for this rewrite operation with a specified list of columns to use
   *
   * @param columns Columns to be used to generate Z-Values
   * @return this for method chaining
   */
  default RewriteDataFiles zOrder(String... columns) {
    throw new UnsupportedOperationException(
        "Z-ORDER Rewrite Strategy not implemented for this framework");
  }

  /**
   * A user provided filter for determining which files will be considered by the rewrite strategy.
   * This will be used in addition to whatever rules the rewrite strategy generates. For example
   * this would be used for providing a restriction to only run rewrite on a specific partition.
   *
   * @param expression An iceberg expression used to determine which files will be considered for
   *     rewriting
   * @return this for chaining
   */
  RewriteDataFiles filter(Expression expression);

  /**
   * A map of file group information to the results of rewriting that file group. If the results are
   * null then that particular file group failed. We should only have failed groups if partial
   * progress is enabled otherwise we will report a total failure for the job.
   */
  @Value.Immutable
  interface Result {
    List<FileGroupRewriteResult> rewriteResults();

    @Value.Default
    default List<FileGroupFailureResult> rewriteFailures() {
      return ImmutableList.of();
    }

    @Value.Default
    default int addedDataFilesCount() {
      return rewriteResults().stream().mapToInt(FileGroupRewriteResult::addedDataFilesCount).sum();
    }

    @Value.Default
    default int rewrittenDataFilesCount() {
      return rewriteResults().stream()
          .mapToInt(FileGroupRewriteResult::rewrittenDataFilesCount)
          .sum();
    }

    @Value.Default
    default long rewrittenBytesCount() {
      return rewriteResults().stream().mapToLong(FileGroupRewriteResult::rewrittenBytesCount).sum();
    }

    @Value.Default
    default int failedDataFilesCount() {
      return rewriteFailures().stream().mapToInt(FileGroupFailureResult::dataFilesCount).sum();
    }
  }

  /**
   * For a particular file group, the number of files which are newly created and the number of
   * files which were formerly part of the table but have been rewritten.
   */
  @Value.Immutable
  interface FileGroupRewriteResult {
    FileGroupInfo info();

    int addedDataFilesCount();

    int rewrittenDataFilesCount();

    @Value.Default
    default long rewrittenBytesCount() {
      return 0L;
    }
  }

  /** For a file group that failed to rewrite. */
  @Value.Immutable
  interface FileGroupFailureResult {
    FileGroupInfo info();

    int dataFilesCount();
  }

  /**
   * A description of a file group, when it was processed, and within which partition. For use
   * tracking rewrite operations and for returning results.
   */
  @Value.Immutable
  interface FileGroupInfo {

    /** returns which file group this is out of the total set of file groups for this rewrite */
    int globalIndex();

    /** returns which file group this is out of the set of file groups for this partition */
    int partitionIndex();

    /** returns which partition this file group contains files from */
    StructLike partition();
  }
}
