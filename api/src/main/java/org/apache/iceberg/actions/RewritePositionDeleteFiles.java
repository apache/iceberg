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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.immutables.value.Value;

/**
 * An action for rewriting position delete files.
 *
 * <p>Generally used for optimizing the size and layout of position delete files within a table.
 */
@Value.Enclosing
public interface RewritePositionDeleteFiles
    extends SnapshotUpdate<RewritePositionDeleteFiles, RewritePositionDeleteFiles.Result> {

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
   * The max number of file groups to be simultaneously rewritten by the rewrite strategy. The
   * structure and contents of the group is determined by the rewrite strategy. Each file group will
   * be rewritten independently and asynchronously.
   */
  String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max-concurrent-file-group-rewrites";

  int MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT = 5;

  /**
   * Forces the rewrite job order based on the value.
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
   * A filter for finding deletes to rewrite.
   *
   * <p>The filter will be converted to a partition filter with an inclusive projection. Any file
   * that may contain rows matching this filter will be used by the action. The matching delete
   * files will be rewritten.
   *
   * @param expression An iceberg expression used to find deletes.
   * @return this for method chaining
   */
  RewritePositionDeleteFiles filter(Expression expression);

  /** The action result that contains a summary of the execution. */
  @Value.Immutable
  interface Result {
    List<FileGroupRewriteResult> rewriteResults();

    /** Returns the count of the position delete files that have been rewritten. */
    default int rewrittenDeleteFilesCount() {
      return rewriteResults().stream()
          .mapToInt(FileGroupRewriteResult::rewrittenDeleteFilesCount)
          .sum();
    }

    /** Returns the count of the added position delete files. */
    default int addedDeleteFilesCount() {
      return rewriteResults().stream()
          .mapToInt(FileGroupRewriteResult::addedDeleteFilesCount)
          .sum();
    }

    /** Returns the number of bytes of position delete files that have been rewritten */
    default long rewrittenBytesCount() {
      return rewriteResults().stream().mapToLong(FileGroupRewriteResult::rewrittenBytesCount).sum();
    }

    /** Returns the number of bytes of newly added position delete files */
    default long addedBytesCount() {
      return rewriteResults().stream().mapToLong(FileGroupRewriteResult::addedBytesCount).sum();
    }
  }

  /**
   * For a particular position delete file group, the number of position delete files which are
   * newly created and the number of files which were formerly part of the table but have been
   * rewritten.
   */
  @Value.Immutable
  interface FileGroupRewriteResult {
    /** Description of this position delete file group. */
    FileGroupInfo info();

    /** Returns the count of the position delete files that been rewritten in this group. */
    int rewrittenDeleteFilesCount();

    /** Returns the count of the added position delete files in this group. */
    int addedDeleteFilesCount();

    /** Returns the number of bytes of rewritten position delete files in this group. */
    long rewrittenBytesCount();

    /** Returns the number of bytes of newly added position delete files in this group. */
    long addedBytesCount();
  }

  /**
   * A description of a position delete file group, when it was processed, and within which
   * partition. For use tracking rewrite operations and for returning results.
   */
  @Value.Immutable
  interface FileGroupInfo {
    /**
     * Returns which position delete file group this is out of the total set of file groups for this
     * rewrite
     */
    int globalIndex();

    /**
     * Returns which position delete file group this is out of the set of file groups for this
     * partition
     */
    int partitionIndex();

    /**
     * Returns which partition this position delete file group contains files from. This will be of
     * the type of the table's unified partition type considering all specs in a table.
     */
    StructLike partition();
  }
}
