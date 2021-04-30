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

public interface CompactDataFiles extends Action<CompactDataFiles, CompactDataFiles.Result> {

  /**
   * Enable committing groups of chunks prior see max-chunk-size to the entire compaction completing. This will produce additional commits
   * but allow for progress even if some chunks fail to commit. The default is false, which produces a single commit
   * when the entire job has completed.
   */
  String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";
  boolean PARTIAL_PROGRESS_ENABLED_DEFAULT = false;

  /**
   * The maximum amount of commits that compaction is allowed to produce if partial progress is enabled.
   */
  String PARTIAL_PROGRESS_MAX_COMMITS = "partial-progress.max-commits";
  int PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT = 10;

  // TODO be set once we have an ENUM in core
  // String COMPACTION_STRATEGY_DEFAULT;

  /**
   * The entire compaction operation is broken down into pieces based on partitioning and within partitions based
   * on size. These sub-units of compaction are referred to as chunks. The largest amount of data that should be
   * compacted in a single chunk is controlled by MAX_CHUNK_SIZE_BYTES. When grouping files, the underlying
   * compaction strategy will use this value to but an upper bound on the number of files included in a single
   * chunk. A chunk will be processed by a single framework "job". For example, in Spark this means that each chunk
   * would be processed in it's own Spark action. A chunk will never contain files for multiple output partitions.
   */
  String MAX_CHUNK_SIZE_BYTES = "max-chunk-size-bytes";
  long MAX_CHUNK_SIZE_BYTES_DEFAULT = 1024L * 1024L * 1024L * 100L; // 100 Gigabytes

  /**
   * The file size that this compaction strategy will attempt to generate when rewriting files. By default this
   * will use the write.target-size value in the table properties of the table being updated.
   */
  String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  /**
   * The max number of chunks to be simultaneously rewritten by the compaction strategy. The structure and
   * contents of the chunk is determined by the compaction strategy. When running each job chunk will be run
   * independently and asynchronously.
   **/
  String MAX_CONCURRENT_CHUNKS = "max-concurrent-chunks";
  int MAX_CONCURRENT_CHUNKS_DEFAULT = 1;

  /**
   * The name of the compaction strategy to be used when compacting data files. Currently we only support BINPACK and
   * SORT as options.
   *
   * @param strategyName name of the strategy
   * @return this for method chaining
   */
  CompactDataFiles strategy(String strategyName);

  /**
   * A user provided filter for determining which files will be considered by the compaction strategy. This will be used
   * in addition to whatever rules the compaction strategy generates. For example this would be used for providing a
   * restriction to only run compaction on a specific partition.
   *
   * @param expression only entries that pass this filter will be compacted
   * @return this for chaining
   */
  CompactDataFiles filter(Expression expression);

  /**
   * A pairing of Chunk information to the result of that chunk's results. If the results are null then that particular
   * chunk failed. We should only have failed chunks if partial progress is enabled.
   */
  interface Result {
    Map<ChunkInfo, ChunkResult> resultMap();
  }

  interface ChunkResult {
    int addedDataFilesCount();

    int rewrittenDataFilesCount();
  }

  /**
   * A description of a Chunk, when it was processed, and within which partition.
   */
  interface ChunkInfo {

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
