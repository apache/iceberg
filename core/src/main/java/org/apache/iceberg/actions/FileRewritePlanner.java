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
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;

/**
 * A class for planning a rewrite operation.
 *
 * <p>A Rewrite Operation is performed by several classes working in conjunction.
 *
 * <ul>
 *   <li>{@link FileRewritePlanner} - (this class) which scans the files in the table and determines
 *       which files should be rewritten and how they should be grouped. The grouping is based on
 *       partitioning and the planning could create multiple groups within a partition. This
 *       produces a {@link FileRewritePlan}.
 *   <li>{@link FileRewritePlan} - This immutable output of the planner contains a set of groups
 *       like {@link RewriteFileGroup} or {@link RewritePositionDeletesGroup}, each containing a set
 *       of files that are meant to be compacted together by the {@link FileRewriteRunner}.
 *   <li>{@link FileRewriteRunner} - The runner is the engine specific implementation that can take
 *       a {@link FileRewritePlan} and perform a rewrite on each of the groups within. This is the
 *       actual implementation of the rewrite which generates new files.
 * </ul>
 *
 * <p>The lifecycle for the planner looks like the following:
 *
 * <ul>
 *   <li>{@link #init(Map)} initializes the planner with the configuration parameters
 *   <li>{@link #plan()} called for generating the {@link FileRewritePlan} for the given parameters.
 * </ul>
 *
 * @param <I> the Java type of the plan info like {@link RewriteDataFiles.FileGroupInfo} or {@link
 *     RewritePositionDeleteFiles.FileGroupInfo}
 * @param <T> the Java type of the input scan tasks (input)
 * @param <F> the Java type of the content files (input and output)
 * @param <G> the Java type of the rewrite file group like {@link RewriteFileGroup} or {@link
 *     RewritePositionDeletesGroup}
 */
public interface FileRewritePlanner<
    I,
    T extends ContentScanTask<F>,
    F extends ContentFile<F>,
    G extends RewriteGroupBase<I, T, F>> {

  /** Returns a description for this planner. */
  default String description() {
    return getClass().getName();
  }

  /**
   * Returns a set of supported options for this planner. Only options specified in this list will
   * be accepted at runtime. Any other options will be rejected.
   */
  Set<String> validOptions();

  /**
   * Initializes this planner using provided options.
   *
   * @param options options to initialize this planner
   */
  void init(Map<String, String> options);

  /**
   * Generates the plan for rewrite.
   *
   * @return the generated plan which could be executed during the compaction
   */
  FileRewritePlan<I, T, F, G> plan();
}
