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
 * A class for planning content file rewrites.
 *
 * <p>The entire rewrite operation is broken down into pieces. The grouping is based on partitioning
 * and the planning could create multiple groups within a partition. As a result {@link
 * FileRewritePlan} is generated which contains the data need by the {@link FileRewriteExecutor}s
 * which execute the actual file rewrite.
 *
 * <p>The lifecycle of the planner is:
 *
 * <ul>
 *   <li>{@link #init(Map)} initializes the planner with the configuration parameters
 *   <li>{@link #plan()} generates the plan for the given configuration
 * </ul>
 *
 * @param <FGI> the Java type of the plan info like {@link RewriteDataFiles.FileGroupInfo} or {@link
 *     RewritePositionDeleteFiles.FileGroupInfo}
 * @param <T> the Java type of the tasks to read the files which are rewritten
 * @param <F> the Java type of the content files which are rewritten
 * @param <G> the Java type of the planned groups
 */
public interface FileRewritePlanner<
    FGI,
    T extends ContentScanTask<F>,
    F extends ContentFile<F>,
    G extends FileRewriteGroup<FGI, T, F>> {

  /** Returns a description for this rewriter. */
  default String description() {
    return getClass().getName();
  }

  /**
   * Returns a set of supported options for this rewriter. Only options specified in this list will
   * be accepted at runtime. Any other options will be rejected.
   */
  Set<String> validOptions();

  /**
   * Initializes this planner using provided options.
   *
   * @param options options to initialize this rewriter
   */
  void init(Map<String, String> options);

  /**
   * Generates the plan for rewrite.
   *
   * @return the generated plan which could be executed during the compaction
   */
  FileRewritePlan<FGI, T, F, G> plan();
}
