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
 * A class for rewriting content file groups ({@link RewriteGroupBase}). The lifecycle for the
 * runner looks like the following:
 *
 * <ul>
 *   <li>{@link #init(Map)} initializes the runner with the configuration parameters
 *   <li>{@link #rewrite(RewriteGroupBase)} called for every group in the plan to do the actual
 *       rewrite of the files, and returns the generated new files.
 * </ul>
 *
 * @param <I> the Java type of the plan info like {@link RewriteDataFiles.FileGroupInfo} or {@link
 *     RewritePositionDeleteFiles.FileGroupInfo}
 * @param <T> the Java type of the input scan tasks (input)
 * @param <F> the Java type of the content files (input and output)
 * @param <G> the Java type of the rewrite file group like {@link RewriteFileGroup} or {@link
 *     RewritePositionDeletesGroup}
 */
public interface FileRewriteRunner<
    I,
    T extends ContentScanTask<F>,
    F extends ContentFile<F>,
    G extends RewriteGroupBase<I, T, F>> {

  /** Returns a description for this runner. */
  default String description() {
    return getClass().getName();
  }

  /**
   * Returns a set of supported options for this runner. Only options specified in this list will be
   * accepted at runtime. Any other options will be rejected.
   */
  Set<String> validOptions();

  /**
   * Initializes this runner using provided options.
   *
   * @param options options to initialize this runner
   */
  void init(Map<String, String> options);

  /**
   * Rewrite a group of files represented by the given list of scan tasks.
   *
   * <p>The implementation is supposed to be engine-specific (e.g. Spark, Flink, Trino).
   *
   * @param group of scan tasks for files to be rewritten together
   * @return a set of newly written files
   */
  Set<F> rewrite(G group);
}
