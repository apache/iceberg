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
 * A class for rewriting content file groups ({@link FileRewriteGroup}).
 *
 * @param <I> the Java type of the plan info
 * @param <T> the Java type of the tasks to read content files
 * @param <F> the Java type of the content files
 * @param <G> the Java type of the planned groups
 */
public interface FileRewriteExecutor<
    I,
    T extends ContentScanTask<F>,
    F extends ContentFile<F>,
    G extends FileRewriteGroup<I, T, F>> {

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
   * Initializes this rewriter using provided options.
   *
   * @param options options to initialize this rewriter
   */
  void init(Map<String, String> options);

  /**
   * Initializes the rewriter using the information generated during planning.
   *
   * @param plan containing the configuration data
   */
  void initPlan(FileRewritePlan<I, T, F, G> plan);

  /**
   * Rewrite a group of files represented by the given list of scan tasks.
   *
   * <p>The implementation is supposed to be engine-specific (e.g. Spark, Flink, Trino).
   *
   * @param group a group of scan tasks for files to be rewritten together
   * @return a set of newly written files
   */
  Set<F> rewrite(G group);
}
