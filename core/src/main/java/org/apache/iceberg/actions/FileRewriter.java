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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;

/**
 * A class for rewriting content files.
 *
 * @param <T> the Java type of tasks to read content files
 * @param <F> the Java type of content files
 */
public interface FileRewriter<T extends ContentScanTask<F>, F extends ContentFile<F>> {
  /** Returns a description for this rewriter. */
  default String description() {
    return getClass().getName();
  }

  /**
   * Returns a set of supported options for this rewriter. This is an allowed-list and any options
   * not specified here will be rejected at runtime.
   *
   * @return returns a set of supported options
   */
  Set<String> validOptions();

  /**
   * Initializes this rewriter using provided options.
   *
   * @param options options to initialize this rewriter
   */
  void init(Map<String, String> options);

  /**
   * Selects files which this rewriter believes are valid targets to be rewritten.
   *
   * @param tasks an iterable of scan task for files in a partition
   * @return the iterable containing only scan task for files to be rewritten
   */
  Iterable<T> selectFiles(Iterable<T> tasks);

  /**
   * Groups scan tasks into lists which will be processed in a single executable unit. Each group
   * will end up being rewritten as an independent set of changes. This creates the jobs which will
   * eventually be run by the underlying action.
   *
   * @param tasks an iterable of scan tasks for files to be rewritten
   * @return the iterable of lists of scan tasks for files which will be processed together
   */
  Iterable<List<T>> planFileGroups(Iterable<T> tasks);

  /**
   * Rewrite a group of files represented by the given list of scan tasks.
   *
   * <p>The implementation is supposed to be engine-specific (e.g. Spark, Flink, Trino).
   *
   * @param group a group of scan tasks for files to be rewritten together
   * @return a set of newly written files
   */
  Set<F> rewrite(List<T> group);
}
