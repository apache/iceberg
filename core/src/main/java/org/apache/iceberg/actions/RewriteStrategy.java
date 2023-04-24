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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

/**
 * A strategy for rewriting files.
 *
 * @deprecated since 1.3.0, will be removed in 1.4.0; use {@link FileRewriter} instead.
 */
@Deprecated
public interface RewriteStrategy extends Serializable {
  /** Returns the name of this rewrite strategy */
  String name();

  /** Returns the table being modified by this rewrite strategy */
  Table table();

  /**
   * Returns a set of options which this rewrite strategy can use. This is an allowed-list and any
   * options not specified here will be rejected at runtime.
   */
  Set<String> validOptions();

  /** Sets options to be used with this strategy */
  RewriteStrategy options(Map<String, String> options);

  /**
   * Selects files which this strategy believes are valid targets to be rewritten.
   *
   * @param dataFiles iterable of FileScanTasks for files in a given partition
   * @return iterable containing only FileScanTasks to be rewritten
   */
  Iterable<FileScanTask> selectFilesToRewrite(Iterable<FileScanTask> dataFiles);

  /**
   * Groups file scans into lists which will be processed in a single executable unit. Each group
   * will end up being committed as an independent set of changes. This creates the jobs which will
   * eventually be run as by the underlying Action.
   *
   * @param dataFiles iterable of FileScanTasks to be rewritten
   * @return iterable of lists of FileScanTasks which will be processed together
   */
  Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> dataFiles);

  /**
   * Method which will rewrite files based on this particular RewriteStrategy's algorithm. This will
   * most likely be Action framework specific (Spark/Presto/Flink ....).
   *
   * @param filesToRewrite a group of files to be rewritten together
   * @return a set of newly written files
   */
  Set<DataFile> rewriteFiles(List<FileScanTask> filesToRewrite);
}
