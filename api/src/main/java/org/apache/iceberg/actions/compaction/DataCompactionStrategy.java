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

package org.apache.iceberg.actions.compaction;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

interface DataCompactionStrategy extends Serializable {
  /**
   * Returns the name of this compaction strategy
   */
  String name();

  /**
   * Returns a set of options which this compaction strategy can use. This is an allowed-list and any options not
   * specified here will be rejected at runtime.
   */
  Set<String> validOptions();

  DataCompactionStrategy withOptions(Map<String, String> options);

  /**
   * Removes all file references which this plan will not rewrite or change. Unlike the preFilter, this method can
   * execute arbitrary code and isn't restricted to just Iceberg Expressions.
   *
   * @param dataFiles iterator of live datafiles in a given partition
   * @return iterator containing only files to be rewritten
   */
  Iterable<FileScanTask> selectFilesToCompact(Iterable<FileScanTask> dataFiles);

  /**
   * Groups file scans into lists which will be processed in a single executable unit. Each group will end up being
   * committed as an independent set of changes. This creates the jobs which will eventually be run as by the underlying
   * Action.
   *
   * @param dataFiles iterator of files to be rewritten
   * @return iterator of sets of files to be processed together
   */
  Iterable<List<FileScanTask>> groupFilesIntoChunks(Iterable<FileScanTask> dataFiles);

  /**
   * Method which will rewrite files based on this particular DataCompactionStrategy's Algorithm.
   * This will most likely be Action framework specific.
   *
   * @param table          table being modified
   * @param filesToRewrite a group of files to be rewritten together
   * @return a list of newly written files
   */
  List<DataFile> rewriteFiles(Table table, Set<FileScanTask> filesToRewrite);

}
