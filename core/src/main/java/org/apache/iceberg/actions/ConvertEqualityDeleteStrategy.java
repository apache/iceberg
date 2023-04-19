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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

/** A strategy for the action to convert equality delete to position deletes. */
public interface ConvertEqualityDeleteStrategy {

  /** Returns the name of this convert deletes strategy */
  String name();

  /** Returns the table being modified by this convert strategy */
  Table table();

  /**
   * Returns a set of options which this convert strategy can use. This is an allowed-list and any
   * options not specified here will be rejected at runtime.
   */
  Set<String> validOptions();

  /** Sets options to be used with this strategy */
  ConvertEqualityDeleteStrategy options(Map<String, String> options);

  /**
   * Select the delete files to convert.
   *
   * @param deleteFiles iterable of delete files in a group.
   * @return iterable of original delete file to be converted.
   */
  Iterable<DeleteFile> selectDeleteFiles(Iterable<DeleteFile> deleteFiles);

  /**
   * Groups delete files into lists which will be processed in a single executable unit. Each group
   * will end up being committed as an independent set of changes. This creates the jobs which will
   * eventually be run as by the underlying Action.
   *
   * @param dataFiles iterable of data files that contain the DeleteFile to be converted
   * @return iterable of lists of FileScanTasks which will be processed together
   */
  Iterable<Iterable<FileScanTask>> planDeleteFileGroups(Iterable<FileScanTask> dataFiles);

  /**
   * Define how to convert the deletes.
   *
   * @param deleteFilesToConvert a group of files to be converted together
   * @return iterable of delete files used to replace the original delete files.
   */
  Iterable<DeleteFile> convertDeleteFiles(Iterable<DeleteFile> deleteFilesToConvert);
}
