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

import java.util.concurrent.ExecutorService;

public interface RemoveExpiredFiles extends Action<RemoveExpiredFiles, RemoveExpiredFiles.Result> {

  /**
   * Passes an alternative executor service that will be used for the removal of expired files. If
   * this method is not called, snapshot integrity checker will still be running by a single
   * threaded executor service.
   *
   * @param executorService an executor service to parallelize tasks to check snapshot integrity
   * @return this for method chaining
   */
  RemoveExpiredFiles executeWith(ExecutorService executorService);

  /**
   * Pass the target version to check. The action checks the snapshots in the target version, not in
   * the current version of the table.
   *
   * @param targetVersion the target version file to be checked. Either a file name or a file path
   *     is acceptable. For example, it could be either
   *     "00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json" or
   *     "/path/to/00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json".
   * @return this for method chaining
   */
  RemoveExpiredFiles targetVersion(String targetVersion);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the number of deleted data files. */
    long deletedDataFilesCount();

    /** Returns the number of deleted manifests. */
    long deletedManifestsCount();

    /** Returns the number of deleted manifest lists. */
    long deletedManifestListsCount();
  }
}
