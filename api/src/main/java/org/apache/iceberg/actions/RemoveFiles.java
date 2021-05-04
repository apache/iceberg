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
import java.util.function.Consumer;

/**
 * An action that deletes data, manifest, manifest lists in a table.
 * <p>
 * Implementations may use a query engine to distribute parts of work.
 */
public interface RemoveFiles extends Action<RemoveFiles, RemoveFiles.Result> {

  /**
   * Passes an alternative delete implementation that will be used for manifests and data files.
   *
   * @param deleteFunc a function that will be called to delete manifests and data files.
   *                  The function accepts path to file as an argument.
   * @return this for method chaining
   */
  RemoveFiles deleteWith(Consumer<String> deleteFunc);

  /**
   * Passes an alternative executor service that will be used for manifests and data files deletion.
   * <p>
   * If this method is not called, manifests and data files will still be deleted in
   * the current thread.
   * <p>
   *
   * @param executorService the service to use
   * @return this for method chaining
   */
  RemoveFiles executeDeleteWith(ExecutorService executorService);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns the number of deleted data files.
     */
    long deletedDataFilesCount();

    /**
     * Returns the number of deleted manifests.
     */
    long deletedManifestsCount();

    /**
     * Returns the number of deleted manifest lists.
     */
    long deletedManifestListsCount();

    /**
     * Returns the number of files deleted other than data, manifest and manifest list.
     */
    long otherDeletedFilesCount();
  }
}
