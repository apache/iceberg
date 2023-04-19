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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.immutables.value.Value;

/**
 * An action that deletes all files referenced by a table metadata file.
 *
 * <p>This action will irreversibly delete all reachable files such as data files, manifests,
 * manifest lists and should be used to clean up the underlying storage once a table is dropped and
 * no longer needed.
 *
 * <p>Implementations may use a query engine to distribute parts of work.
 */
@Value.Enclosing
public interface DeleteReachableFiles
    extends Action<DeleteReachableFiles, DeleteReachableFiles.Result> {

  /**
   * Passes an alternative delete implementation that will be used for files.
   *
   * @param deleteFunc a function that will be called to delete files. The function accepts path to
   *     file as an argument.
   * @return this for method chaining
   */
  DeleteReachableFiles deleteWith(Consumer<String> deleteFunc);

  /**
   * Passes an alternative executor service that will be used for files removal. This service will
   * only be used if a custom delete function is provided by {@link #deleteWith(Consumer)} or if the
   * FileIO does not {@link SupportsBulkOperations support bulk deletes}. Otherwise, parallelism
   * should be controlled by the IO specific {@link SupportsBulkOperations#deleteFiles(Iterable)
   * deleteFiles} method.
   *
   * @param executorService the service to use
   * @return this for method chaining
   */
  DeleteReachableFiles executeDeleteWith(ExecutorService executorService);

  /**
   * Set the {@link FileIO} to be used for files removal
   *
   * @param io FileIO to use for files removal
   * @return this for method chaining
   */
  DeleteReachableFiles io(FileIO io);

  /** The action result that contains a summary of the execution. */
  @Value.Immutable
  interface Result {

    /** Returns the number of deleted data files. */
    long deletedDataFilesCount();

    /** Returns the number of deleted equality delete files. */
    long deletedEqualityDeleteFilesCount();

    /** Returns the number of deleted position delete files. */
    long deletedPositionDeleteFilesCount();

    /** Returns the number of deleted manifests. */
    long deletedManifestsCount();

    /** Returns the number of deleted manifest lists. */
    long deletedManifestListsCount();

    /** Returns the number of deleted metadata json, version hint files. */
    long deletedOtherFilesCount();
  }
}
