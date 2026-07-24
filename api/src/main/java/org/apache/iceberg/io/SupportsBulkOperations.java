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
package org.apache.iceberg.io;

import javax.annotation.Nullable;

public interface SupportsBulkOperations extends FileIO {
  /**
   * Delete the files at the given paths.
   *
   * @param pathsToDelete The paths to delete
   * @throws BulkDeletionFailureException in case of failure to delete at least 1 file
   */
  void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException;

  /**
   * Delete the files at the given paths, invoking {@code failureHandler} for each per-file failure.
   *
   * <p>The handler receives a {@link FileFailure} for every object that failed to delete, allowing
   * callers to inspect categorized failures (e.g. for retry decisions) without parsing
   * cloud-specific errors. The default implementation ignores the handler and delegates to {@link
   * #deleteFiles(Iterable)}; FileIO implementations should override to invoke the handler.
   *
   * <p>Deleting an object that does not exist is the desired end state for a delete and is treated
   * as success: such an object is neither reported to the handler nor counted toward {@link
   * BulkDeletionFailureException}, regardless of whether the underlying store signals the missing
   * object with a success code or an exception. The handler therefore only sees objects that may
   * still exist (e.g. {@link FailureCategory#AUTH}, {@link FailureCategory#THROTTLED}, {@link
   * FailureCategory#TRANSIENT}).
   *
   * <p>The handler may be invoked concurrently from multiple threads and must be thread-safe; see
   * {@link FailureHandler}.
   *
   * @param pathsToDelete The paths to delete
   * @param failureHandler Callback invoked once per failed object; a null handler is treated as
   *     {@link FailureHandler#NOOP}
   * @throws BulkDeletionFailureException in case of failure to delete at least 1 file
   */
  default void deleteFiles(Iterable<String> pathsToDelete, @Nullable FailureHandler failureHandler)
      throws BulkDeletionFailureException {
    deleteFiles(pathsToDelete);
  }
}
