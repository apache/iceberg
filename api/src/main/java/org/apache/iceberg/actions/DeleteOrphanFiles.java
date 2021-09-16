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

import java.util.function.Consumer;

/**
 * An action that deletes orphan files in a table.
 * <p>
 * A metadata or data file is considered orphan if it is not reachable by any valid snapshot.
 * The set of actual files is built by listing the underlying storage which makes this operation
 * expensive.
 */
public interface DeleteOrphanFiles extends Action<DeleteOrphanFiles, DeleteOrphanFiles.Result> {
  /**
   * Passes a location which should be scanned for orphan files.
   * <p>
   * If not set, the root table location will be scanned potentially removing both orphan data and
   * metadata files.
   *
   * @param location the location where to look for orphan files
   * @return this for method chaining
   */
  DeleteOrphanFiles location(String location);

  /**
   * Removes orphan files only if they are older than the given timestamp.
   * <p>
   * This is a safety measure to avoid removing files that are being added to the table.
   * For example, there may be a concurrent operation adding new files while this action searches
   * for orphan files. New files may not be referenced by the metadata yet but they are not orphan.
   * <p>
   * If not set, defaults to a timestamp 3 days ago.
   *
   * @param olderThanTimestamp a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  DeleteOrphanFiles olderThan(long olderThanTimestamp);

  /**
   * Passes an alternative delete implementation that will be used for orphan files.
   * <p>
   * This method allows users to customize the delete func. For example, one may set a custom delete
   * func and collect all orphan files into a set instead of physically removing them.
   * <p>
   * If not set, defaults to using the table's {@link org.apache.iceberg.io.FileIO io} implementation.
   *
   * @param deleteFunc a function that will be called to delete files
   * @return this for method chaining
   */
  DeleteOrphanFiles deleteWith(Consumer<String> deleteFunc);

  /**
   * The action result that contains a summary of the execution.
   */
  interface Result {
    /**
     * Returns locations of orphan files.
     */
    Iterable<String> orphanFileLocations();
  }
}
