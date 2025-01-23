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
package org.apache.iceberg;

/**
 * API for deleting files from a table. This is intended for use in removing missing files, both
 * {@link DataFile data files} and {@link DeleteFile delete files}.
 *
 * <p>This API accumulates file deletions, produces a new {@link Snapshot} of the table, and commits
 * that snapshot as the current.
 *
 * <p>When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface RemoveMissingFiles extends SnapshotUpdate<RemoveMissingFiles> {
  /**
   * Delete a file tracked by a {@link DataFile} from the underlying table.
   *
   * @param file a DataFile to remove from the table
   * @return this for method chaining
   */
  RemoveMissingFiles deleteFile(DataFile file);

  /**
   * Delete a file tracked by a {@link DeleteFile} from the underlying table.
   *
   * @param file a DeleteFile to remove from the table
   * @return this for method chaining
   */
  RemoveMissingFiles deleteFile(DeleteFile file);

  /**
   * Enables validation that any files that are part of the deletion still exist when committing the
   * operation.
   *
   * @return this for method chaining
   */
  RemoveMissingFiles validateFilesExist();
}
