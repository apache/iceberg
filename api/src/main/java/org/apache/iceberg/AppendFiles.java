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
 * API for appending new files in a table.
 * <p>
 * This API accumulates file additions, produces a new {@link Snapshot} of the table, and commits
 * that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface AppendFiles extends SnapshotUpdate<AppendFiles> {
  /**
   * Append a {@link DataFile} to the table.
   *
   * @param file a data file
   * @return this for method chaining
   */
  AppendFiles appendFile(DataFile file);

  /**
   * Append the contents of a manifest to the table.
   * <p>
   * The manifest must contain only appended files. All files in the manifest will be appended to
   * the table in the snapshot created by this update.
   *
   * @param file a manifest file
   * @return this for method chaining
   */
  AppendFiles appendManifest(ManifestFile file);
}
